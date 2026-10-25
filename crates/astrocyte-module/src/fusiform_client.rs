//! Wire-backed fusiform client: `catalog.status` for the history boundary,
//! `catalog.get` for token rates and
//! `plan.prices` for the subscription multiplier's plan prices.
//!
//! The refresher owns this synchronous facade on its dedicated OS thread. The
//! subc connection and tool-provider route are opened lazily, reused while
//! healthy, and evicted after transport timeouts or route-death errors.

use std::path::PathBuf;
use std::time::Duration;

use astrocyte_core::catalog_client::{CatalogCallError, CatalogConnection};
use astrocyte_core::multiplier::PlanPriceSource;
use fusiform_protocol::{
    CatalogGetRequest, CatalogGetResponse, PlanPricesRequest, PlanPricesResponse, StatusRequest,
    StatusResponse,
};
use serde_json::{json, Value};
use subc_protocol::ErrorBody as WireError;
use subc_protocol::{error_codes, Frame, FrameType};
#[cfg(any(test, feature = "test-support"))]
use subc_protocol::{Flags, Priority};
use subc_transport::{authenticate_client, connection_file};
#[cfg(any(test, feature = "test-support"))]
use subc_transport::{read_frame, write_frame};
#[cfg(any(test, feature = "test-support"))]
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

// Copied sibling facts; nothing links them to fusiform's source, so a rename
// on their side fails here at the first call rather than at build. Re-derive
// against `fusiform/crates/*/src/*.rs` MODULE_ID and the tool name their
// provider matches on. Last re-derived 2026-09-06 at fusiform 901b3ae.
const FUSIFORM_MODULE_ID: &str = "fusiform";
const OP_CATALOG_GET: &str = "catalog.get";
const OP_CATALOG_STATUS: &str = "catalog.status";
// Unlike catalog.get, the protocol crate names this tool (since 0.24); a test
// below pins this copy to it, so a rename there fails the build's tests
// rather than the first call.
const OP_PLAN_PRICES: &str = "plan.prices";
const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

struct Live {
    stream: TcpStream,
    route_channel: u16,
    route_epoch: u32,
    next_corr: u64,
}

pub struct FusiformClient {
    connection_file_path: PathBuf,
    request_timeout: Duration,
    runtime: tokio::runtime::Runtime,
    live: std::sync::Mutex<Option<Live>>,
}

impl FusiformClient {
    pub fn new(connection_file_path: impl Into<PathBuf>) -> Result<Self, String> {
        Self::build(connection_file_path.into(), REQUEST_TIMEOUT)
    }

    fn build(connection_file_path: PathBuf, request_timeout: Duration) -> Result<Self, String> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .enable_time()
            .build()
            .map_err(|error| format!("client runtime: {error}"))?;
        Ok(Self {
            connection_file_path,
            request_timeout,
            runtime,
            live: std::sync::Mutex::new(None),
        })
    }

    #[cfg(test)]
    fn new_with_request_timeout(
        connection_file_path: impl Into<PathBuf>,
        request_timeout: Duration,
    ) -> Result<Self, String> {
        Self::build(connection_file_path.into(), request_timeout)
    }

    async fn open(&self) -> Result<Live, String> {
        let metadata = connection_file::read(&self.connection_file_path)
            .map_err(|error| format!("connection file: {error:?}"))?;
        let endpoint = metadata
            .endpoints
            .first()
            .ok_or_else(|| "no endpoint in connection file".to_string())?;
        let mut stream = tokio::time::timeout(
            CONNECT_TIMEOUT,
            crate::peer_exchange::connect_endpoint(&endpoint.host, endpoint.port),
        )
        .await
        .map_err(|_| "connect timeout".to_string())?
        .map_err(|error| format!("connect: {error}"))?;
        authenticate_client(&mut stream, &metadata, CONNECT_TIMEOUT)
            .await
            .map_err(|error| format!("auth: {error:?}"))?;

        let mut live = Live {
            stream,
            route_channel: 0,
            route_epoch: 0,
            next_corr: 1,
        };
        // Fusiform is a TOOL PROVIDER, not a management surface: its manifest
        // declares catalog.get as a tool and the daemon refuses a
        // management_surface target with `target_unavailable`. Broca, the one
        // live consumer, opens the same kind.
        let body = serde_json::to_vec(&json!({
            "op": "route.open",
            "target": { "kind": "tool_provider", "module_id": FUSIFORM_MODULE_ID },
            "identity": {
                "project_root": "/",
                "harness": "ck-astrocyte",
                "session": "catalog-pricing",
            },
        }))
        .map_err(|error| error.to_string())?;
        let response = self.roundtrip_on(&mut live, 0, 0, body).await?;
        let value: Value =
            serde_json::from_slice(&response.body).map_err(|error| error.to_string())?;
        if response.header.ty == FrameType::Error {
            return Err(format!("route.open error: {value}"));
        }
        live.route_channel = value
            .get("route_channel")
            .and_then(Value::as_u64)
            .and_then(|value| u16::try_from(value).ok())
            .ok_or("route_channel missing")?;
        live.route_epoch = value
            .get("route_epoch")
            .and_then(Value::as_u64)
            .and_then(|value| u32::try_from(value).ok())
            .ok_or("route_epoch missing")?;
        Ok(live)
    }

    async fn roundtrip_on(
        &self,
        live: &mut Live,
        channel: u16,
        epoch: u32,
        body: Vec<u8>,
    ) -> Result<Frame, String> {
        let corr = live.next_corr;
        live.next_corr += 1;
        crate::peer_exchange::exchange(
            &mut live.stream,
            channel,
            epoch,
            corr,
            body,
            crate::peer_exchange::ExchangeMode::Peer,
        )
        .await
    }

    fn call_catalog(
        &self,
        request: CatalogGetRequest,
    ) -> Result<CatalogGetResponse, CatalogCallError> {
        // fusiform-protocol's request type is Deserialize-only at the pinned
        // revision, so encode its public fields as the arguments object
        // without inventing a second request model.
        let mut arguments = json!({
            "source": request.source,
            "at_ms": request.at_ms,
            "fact_prefixes": request.fact_prefixes,
            "include_retired": request.include_retired,
            "model_id": request.model_id,
            "provider_id": request.provider_id,
        });
        // Producers predating billing planes reject unknown fields, even null.
        // Omit auth_method when no plane was selected to preserve that shape.
        if let Some(auth_method) = request.auth_method {
            arguments["auth_method"] = auth_method.into();
        }
        let payload = self.call_tool(OP_CATALOG_GET, arguments, "models")?;
        serde_json::from_value(payload).map_err(|error| CatalogCallError {
            code: "undecodable".into(),
            message: format!("fusiform catalog.get result: {error}"),
        })
    }

    fn call_plan_prices(
        &self,
        request: PlanPricesRequest,
    ) -> Result<PlanPricesResponse, CatalogCallError> {
        let arguments = serde_json::to_value(&request).map_err(|error| CatalogCallError {
            code: "encode".into(),
            message: error.to_string(),
        })?;
        let payload = self.call_tool(OP_PLAN_PRICES, arguments, "prices")?;
        serde_json::from_value(payload).map_err(|error| CatalogCallError {
            code: "undecodable".into(),
            message: format!("fusiform plan.prices result: {error}"),
        })
    }

    /// One tool call over the shared route, returning the response payload.
    /// `payload_key` is a member only the direct response shape carries, which
    /// tells it apart from a wrapped one.
    fn call_tool(
        &self,
        name: &str,
        arguments: Value,
        payload_key: &str,
    ) -> Result<Value, CatalogCallError> {
        // Fusiform decodes the canonical subc tool-call envelope
        // `{"name", "arguments"}`; a `method`/`params` body is refused as
        // bad_request. `tool_call_id` is omitted: no model asked for this call
        // and nothing minted an id.
        let body = serde_json::to_vec(&json!({
            "name": name,
            "arguments": arguments,
        }))
        .map_err(|error| CatalogCallError {
            code: "encode".into(),
            message: error.to_string(),
        })?;
        let mut guard = self
            .live
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        self.runtime.block_on(async {
            // Setup, writes, Pongs and both attempts share one deadline. A
            // peer sending unrelated frames must not extend the call's life.
            let deadline = tokio::time::Instant::now() + self.request_timeout;
            let mut last_error: Option<CatalogCallError> = None;
            let result = tokio::time::timeout_at(deadline, async {
                for _attempt in 0..2 {
                    if guard.is_none() {
                        match self.open().await {
                            Ok(live) => *guard = Some(live),
                            Err(error) => {
                                let previous = last_error
                                    .as_ref()
                                    .map(|error: &CatalogCallError| {
                                        format!(
                                            "; previous exchange: {}: {}",
                                            error.code, error.message
                                        )
                                    })
                                    .unwrap_or_default();
                                return Err(CatalogCallError {
                                    code: "unavailable".into(),
                                    message: format!("fusiform connect: {error}{previous}"),
                                });
                            }
                        }
                    }
                    let live = guard.as_mut().expect("connection was just established");
                    let (channel, epoch) = (live.route_channel, live.route_epoch);
                    let response = match self.roundtrip_on(live, channel, epoch, body.clone()).await
                    {
                        Ok(response) => response,
                        Err(error) => {
                            *guard = None;
                            last_error = Some(CatalogCallError {
                                code: "unavailable".into(),
                                message: format!("fusiform exchange: {error}"),
                            });
                            continue;
                        }
                    };
                    if response.header.ty == FrameType::Error {
                        let error: WireError =
                            serde_json::from_slice(&response.body).map_err(|decode| {
                                CatalogCallError {
                                    code: "undecodable".into(),
                                    message: format!("fusiform error body: {decode}"),
                                }
                            })?;
                        if error_codes::is_established_route_dead(
                            response.header.flags,
                            &error.code,
                        ) {
                            *guard = None;
                            last_error = Some(CatalogCallError {
                                code: error.code,
                                message: error.message,
                            });
                            continue;
                        }
                        // Reloading is transient but does not invalidate the route.
                        if error.code == "module_reloading" {
                            last_error = Some(CatalogCallError {
                                code: error.code,
                                message: error.message,
                            });
                            continue;
                        }
                        return Err(CatalogCallError {
                            code: error.code,
                            message: error.message,
                        });
                    }
                    // Fusiform serializes the untagged `ToolResponse` directly: the
                    // body IS the tool's response, with no `result` wrapper.
                    // Accept the direct shape first. Broca's decoder additionally
                    // accepts a CallToolResult text envelope; fusiform's serve loop
                    // does not emit one today, so a `result` key here is decoded
                    // only as a defensive second arm and never assumed.
                    let envelope: Value =
                        serde_json::from_slice(&response.body).map_err(|error| {
                            CatalogCallError {
                                code: "undecodable".into(),
                                message: format!("fusiform response: {error}"),
                            }
                        })?;
                    return if envelope.get(payload_key).is_some() {
                        Ok(envelope)
                    } else {
                        envelope
                            .get("result")
                            .cloned()
                            .ok_or_else(|| CatalogCallError {
                                code: "missing_result".into(),
                                message: envelope.to_string(),
                            })
                    };
                }
                Err(last_error.take().unwrap_or(CatalogCallError {
                    code: "unavailable".into(),
                    message: "fusiform attempts exhausted".into(),
                }))
            })
            .await;
            match result {
                Ok(result) => result,
                Err(_) => {
                    *guard = None;
                    let previous = last_error
                        .map(|error| {
                            format!("; previous exchange: {}: {}", error.code, error.message)
                        })
                        .unwrap_or_default();
                    Err(CatalogCallError {
                        code: "unavailable".into(),
                        message: format!("fusiform exchange: response timeout{previous}"),
                    })
                }
            }
        })
    }
}

impl CatalogConnection for FusiformClient {
    fn catalog_status(&self, request: StatusRequest) -> Result<StatusResponse, CatalogCallError> {
        let payload = self.call_tool(
            OP_CATALOG_STATUS,
            json!({"source": request.source, "polls": request.polls}),
            "catalog_version",
        )?;
        serde_json::from_value(payload).map_err(|error| CatalogCallError {
            code: "undecodable".into(),
            message: format!("fusiform catalog.status result: {error}"),
        })
    }

    fn catalog_get(
        &self,
        request: CatalogGetRequest,
    ) -> Result<CatalogGetResponse, CatalogCallError> {
        self.call_catalog(request)
    }
}

impl PlanPriceSource for FusiformClient {
    fn plan_prices(
        &self,
        request: PlanPricesRequest,
    ) -> Result<PlanPricesResponse, CatalogCallError> {
        self.call_plan_prices(request)
    }
}

// Test ledger totals are nonnegative, and the clock fixture is within i64 milliseconds.
#[allow(clippy::cast_possible_truncation)]
#[cfg(test)]
pub(crate) mod tests {
    #[test]
    fn shared_error_body_accepts_optional_and_arbitrary_detail() {
        for detail in [
            None,
            Some(serde_json::json!(null)),
            Some(serde_json::json!({"retry_ms": 10})),
            Some(serde_json::json!([1])),
            Some(serde_json::json!("text")),
            Some(serde_json::json!(true)),
            Some(serde_json::json!(42)),
        ] {
            let mut body =
                serde_json::json!({"code":"refused", "message":"same message", "future_field":1});
            if let Some(detail) = detail {
                body["detail"] = detail;
            }
            let error: super::WireError = serde_json::from_value(body).unwrap();
            assert_eq!(error.code, "refused");
            assert_eq!(error.message, "same message");
        }
    }
    use std::path::PathBuf;
    use std::sync::{atomic::AtomicBool, Arc};

    use astrocyte_core::ingest::EventRecordRequest;
    use subc_transport::auth::authenticate_server;
    use subc_transport::connection_file::{ConnectionInfo, Endpoint, KEY_LEN, SCHEMA_VERSION};

    use super::*;
    use crate::refresher::{self, ExportPage, Producer, PullError, SourceClient, SourceCursor};
    use crate::state::test_state;
    use crate::test_support::catalog::{event, CatalogPeer};
    use crate::test_support::Background;

    /// The plan-price tool name is a copied literal; the protocol crate is the
    /// authority for it.
    #[test]
    fn plan_prices_tool_name_matches_the_protocol() {
        assert_eq!(OP_PLAN_PRICES, fusiform_protocol::TOOL_PLAN_PRICES);
    }

    struct OneEvent(EventRecordRequest);

    impl SourceClient for OneEvent {
        fn pull(&self, _since: Option<SourceCursor>, _limit: u32) -> Result<ExportPage, PullError> {
            Ok(ExportPage {
                set_aside: Vec::new(),
                cursor: SourceCursor {
                    lineage: "fusiform-loop-test:1".into(),
                    ordinal: 1,
                },
                high_water: SourceCursor {
                    lineage: "fusiform-loop-test:1".into(),
                    ordinal: 1,
                },
                events: vec![self.0.clone()],
                pending_known: 0,
                pending_unknown: false,
            })
        }
    }

    fn install_live_catalog(
        state: &crate::state::SharedState,
        path: &PathBuf,
    ) -> astrocyte_core::catalog_client::CatalogClient {
        let connection: Arc<dyn CatalogConnection> = Arc::new(FusiformClient::new(path).unwrap());
        let catalog = refresher::connect_catalog(connection).unwrap();
        state.install_catalog(Some(catalog.clone())).unwrap();
        catalog
    }

    fn run_one_iteration(state: &crate::state::SharedState, event: EventRecordRequest) {
        let producers = vec![Producer {
            source_key: "broca".into(),
            client: Arc::new(OneEvent(event)),
        }];
        refresher::run_iteration(state, &producers, &AtomicBool::new(false));
    }

    /// The requests a `CatalogPeer` received when an event at `event_at` was
    /// priced through it: status without a boundary, a refused time-zero read
    /// that names the earliest covered instant, then at least one resolution, and
    /// every resolution asked for the event's own time.
    fn assert_resolved_through_the_peer(requests: &[Value], event_at: i64) {
        assert_eq!(requests[0]["name"], OP_CATALOG_STATUS);
        assert_eq!(
            requests
                .get(1)
                .map(|request| &request["arguments"]["at_ms"]),
            Some(&json!(0)),
            "the boundary probe must follow status: {requests:?}"
        );
        let resolutions = &requests[2..];
        assert!(
            !resolutions.is_empty(),
            "no resolution reached the peer: {requests:?}"
        );
        assert!(
            resolutions
                .iter()
                .all(|request| request["arguments"]["at_ms"] == event_at),
            "every resolution must ask for the event's time {event_at}: {requests:?}"
        );
    }

    #[test]
    fn loop_prices_drained_event_through_live_fusiform_socket() {
        let event_at = 1_000;
        let resolved_at_ms = 44_444;
        let peer = CatalogPeer::start(event_at - 1, 9, resolved_at_ms);
        let state = test_state();
        install_live_catalog(&state, &peer.path);

        run_one_iteration(&state, event(event_at));

        let stored: (i64, i64) = state
            .with_conn(|conn| {
                conn.query_row(
                    "SELECT COUNT(*),MIN(resolved_at_ms) FROM priced_charge WHERE outcome='priced'",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .map_err(|error| error.to_string())
            })
            .unwrap();
        assert_eq!(stored, (5, resolved_at_ms));
        assert_resolved_through_the_peer(&peer.finish(), event_at);
    }

    #[test]
    fn unconfigured_loop_records_named_unpriced_reason_without_panicking() {
        let state = test_state();
        run_one_iteration(&state, event(1_000));
        let stored: (i64, String) = state
            .with_conn(|conn| {
                conn.query_row(
                    "SELECT COUNT(*),MIN(unpriced_reason) FROM priced_charge WHERE outcome='unpriced'",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .map_err(|error| error.to_string())
            })
            .unwrap();
        assert_eq!(stored, (5, "catalog_unconfigured".into()));
    }

    #[test]
    fn live_catalog_non_nine_exponent_refuses_without_priced_charge_rows() {
        let event_at = 1_000;
        let peer = CatalogPeer::start(event_at - 1, 6, 55_555);
        let state = test_state();
        install_live_catalog(&state, &peer.path);

        run_one_iteration(&state, event(event_at));

        let count = state
            .with_conn(|conn| {
                conn.query_row("SELECT COUNT(*) FROM priced_charge", [], |row| {
                    row.get::<_, i64>(0)
                })
                .map_err(|error| error.to_string())
            })
            .unwrap();
        assert_eq!(count, 0);
        // The refusal must come from the rate the peer served, not from a
        // resolution that never reached it.
        assert_resolved_through_the_peer(&peer.finish(), event_at);
    }

    #[test]
    fn startup_probe_stores_the_boundary_refused_by_the_live_peer() {
        let event_at = 1_000;
        let boundary = event_at + i64::from(std::process::id() % 97) + 1;
        let peer = CatalogPeer::start(boundary, 9, 66_666);
        let state = Arc::new(test_state());
        let mut config = crate::config::intentional_empty().config;
        config.fusiform_connection_file = Some(peer.path.to_string_lossy().into_owned());
        state.publish_desired(config, 0);
        let stop = Arc::new(AtomicBool::new(false));
        // The live refresher loop runs its own pricing pass on this state. If
        // it prices the drained event first, the test's own pass then finds
        // unpriced heads and re-resolves them, writing a second set of rows
        // that supersede the first: still five current heads, but ten rows,
        // which the count below (over all rows, not heads) reports as a
        // failure. Measured at 10 instead of 5 under load. It is not a double
        // commit: the generation check in `commit_reprice_in_tx` refuses a
        // plan that went stale. Holding the loop's pass makes the test the only
        // pricer. The hold also skips the pricing step inside
        // `run_one_iteration`, so the event is drained there and priced by a
        // direct `price_unpriced_events` call, which the hold does not affect.
        // That is the same pricing pass the loop runs, against the catalog
        // the refresher's startup probe installed. That probe's first catalog
        // call was refused by the fake peer with "history begins at
        // <boundary>", so the `catalog_pre_history` rows below still come
        // from the boundary the startup probe learned from that refusal.
        let _pricing_hold = refresher::hold_pricing_pass_for_test(&state);
        let refresher_stop = Arc::clone(&stop);
        let refresher = Background::new(
            "the refresher",
            refresher::start_refresher(Arc::clone(&state), Arc::clone(&stop)).unwrap(),
            move || refresher_stop.store(true, std::sync::atomic::Ordering::Relaxed),
        );
        assert_eq!(state.catalog_client().unwrap().history_start_ms(), boundary);

        run_one_iteration(&state, event(event_at));
        // Recorded at wall-clock time, as the loop's own pass records it.
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        let priced =
            refresher::price_unpriced_events(&state, now_ms, &AtomicBool::new(false)).unwrap();
        assert_eq!(priced, 1);
        let stored: (i64, String) = state
            .with_conn(|conn| {
                conn.query_row(
                    "SELECT COUNT(*),MIN(unpriced_reason) FROM priced_charge",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .map_err(|error| error.to_string())
            })
            .unwrap();
        assert_eq!(stored, (5, "catalog_pre_history".into()));
        refresher.stop_and_join().unwrap();
        // Only status and the refused time-zero read reached the peer: the event predates the
        // boundary, so no resolution for it may be asked of fusiform.
        let requests = peer.finish();
        assert_eq!(requests[0]["name"], OP_CATALOG_STATUS);
        assert_eq!(requests.len(), 2, "only discovery may reach the peer");
        assert!(
            requests
                .iter()
                .skip(1)
                .all(|request| request["arguments"]["at_ms"] == 0),
            "only the pre-history probe (at_ms 0) may reach the peer: {requests:?}"
        );
    }

    #[test]
    fn history_status_typed_boundary_skips_the_text_probe() {
        let boundary = 12_345;
        let peer =
            CatalogPeer::start_with_status(boundary, 9, 66_666, fixtures::StatusMode::Present);
        let state = test_state();
        let catalog = install_live_catalog(&state, &peer.path);
        assert_eq!(catalog.history_start_ms(), boundary);
        let requests = peer.finish();
        assert_eq!(
            requests.len(),
            1,
            "typed status must not send a text probe: {requests:?}"
        );
        assert_eq!(requests[0]["name"], OP_CATALOG_STATUS);
    }

    #[test]
    fn history_status_absent_boundary_uses_the_text_probe() {
        let boundary = 23_456;
        let peer = CatalogPeer::start(boundary, 9, 66_666);
        let state = test_state();
        let catalog = install_live_catalog(&state, &peer.path);
        assert_eq!(catalog.history_start_ms(), boundary);
        let requests = peer.finish();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0]["name"], OP_CATALOG_STATUS);
        assert_eq!(requests[1]["name"], OP_CATALOG_GET);
        assert_eq!(requests[1]["arguments"]["at_ms"], 0);
    }

    #[test]
    fn history_status_typed_boundary_marks_one_ms_before_as_pre_history() {
        let boundary = 45_678;
        let peer =
            CatalogPeer::start_with_status(boundary, 9, 66_666, fixtures::StatusMode::Present);
        let state = test_state();
        install_live_catalog(&state, &peer.path);
        run_one_iteration(&state, event(boundary - 1));
        let stored: (i64, String) = state
            .with_conn(|conn| {
                conn.query_row(
                    "SELECT COUNT(*),MIN(unpriced_reason) FROM priced_charge",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .map_err(|error| error.to_string())
            })
            .unwrap();
        assert_eq!(stored, (5, "catalog_pre_history".into()));
        let requests = peer.finish();
        assert_eq!(
            requests.len(),
            1,
            "pre-history pricing must not resolve against the peer"
        );
        assert_eq!(requests[0]["name"], OP_CATALOG_STATUS);
    }

    /// A timeout spends the call's entire budget. The following call must
    /// reconnect instead of reusing that socket, regardless of whether setup
    /// or the operation consumed the budget.
    #[test]
    fn fusiform_timeout_evicts_the_cached_connection_before_the_next_call() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let port = listener.local_addr().unwrap().port();
        let path = std::env::temp_dir().join(format!(
            "astrocyte-fusiform-timeout-{}-{port}.json",
            std::process::id()
        ));
        let connection = ConnectionInfo {
            schema: SCHEMA_VERSION,
            wire_version: None,
            endpoints: vec![Endpoint {
                host: "127.0.0.1".into(),
                port,
            }],
            key: vec![7; KEY_LEN],
            daemon_id: [9; 16],
            pid: std::process::id(),
            daemon_ver: "test".into(),
        };
        connection_file::write_atomic(&path, &connection).unwrap();
        // One entry per accepted connection, in accept order: whether the
        // client got past route.open and sent its request on it.
        let connections = Arc::new(std::sync::Mutex::new(Vec::<bool>::new()));
        let server_connections = Arc::clone(&connections);
        // The peer stops when released (or when the test drops the sender by
        // failing first), never when a count is reached: a peer that waits for
        // N accepts hangs the test if the client legitimately makes fewer (a
        // timeout during route.open is a connect failure and is not retried).
        // A shortfall must fail as an assertion, below.
        let (release, mut released) = tokio::sync::oneshot::channel::<()>();
        let task = std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async move {
                    let listener = tokio::net::TcpListener::from_std(listener).unwrap();
                    let mut held = Vec::new();
                    loop {
                        let accepted = tokio::select! {
                            accepted = listener.accept() => accepted,
                            _ = &mut released => break,
                        };
                        let Ok((mut stream, _)) = accepted else {
                            continue;
                        };
                        let index = {
                            let mut connections = server_connections.lock().unwrap();
                            connections.push(false);
                            connections.len() - 1
                        };
                        // The client can give up at any step of the setup, so
                        // each step ends the connection quietly.
                        let reached_request = async {
                            authenticate_server(
                                &mut stream,
                                &connection.key,
                                &connection.daemon_id,
                                &connection.daemon_ver,
                                CONNECT_TIMEOUT,
                            )
                            .await
                            .ok()?;
                            let open = read_frame(&mut stream).await.ok()??;
                            let response = Frame::build(
                                FrameType::Response,
                                Flags::new(false, Priority::Passive, false),
                                0,
                                0,
                                open.header.corr,
                                serde_json::to_vec(&json!({"route_channel":7,"route_epoch":1}))
                                    .unwrap(),
                            )
                            .unwrap();
                            write_frame(&mut stream, &response).await.ok()?;
                            stream.flush().await.ok()?;
                            // Read the request and then stay silent: the client
                            // must observe a timeout, never a FIN/RST. An early
                            // close here is the eviction arriving before the
                            // request was written.
                            read_frame(&mut stream).await.ok()??;
                            Some(())
                        };
                        let reached_request = tokio::select! {
                            reached = reached_request => reached.is_some(),
                            _ = &mut released => break,
                        };
                        if reached_request {
                            server_connections.lock().unwrap()[index] = true;
                            held.push(stream);
                        }
                    }
                    drop(held);
                })
        });
        let peer = Background::new("the silent fusiform peer", task, move || {
            let _ = release.send(());
        });
        // The peer answers setup and withholds the operation's response.
        // Both calls must open fresh connections; there is no time left for a
        // same-call retry after the deadline.
        let client =
            FusiformClient::new_with_request_timeout(&path, Duration::from_millis(500)).unwrap();
        for _ in 0..2 {
            let before = connections.lock().unwrap().len();
            let result = client.catalog_get(CatalogGetRequest {
                source: None,
                at_ms: Some(0),
                fact_prefixes: Some(vec!["rate.".into()]),
                include_retired: true,
                model_id: None,
                provider_id: None,
                auth_method: None,
            });
            assert!(result.is_err(), "the peer never answers a request");
            // The client cannot finish a handshake until the peer has
            // accepted, and the peer handles connections in order, so every
            // connection this call made is recorded by the time it returns.
            let made = connections.lock().unwrap()[before..].to_vec();
            assert!(
                !made.is_empty(),
                "a call after a timeout must open a new connection, not reuse the cached one"
            );
            assert_eq!(made.len(), 1, "a timeout leaves no budget for a retry");
        }
        peer.stop_and_join();
        std::fs::remove_file(path).unwrap();
    }

    /// Serves `death_code` for the first request on a cached route, then two
    /// calls: the first must retry on a newly opened route and the second must
    /// keep using it. The assertions are on the route epochs the peer saw.
    fn assert_route_death_evicts_the_cached_route(death_code: &'static str) {
        use crate::route_death_peer::RouteDeathPeer;
        let peer = RouteDeathPeer::start(
            death_code,
            json!({
                "source": "fake-live-fusiform",
                "resolved_at_ms": 1,
                "catalog_version": 1,
                "models": {},
                "withheld": [],
                "uncertain": [],
                "overridden": []
            }),
        );
        let client = FusiformClient::new(&peer.connection_file_path).unwrap();
        let request = || CatalogGetRequest {
            source: None,
            at_ms: Some(0),
            fact_prefixes: Some(vec!["rate.".into()]),
            include_retired: true,
            model_id: None,
            provider_id: None,
            auth_method: None,
        };

        let first = client.catalog_get(request());
        let second = client.catalog_get(request());

        peer.assert_route_reopened(FUSIFORM_MODULE_ID, death_code);
        assert!(
            first.is_ok(),
            "retry on the reopened route: {:?}",
            first.err()
        );
        assert!(second.is_ok(), "next call: {:?}", second.err());
        peer.finish();
    }

    #[test]
    fn fusiform_stale_route_epoch_evicts_the_cached_route() {
        assert_route_death_evicts_the_cached_route("stale_route_epoch");
    }

    #[test]
    fn fusiform_unknown_channel_evicts_the_cached_route() {
        assert_route_death_evicts_the_cached_route("unknown_channel");
    }

    #[test]
    fn fusiform_module_reloading_retries_on_the_live_route() {
        let peer = DeadlinePeer::start(vec![
            PeerReply::opened(Duration::ZERO),
            PeerReply::error("module_reloading", "provider restarting"),
            PeerReply::Frame(Duration::ZERO, FrameType::Response, json!({"models":{}})),
        ]);
        let client = FusiformClient::new(&peer.path).unwrap();
        assert_eq!(deadline_call(&client).unwrap(), json!({"models":{}}));
        let requests = peer.requests.lock().unwrap();
        assert_eq!(requests.len(), 3);
        assert_eq!(
            requests[1], requests[2],
            "retry the same operation, not route.open"
        );
    }

    #[test]
    fn fusiform_module_reloading_stops_after_two_attempts() {
        let peer = DeadlinePeer::start(vec![
            PeerReply::opened(Duration::ZERO),
            PeerReply::error("module_reloading", "first refusal"),
            PeerReply::error("module_reloading", "last refusal"),
        ]);
        let client = FusiformClient::new(&peer.path).unwrap();
        let error = deadline_call(&client).unwrap_err();
        assert_eq!(error.code, "module_reloading");
        assert_eq!(error.message, "last refusal");
        assert_eq!(peer.requests.lock().unwrap().len(), 3);
    }

    pub(crate) enum PeerReply {
        Frame(Duration, FrameType, Value),
        Pings,
    }

    impl PeerReply {
        pub(crate) fn opened(delay: Duration) -> Self {
            Self::Frame(
                delay,
                FrameType::Response,
                json!({"route_channel":7,"route_epoch":1}),
            )
        }

        pub(crate) fn error(code: &str, message: &str) -> Self {
            Self::Frame(
                Duration::ZERO,
                FrameType::Error,
                json!({"code":code,"message":message}),
            )
        }
    }

    /// Scripted replies include route.open so setup and retries spend the same
    /// time budget as the operation. Pings last beyond the tested deadline,
    /// then stop without answering, so an unbounded client still fails finitely.
    pub(crate) struct DeadlinePeer {
        pub(crate) path: PathBuf,
        pub(crate) requests: Arc<std::sync::Mutex<Vec<Value>>>,
        task: Option<Background<()>>,
    }

    impl DeadlinePeer {
        pub(crate) fn start(replies: Vec<PeerReply>) -> Self {
            let (listener, path, connection) = crate::test_support::fake_daemon("deadline", 7, 9);
            let requests = Arc::new(std::sync::Mutex::new(Vec::new()));
            let received = Arc::clone(&requests);
            let (release, mut released) = tokio::sync::oneshot::channel::<()>();
            let task = std::thread::spawn(move || {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap()
                    .block_on(async move {
                        let listener = tokio::net::TcpListener::from_std(listener).unwrap();
                        let mut replies = replies.into_iter();
                        loop {
                            let serve = async {
                                let (mut stream, _) = listener.accept().await.ok()?;
                                authenticate_server(
                                    &mut stream,
                                    &connection.key,
                                    &connection.daemon_id,
                                    &connection.daemon_ver,
                                    CONNECT_TIMEOUT,
                                )
                                .await
                                .ok()?;
                                while let Some(request) = read_frame(&mut stream).await.ok()? {
                                    if request.header.ty != FrameType::Request {
                                        continue;
                                    }
                                    received
                                        .lock()
                                        .unwrap()
                                        .push(serde_json::from_slice(&request.body).ok()?);
                                    match replies.next()? {
                                        PeerReply::Frame(delay, ty, body) => {
                                            tokio::time::sleep(delay).await;
                                            let response = Frame::build(
                                                ty,
                                                request.header.flags,
                                                request.header.channel,
                                                request.header.epoch,
                                                request.header.corr,
                                                serde_json::to_vec(&body).unwrap(),
                                            )
                                            .unwrap();
                                            write_frame(&mut stream, &response).await.ok()?;
                                            stream.flush().await.ok()?;
                                        }
                                        PeerReply::Pings => {
                                            for corr in 0..7 {
                                                let ping = Frame::build(
                                                    FrameType::Ping,
                                                    request.header.flags,
                                                    0,
                                                    0,
                                                    corr,
                                                    Vec::new(),
                                                )
                                                .unwrap();
                                                write_frame(&mut stream, &ping).await.ok()?;
                                                stream.flush().await.ok()?;
                                                tokio::time::sleep(Duration::from_millis(100))
                                                    .await;
                                            }
                                            // No response or close: only the client's timer
                                            // can finish this request.
                                        }
                                    }
                                }
                                Some(())
                            };
                            tokio::select! { _ = serve => {}, _ = &mut released => break }
                        }
                    });
            });
            Self {
                path,
                requests,
                task: Some(Background::new("the deadline peer", task, move || {
                    let _ = release.send(());
                })),
            }
        }
    }

    impl Drop for DeadlinePeer {
        fn drop(&mut self) {
            drop(self.task.take());
            let _ = std::fs::remove_file(&self.path);
        }
    }

    fn deadline_call(client: &FusiformClient) -> Result<Value, CatalogCallError> {
        client.call_tool(OP_CATALOG_GET, json!({}), "models")
    }

    #[test]
    fn fusiform_peer_call_deadline_is_not_extended_by_pings() {
        let peer = DeadlinePeer::start(vec![
            PeerReply::opened(Duration::ZERO),
            PeerReply::Pings,
            PeerReply::opened(Duration::ZERO),
            PeerReply::Pings,
        ]);
        let client =
            FusiformClient::new_with_request_timeout(&peer.path, Duration::from_millis(500))
                .unwrap();
        let started = std::time::Instant::now();
        let error = deadline_call(&client).unwrap_err();
        let elapsed = started.elapsed();
        assert!(
            elapsed < Duration::from_millis(750),
            "call lasted {elapsed:?}: {error:?}"
        );
        assert!(error.message.contains("response timeout"), "{error:?}");
        assert!(
            peer.requests.lock().unwrap().len() >= 2,
            "the operation must reach the peer"
        );
        assert!(client.live.lock().unwrap().is_none());
    }

    #[test]
    fn fusiform_peer_call_deadline_includes_route_open_and_retry() {
        let peer = DeadlinePeer::start(vec![
            PeerReply::opened(Duration::from_millis(300)),
            PeerReply::error("unknown_channel", "channel removed"),
            PeerReply::opened(Duration::from_millis(300)),
            PeerReply::Pings,
        ]);
        let client =
            FusiformClient::new_with_request_timeout(&peer.path, Duration::from_millis(500))
                .unwrap();
        let started = std::time::Instant::now();
        let error = deadline_call(&client).unwrap_err();
        let elapsed = started.elapsed();
        assert!(
            elapsed < Duration::from_millis(750),
            "setup/retry lasted {elapsed:?}: {error:?}"
        );
        assert!(error.message.contains("response timeout"), "{error:?}");
        assert_eq!(
            peer.requests.lock().unwrap().len(),
            3,
            "the retry opens but cannot send an operation after its deadline"
        );
    }

    #[test]
    fn fusiform_reconnect_failure_keeps_the_exchange_error() {
        let peer = DeadlinePeer::start(vec![
            PeerReply::opened(Duration::ZERO),
            PeerReply::error("unknown_channel", "last exchange detail"),
            PeerReply::error("target_unavailable", "route refusal detail"),
        ]);
        let client = FusiformClient::new(&peer.path).unwrap();
        let error = deadline_call(&client).unwrap_err();
        assert_eq!(error.code, "unavailable");
        assert!(error.message.contains("last exchange detail"), "{error:?}");
        assert!(
            error.message.contains("target_unavailable")
                && error.message.contains("route refusal detail"),
            "{error:?}"
        );
        assert_eq!(peer.requests.lock().unwrap().len(), 3);
    }
}

#[cfg(any(test, feature = "test-support"))]
pub(crate) mod fixtures {
    use super::*;
    use crate::test_support::{Background, HANG_GUARD};
    use astrocyte_core::ingest::EventRecordRequest;
    use std::sync::Arc;
    use subc_transport::auth::authenticate_server;
    use subc_transport::connection_file::ConnectionInfo;
    pub fn event(occurred_at_ms: i64) -> EventRecordRequest {
        let mut wire = crate::wire_map::wire_fixture();
        wire["occurred_at_ms"] = occurred_at_ms.into();
        crate::wire_map::map_fact(serde_json::from_value(wire).unwrap()).unwrap()
    }

    /// A fake fusiform peer serving status and catalog reads until the test ends. It
    /// answers the way fusiform does: a request for a time before `boundary`
    /// is refused as outside coverage, naming the boundary (this is the
    /// refusal older producers use to disclose their history boundary), and any
    /// later time is answered with a priced rate for every role. Status can
    /// disclose the same boundary, omit it, or fail before any catalog read.
    ///
    /// It serves until released, never for a fixed number of calls: how many
    /// calls reach it depends on the path the client takes (a reconnect after
    /// a timeout on a loaded runner adds one), and a fake that stopped early
    /// would refuse the next connect while one that waited for a call that
    /// never comes would hang the test. The tests assert on the recorded
    /// requests instead.
    pub struct CatalogPeer {
        pub path: PathBuf,
        /// The body of every `route.open`, in order.
        route_opens: Arc<std::sync::Mutex<Vec<Value>>>,
        /// The body of every request on an open route, in order.
        requests: Arc<std::sync::Mutex<Vec<Value>>>,
        task: Option<Background<()>>,
    }

    pub enum StatusMode {
        Present,
        Absent,
        Refused(&'static str),
        Transient { failures: usize },
    }

    impl CatalogPeer {
        pub fn start(boundary: i64, exponent: i64, resolved_at_ms: i64) -> Self {
            Self::start_with_status(boundary, exponent, resolved_at_ms, StatusMode::Absent)
        }

        pub fn start_with_status(
            boundary: i64,
            exponent: i64,
            resolved_at_ms: i64,
            status_mode: StatusMode,
        ) -> Self {
            let (listener, path, connection) = crate::test_support::fake_daemon("fusiform", 7, 9);
            let route_opens = Arc::new(std::sync::Mutex::new(Vec::new()));
            let requests = Arc::new(std::sync::Mutex::new(Vec::new()));
            let session = CatalogSession {
                connection,
                boundary,
                exponent,
                resolved_at_ms,
                status_mode,
                status_calls: std::sync::atomic::AtomicUsize::new(0),
                route_opens: Arc::clone(&route_opens),
                requests: Arc::clone(&requests),
            };
            let (release, mut released) = tokio::sync::oneshot::channel::<()>();
            let task = std::thread::spawn(move || {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap()
                    .block_on(async move {
                        let listener = tokio::net::TcpListener::from_std(listener).unwrap();
                        loop {
                            let accepted = tokio::select! {
                                accepted = listener.accept() => accepted,
                                _ = &mut released => break,
                            };
                            let Ok((stream, _)) = accepted else {
                                continue;
                            };
                            tokio::select! {
                                () = session.serve(stream) => {}
                                _ = &mut released => break,
                            }
                        }
                    })
            });
            Self {
                path,
                route_opens,
                requests,
                task: Some(Background::new("the fake fusiform peer", task, move || {
                    let _ = release.send(());
                })),
            }
        }

        /// Stops the peer, checks every route it opened and every request it
        /// received had the shape the real fusiform accepts, and returns the
        /// request bodies in order.
        pub fn finish(mut self) -> Vec<Value> {
            self.task.take().unwrap().stop_and_join();
            let route_opens = self.route_opens.lock().unwrap().clone();
            assert!(!route_opens.is_empty(), "the client never opened a route");
            for open in &route_opens {
                assert_eq!(open["target"]["module_id"], FUSIFORM_MODULE_ID);
                // The real daemon refuses a management_surface target for
                // fusiform with target_unavailable; a fake that accepted
                // either kind would let that regression back in green.
                assert_eq!(open["target"]["kind"], "tool_provider");
            }
            let requests = self.requests.lock().unwrap().clone();
            for body in &requests {
                assert!(body["name"] == OP_CATALOG_GET || body["name"] == OP_CATALOG_STATUS);
                if body["name"] == OP_CATALOG_STATUS {
                    serde_json::from_value::<StatusRequest>(body["arguments"].clone()).unwrap();
                } else {
                    serde_json::from_value::<CatalogGetRequest>(body["arguments"].clone()).unwrap();
                    assert!(
                        body["arguments"].get("auth_method").is_none(),
                        "ordinary reads must not send a new field to older producers"
                    );
                }
                assert!(
                    body.get("method").is_none(),
                    "method/params is the surface envelope, not a tool call"
                );
            }
            requests
        }
    }

    impl Drop for CatalogPeer {
        fn drop(&mut self) {
            // Stop the peer thread first; a test that failed before `finish`
            // reaches here with it still serving.
            drop(self.task.take());
            let _ = std::fs::remove_file(&self.path);
        }
    }

    /// What one fake peer connection needs; see `CatalogPeer`.
    struct CatalogSession {
        connection: ConnectionInfo,
        boundary: i64,
        exponent: i64,
        resolved_at_ms: i64,
        status_mode: StatusMode,
        status_calls: std::sync::atomic::AtomicUsize,
        route_opens: Arc<std::sync::Mutex<Vec<Value>>>,
        requests: Arc<std::sync::Mutex<Vec<Value>>>,
    }

    impl CatalogSession {
        /// Serves one connection until the client closes it. The client can
        /// give up at any step, so every step ends the connection quietly: a
        /// panic here would kill the peer and fail the test with a message
        /// about the fake rather than about the client.
        async fn serve(&self, mut stream: tokio::net::TcpStream) {
            // The client bounds the handshake with its own production
            // timeout; this one only stops a client that never finishes it.
            if authenticate_server(
                &mut stream,
                &self.connection.key,
                &self.connection.daemon_id,
                &self.connection.daemon_ver,
                HANG_GUARD,
            )
            .await
            .is_err()
            {
                return;
            }
            let Ok(Some(open)) = read_frame(&mut stream).await else {
                return;
            };
            let open_body: Value = serde_json::from_slice(&open.body).unwrap_or(Value::Null);
            self.route_opens.lock().unwrap().push(open_body);
            let opened = Frame::build(
                FrameType::Response,
                Flags::new(false, Priority::Passive, false),
                0,
                0,
                open.header.corr,
                serde_json::to_vec(&json!({"route_channel": 7, "route_epoch": 1})).unwrap(),
            )
            .unwrap();
            if write_frame(&mut stream, &opened).await.is_err() || stream.flush().await.is_err() {
                return;
            }
            while let Ok(Some(request)) = read_frame(&mut stream).await {
                let body: Value = serde_json::from_slice(&request.body).unwrap_or(Value::Null);
                self.requests.lock().unwrap().push(body.clone());
                let (ty, response_body) = self.answer(&body);
                let response = Frame::build(
                    ty,
                    Flags::new(false, Priority::Passive, false),
                    7,
                    1,
                    request.header.corr,
                    serde_json::to_vec(&response_body).unwrap(),
                )
                .unwrap();
                if write_frame(&mut stream, &response).await.is_err()
                    || stream.flush().await.is_err()
                {
                    return;
                }
            }
        }

        fn answer(&self, body: &Value) -> (FrameType, Value) {
            let boundary = self.boundary;
            if body["name"] == OP_CATALOG_STATUS {
                let call = self
                    .status_calls
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let code = match self.status_mode {
                    StatusMode::Refused(code) => Some(code),
                    StatusMode::Transient { failures } if call < failures => Some("unavailable"),
                    _ => None,
                };
                if let Some(code) = code {
                    return (
                        FrameType::Error,
                        json!({"code": code, "message": "status refused"}),
                    );
                }
                let mut status = json!({
                    "source": "fake-live-fusiform",
                    "catalog_version": 987,
                    "model_count": 1,
                    "era_count": 1,
                    "recent_polls": []
                });
                if !matches!(self.status_mode, StatusMode::Absent) {
                    status["history_begins_at_ms"] = boundary.into();
                }
                return (FrameType::Response, status);
            }
            let at_ms = body["arguments"]["at_ms"].as_i64().unwrap_or(0);
            if at_ms < boundary {
                return (
                    FrameType::Error,
                    json!({
                        "code": "no_coverage",
                        "message": format!(
                            "no record at {at_ms}: fusiform's history begins at {boundary}. An empty answer would claim the catalog was empty; it was unobserved."
                        )
                    }),
                );
            }
            let exponent = self.exponent;
            let resolved_at_ms = self.resolved_at_ms;
            let rate = json!({
                "state": "priced",
                "units": 3_000,
                "exponent": exponent,
                "currency": "USD",
                "unit_provenance": {"kind": "stated"}
            });
            (
                FrameType::Response,
                json!({
                            "source": "fake-live-fusiform",
                            "resolved_at_ms": resolved_at_ms,
                            "catalog_version": 987,
                            // ALL FIVE KEYS ARE LOAD-BEARING, AND THE
                            // TWO TESTS THAT REACH THIS FAKE HOLD THEM
                            // IN BOTH DIRECTIONS. `catalog_key_for_role`
                            // in pricing.rs maps five measurement roles
                            // onto these five names, and core's tests stub
                            // the catalog rather than serving it, so core
                            // holds none of the mappings. Measured:
                            //
                            //   mapping -> a wrong key   277 core PASS,
                            //                            these 2 FAIL
                            //   a key dropped from here  277 core PASS,
                            //                            these 2 FAIL
                            //
                            // Both because the drained event they price
                            // carries all five roles, so a key missing here
                            // leaves a measurement unpriced and the
                            // assertion fails. Keep that event's roles and
                            // this list the same size.
                            "models": {"anthropic/claude": {
                                "rate.input": rate,
                                "rate.cache_read": rate,
                                "rate.cache_write": rate,
                                "rate.output": rate,
                                "rate.reasoning": rate
                            }},
                            "withheld": [],
                            "uncertain": [],
                            "overridden": []
                }),
            )
        }
    }
}
