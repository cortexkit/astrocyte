//! A fake subc daemon for the route-death tests of both wire clients
//! (fusiform and QTA), which share subc-protocol's
//! `is_established_route_dead` and so share the scenario that proves it.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use serde_json::{json, Value};
use subc_protocol::{Flags, Frame, FrameType, Priority};
use subc_transport::auth::authenticate_server;

use subc_transport::{read_frame, write_frame};
use tokio::io::AsyncWriteExt;

use crate::test_support::{Background, HANG_GUARD};

/// The channel every route the peer opens is bound to. Only the epoch
/// changes between routes, exactly as when the daemon releases an epoch
/// and rebinds the same channel.
pub(crate) const ROUTE_CHANNEL: u16 = 7;

/// Accepts any number of connections and opens one route on each. The
/// Nth `route.open` (counting from 1) binds epoch N, so the epoch a
/// request carries says which `route.open` it is using. The first request
/// of the whole session is answered with an ERROR frame carrying the
/// route-death code under test; every later request gets the success body.
pub(crate) struct RouteDeathPeer {
    pub(crate) connection_file_path: PathBuf,
    /// The `target.module_id` of every `route.open`, in order.
    route_opens: Arc<Mutex<Vec<String>>>,
    /// `(channel, epoch)` from the header of every request, in order.
    requests: Arc<Mutex<Vec<(u16, u32)>>>,
    task: Option<Background<()>>,
}

impl RouteDeathPeer {
    pub(crate) fn start(death_code: &'static str, success_body: Value) -> Self {
        let (listener, connection_file_path, connection) =
            crate::test_support::fake_daemon("route-death", 7, 9);

        let route_opens = Arc::new(Mutex::new(Vec::new()));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let server_route_opens = Arc::clone(&route_opens);
        let server_requests = Arc::clone(&requests);
        let (release, mut released) = tokio::sync::oneshot::channel::<()>();
        let task = std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async move {
                    let listener = tokio::net::TcpListener::from_std(listener).unwrap();
                    let mut answered_death = false;
                    // Serve until released, never until a count is reached:
                    // a client that wrongly keeps its dead route makes fewer
                    // connections than a correct one, and that shortfall
                    // must fail as an assertion in the test, not hang here.
                    loop {
                        // Cancellation covers authentication and route opening too:
                        // an abandoned client may never send the next frame.
                        let serve = async {
                            let (mut stream, _) = listener.accept().await.ok()?;
                            authenticate_server(
                                &mut stream,
                                &connection.key,
                                &connection.daemon_id,
                                &connection.daemon_ver,
                                HANG_GUARD,
                            )
                            .await
                            .ok()?;
                            let open = read_frame(&mut stream).await.ok()??;
                            let open_body: Value = serde_json::from_slice(&open.body).ok()?;
                            let epoch = {
                                let mut opens = server_route_opens.lock().unwrap();
                                opens.push(
                                    open_body["target"]["module_id"]
                                        .as_str()
                                        .unwrap_or_default()
                                        .to_string(),
                                );
                                u32::try_from(opens.len()).unwrap()
                            };
                            let opened = Frame::build(
                                FrameType::Response,
                                Flags::new(false, Priority::Passive, false),
                                0,
                                0,
                                open.header.corr,
                                serde_json::to_vec(&json!({
                                    "op": "route.open",
                                    "route_channel": ROUTE_CHANNEL,
                                    "route_epoch": epoch,
                                }))
                                .unwrap(),
                            )
                            .unwrap();
                            write_frame(&mut stream, &opened).await.ok()?;
                            stream.flush().await.ok()?;

                            // Answer requests on this connection until the client
                            // drops it (after evicting the route) or the test ends.
                            while let Some(request) = read_frame(&mut stream).await.ok()? {
                                server_requests
                                    .lock()
                                    .unwrap()
                                    .push((request.header.channel, request.header.epoch));
                                let (ty, body) = if answered_death {
                                    (FrameType::Response, success_body.clone())
                                } else {
                                    answered_death = true;
                                    (
                                        FrameType::Error,
                                        json!({"code": death_code, "message": "route is gone"}),
                                    )
                                };
                                let response = Frame::build(
                                    ty,
                                    Flags::new(false, Priority::Passive, false),
                                    request.header.channel,
                                    request.header.epoch,
                                    request.header.corr,
                                    serde_json::to_vec(&body).unwrap(),
                                )
                                .unwrap();
                                write_frame(&mut stream, &response).await.ok()?;
                                stream.flush().await.ok()?;
                            }
                            Some(())
                        };
                        tokio::select! {
                            _ = serve => {},
                            _ = &mut released => break,
                        }
                    }
                });
        });

        Self {
            connection_file_path,
            route_opens,
            requests,
            task: Some(Background::new("the route-death peer", task, move || {
                let _ = release.send(());
            })),
        }
    }

    /// Asserts the effect of eviction after two calls: the route-death
    /// answer to the first request made the client open a second route,
    /// and both the retry and the following call used that route's epoch.
    /// A client that kept the cached route sends the released epoch 1
    /// again and never opens a second route.
    pub(crate) fn assert_route_reopened(&self, module_id: &str, death_code: &str) {
        assert_eq!(
            *self.requests.lock().unwrap(),
            vec![(ROUTE_CHANNEL, 1), (ROUTE_CHANNEL, 2), (ROUTE_CHANNEL, 2)],
            "a {death_code} error must evict the cached route: the retry and the next call \
             must use the newly opened route (epoch 2), never the released epoch 1",
        );
        assert_eq!(
            *self.route_opens.lock().unwrap(),
            vec![module_id.to_string(), module_id.to_string()],
            "a {death_code} error must cause exactly one new route.open",
        );
    }

    pub(crate) fn finish(mut self) {
        self.task.take().unwrap().stop_and_join();
    }
}

impl Drop for RouteDeathPeer {
    fn drop(&mut self) {
        drop(self.task.take());
        let _ = std::fs::remove_file(&self.connection_file_path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn route_death_peer_stops_while_authenticated_client_has_not_opened_a_route() {
        let peer = RouteDeathPeer::start("unknown_channel", json!({}));
        let metadata = subc_transport::connection_file::read(&peer.connection_file_path).unwrap();
        let endpoint = &metadata.endpoints[0];
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let stream = runtime.block_on(async {
            let mut stream =
                tokio::net::TcpStream::connect((endpoint.host.as_str(), endpoint.port))
                    .await
                    .unwrap();
            subc_transport::authenticate_client(&mut stream, &metadata, HANG_GUARD)
                .await
                .unwrap();
            stream
        });
        // Keep the socket open: only the peer's stop signal can end its wait
        // for route.open. finish must not depend on the client closing first.
        peer.finish();
        drop(stream);
    }
}
