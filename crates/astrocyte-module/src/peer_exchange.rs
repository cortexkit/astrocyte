//! Attested peer routes, and framing for the standalone management driver.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use serde_json::json;
use subc_client_rs::{
    CallError, CallOptions, ClaimError, ConsumerError, ConsumerOptions, OutcomeUnknownCause,
    RouteHandle, SubcConsumer,
};
use subc_protocol::{Flags, Frame, FrameType, Priority, RouteTarget, SUBC_MODULE_ID_ENV};
use subc_transport::{read_frame, write_frame};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

#[derive(Clone)]
pub(crate) enum PeerAccess {
    Module(Arc<crate::quota_events::Control>),
    #[cfg(any(test, feature = "test-support"))]
    Fixture(std::path::PathBuf),
}

impl PeerAccess {
    pub(crate) async fn connect(&self, budget: Duration) -> Result<SubcConsumer, &'static str> {
        match self {
            Self::Module(control) => {
                let access = control
                    .access()
                    .ok_or("attested module identity unavailable")?;
                let nonce = subc_os::launch_nonce().map_err(|_| "launch identity unavailable")?;
                // The SDK derives its identity from the same process cache as HELLO.
                if std::env::var(SUBC_MODULE_ID_ENV).ok().as_deref() != Some(&access.module_id)
                    || nonce.as_ref().map(|nonce| nonce.value()) != Some(&access.nonce)
                {
                    return Err("attested module identity unavailable");
                }
                connect_consumer(&access.path, budget).await
            }
            #[cfg(any(test, feature = "test-support"))]
            Self::Fixture(path) => connect_consumer(path, budget).await,
        }
    }
}

async fn connect_consumer(path: &Path, budget: Duration) -> Result<SubcConsumer, &'static str> {
    tokio::time::timeout(budget, async {
        loop {
            match SubcConsumer::connect(path, ConsumerOptions::default()).await {
                // Just after the HELLO_ACK hook, the SDK exposes the serving
                // connection that issues credentials for consumer connections.
                // A reader may ask for one before that connection is ready.
                Err(ConsumerError::Connect { source, .. })
                    if source
                        .get_ref()
                        .and_then(|error| error.downcast_ref::<ClaimError>())
                        .is_some_and(|error| matches!(error, ClaimError::Unavailable)) =>
                {
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
                result => return result,
            }
        }
    })
    .await
    .map_err(|_| "daemon connection timeout")?
    .map_err(|_| "daemon connection unavailable")
}

pub(crate) fn options(budget: Duration) -> CallOptions {
    CallOptions {
        timeout: budget,
        route_retry_deadline: budget,
        priority: Priority::Passive,
        ..CallOptions::default()
    }
}

pub(crate) async fn open_route(
    consumer: &SubcConsumer,
    target: RouteTarget,
    session: &str,
    options: CallOptions,
) -> Result<RouteHandle, CallError> {
    consumer
        .open_route(target, identity(session), options)
        .await
}

fn identity(session: &str) -> subc_protocol::BindIdentity {
    serde_json::from_value(json!({"harness":"ck-astrocyte","session":session}))
        .expect("peer identity has the SDK bind shape")
}

pub(crate) enum ReadError {
    Unavailable(&'static str),
    Refused(subc_protocol::ErrorBody),
}

/// Send a read with the SDK's route caching and bounded reopen/backoff. A lost
/// connection can also be retried once: none of these operations writes.
pub(crate) async fn read_peer(
    access: &PeerAccess,
    cached: &mut Option<SubcConsumer>,
    target: RouteTarget,
    session: &str,
    body: Vec<u8>,
    deadline: tokio::time::Instant,
) -> Result<Vec<u8>, ReadError> {
    const TIMEOUT: ReadError = ReadError::Unavailable("response timeout");
    let remaining = || deadline.saturating_duration_since(tokio::time::Instant::now());
    let result = tokio::time::timeout_at(deadline, async {
        for attempt in 0..2 {
            if cached.is_none() {
                *cached = Some(
                    access
                        .connect(remaining().min(Duration::from_secs(2)))
                        .await
                        .map_err(ReadError::Unavailable)?,
                );
            }
            let consumer = cached.as_ref().expect("consumer was just connected");
            match consumer
                .call(
                    target.clone(),
                    identity(session),
                    body.clone(),
                    options(remaining()),
                )
                .await
            {
                Ok(reply) => return Ok(reply),
                Err(CallError::Module(error)) => return Err(ReadError::Refused(error)),
                Err(error) => {
                    *cached = None;
                    if remaining().is_zero()
                        || error.outcome_cause() == Some(OutcomeUnknownCause::Deadline)
                    {
                        return Err(TIMEOUT);
                    }
                    if attempt == 0
                        && error.outcome_cause() == Some(OutcomeUnknownCause::ConnectionFailed)
                    {
                        continue;
                    }
                    // Route-open refusals can echo a launch credential. Only
                    // fixed diagnostics cross that boundary, not daemon prose.
                    let reason = match error.route_open_refusal().map(|body| body.code.as_str()) {
                        Some(subc_protocol::error_codes::TARGET_UNAVAILABLE) => {
                            "route unavailable (target_unavailable)"
                        }
                        Some(subc_protocol::error_codes::UNKNOWN_MODULE) => {
                            "route unavailable (unknown_module)"
                        }
                        _ => "peer unavailable",
                    };
                    return Err(ReadError::Unavailable(reason));
                }
            }
        }
        unreachable!("last attempt cannot retry")
    })
    .await;
    result.unwrap_or_else(|_| {
        *cached = None;
        Err(TIMEOUT)
    })
}

/// Send one management request and service control Pings until its reply arrives.
pub async fn exchange<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    channel: u16,
    epoch: u32,
    corr: u64,
    body: Vec<u8>,
) -> Result<Frame, String> {
    let request = Frame::build(
        FrameType::Request,
        Flags::new(false, Priority::Interactive, false),
        channel,
        epoch,
        corr,
        body,
    )
    .map_err(|error| error.to_string())?;
    write_frame(stream, &request)
        .await
        .map_err(|error| format!("write: {error:?}"))?;
    stream.flush().await.map_err(|error| error.to_string())?;
    loop {
        let frame = read_frame(stream)
            .await
            .map_err(|error| format!("read: {error:?}"))?
            .ok_or_else(|| "connection closed; write outcome may be unknown".to_string())?;
        match frame.header.ty {
            FrameType::Ping => {
                let pong = Frame::build_with_version(
                    frame.header.ver,
                    FrameType::Pong,
                    frame.header.flags,
                    0,
                    0,
                    frame.header.corr,
                    Vec::new(),
                )
                .map_err(|error| error.to_string())?;
                write_frame(stream, &pong)
                    .await
                    .map_err(|error| format!("pong: {error:?}"))?;
                stream
                    .flush()
                    .await
                    .map_err(|error| format!("pong flush: {error}"))?;
            }
            FrameType::Response | FrameType::Error
                if frame.header.corr == corr
                    && frame.header.channel == channel
                    && frame.header.epoch == epoch =>
            {
                return Ok(frame);
            }
            _ => {}
        }
    }
}
