//! Standalone management consumer; never registers a provider or opens a ledger.
//!
//! Usage: spend_drive <connection-file> <operation> '<json params>'
//! The subc connection file is passed as the first argument; nothing is read
//! from a default location.
#![forbid(unsafe_code)]

// The in-process test server uses ops::dispatch so refusals and SQLite writes
// exercise the running module's implementation, not a client-side imitation.
#[cfg(test)]
use astrocyte_module::{ops, state, test_support::test_state};

use std::{io::Write, path::Path, time::Duration};

use astrocyte_module::config::DEFAULT_MODULE_ID;
use serde_json::{json, Value};
use subc_protocol::{Frame, FrameType};
use subc_transport::{authenticate_client, connection_file};
#[cfg(test)]
use subc_transport::{read_frame, write_frame};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;

const TIMEOUT: Duration = Duration::from_secs(15);
const REPRICE_NOTICE: &str = "a plan reprices events already recorded: the autonomous pass also takes unpriced heads whose reason is no_subscription_in_force and whose instant falls inside this plan's range, bounded per tick, draining until none remain. COVERAGE IS ONE-WAY: a covered head is a priced head, and reprice re-resolves only unpriced ones, so an event this plan covers cannot be uncovered by closing or superseding the plan.";

type Reply = Result<Value, (String, String)>;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.len() != 3 {
        eprintln!("usage: spend_drive <connection-file> <operation> '<json params>'");
        std::process::exit(2);
    }
    let params = match serde_json::from_str(&args[2]) {
        Ok(params) => params,
        Err(error) => {
            eprintln!("malformed: invalid JSON: {error}");
            std::process::exit(2);
        }
    };
    let reply = match tokio::time::timeout(
        TIMEOUT,
        connect_and_call(Path::new(&args[0]), &args[1], params),
    )
    .await
    {
        Ok(Ok(reply)) => reply,
        Ok(Err(error)) => Err(("transport".into(), error)),
        Err(_) => Err((
            "transport".into(),
            "request timeout; write outcome may be unknown; not retried".into(),
        )),
    };
    let code = render(
        &args[1],
        reply,
        &mut std::io::stdout(),
        &mut std::io::stderr(),
    )
    .unwrap_or_else(|error| {
        eprintln!("output: {error}");
        1
    });
    std::process::exit(code);
}

async fn connect_and_call(path: &Path, operation: &str, params: Value) -> Result<Reply, String> {
    let metadata =
        connection_file::read(path).map_err(|error| format!("connection file: {error:?}"))?;
    let endpoint = metadata
        .endpoints
        .first()
        .ok_or("no endpoint in connection file")?;
    let mut stream = TcpStream::connect((endpoint.host.as_str(), endpoint.port))
        .await
        .map_err(|error| format!("connect: {error}"))?;
    authenticate_client(&mut stream, &metadata, TIMEOUT)
        .await
        .map_err(|error| format!("auth: {error:?}"))?;
    // Mutations are never retried: losing a response does not mean the write failed.
    call(&mut stream, operation, params).await
}

async fn call<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    operation: &str,
    params: Value,
) -> Result<Reply, String> {
    let route = exchange(stream, 0, 0, 1, json!({
        "op": "route.open",
        "target": {"kind": "management_surface", "module_id": DEFAULT_MODULE_ID},
        "identity": {"project_root": "/", "harness": "ck-astrocyte", "session": format!("astro-spend-drive-{}", std::process::id())}
    })).await?;
    let route = match decode(route)? {
        Ok(route) => route,
        Err(refusal) => return Ok(Err(refusal)),
    };
    let channel = route["route_channel"]
        .as_u64()
        .and_then(|n| u16::try_from(n).ok())
        .filter(|n| *n != 0)
        .ok_or("route_channel missing or invalid")?;
    let epoch = route["route_epoch"]
        .as_u64()
        .and_then(|n| u32::try_from(n).ok())
        .ok_or("route_epoch missing or invalid")?;
    let response = exchange(
        stream,
        channel,
        epoch,
        2,
        json!({"method": operation, "params": params}),
    )
    .await?;
    decode(response)
}

async fn exchange<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    channel: u16,
    epoch: u32,
    corr: u64,
    body: Value,
) -> Result<Frame, String> {
    astrocyte_module::peer_exchange::exchange(
        stream,
        channel,
        epoch,
        corr,
        serde_json::to_vec(&body).map_err(|error| error.to_string())?,
        astrocyte_module::peer_exchange::ExchangeMode::Management,
    )
    .await
}

fn decode(frame: Frame) -> Result<Reply, String> {
    let body: Value = serde_json::from_slice(&frame.body).map_err(|error| error.to_string())?;
    if frame.header.ty == FrameType::Error {
        let code = body["code"].as_str().ok_or("refusal code missing")?;
        let message = body["message"].as_str().ok_or("refusal message missing")?;
        Ok(Err((code.into(), message.into())))
    } else {
        Ok(Ok(body))
    }
}

fn render(
    operation: &str,
    reply: Reply,
    stdout: &mut impl Write,
    stderr: &mut impl Write,
) -> std::io::Result<i32> {
    match reply {
        Ok(value) => {
            writeln!(stdout, "{value}")?;
            if operation == "spend.subscription.set" {
                writeln!(stderr, "{REPRICE_NOTICE}")?;
            }
            Ok(0)
        }
        Err((code, message)) => {
            writeln!(stderr, "{code}: {message}")?;
            Ok(1)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plan() -> Value {
        json!({"account_ref":"acct","provider":"provider","plan_name":"plan","recurring_cost_minor":100,"currency":"USD","amount_exponent":2,"period":"month","effective_from":1})
    }

    async fn drive(operation: &str, params: Value) -> (Reply, Value, state::SharedState) {
        drive_with_stale_reply(operation, params, false).await
    }

    async fn drive_with_stale_reply(
        operation: &str,
        params: Value,
        stale_reply: bool,
    ) -> (Reply, Value, state::SharedState) {
        let state = test_state();
        let (mut client, mut server) = tokio::io::duplex(65536);
        let fake = async {
            let route = read_frame(&mut server).await.unwrap().unwrap();
            let body: Value = serde_json::from_slice(&route.body).unwrap();
            assert_eq!(
                body["target"],
                json!({"kind":"management_surface","module_id":DEFAULT_MODULE_ID})
            );
            assert!(body["identity"]["session"]
                .as_str()
                .unwrap()
                .starts_with("astro-spend-drive-"));
            respond(
                &mut server,
                &route,
                FrameType::Response,
                json!({"route_channel":7,"route_epoch":3}),
            )
            .await;
            let request = read_frame(&mut server).await.unwrap().unwrap();
            assert_eq!((request.header.channel, request.header.epoch), (7, 3));
            let body: Value = serde_json::from_slice(&request.body).unwrap();
            let reply = ops::dispatch(
                &state,
                body["method"].as_str().unwrap(),
                body["params"].clone(),
            );
            let (ty, response) = match reply {
                Ok(result) => (FrameType::Response, json!({"result":result})),
                Err((code, message)) => (FrameType::Error, json!({"code":code,"message":message})),
            };
            if stale_reply {
                // A previous request's successful reply must not mask this request's refusal.
                let decoy = Frame::build(
                    FrameType::Response,
                    request.header.flags,
                    request.header.channel,
                    request.header.epoch,
                    request.header.corr - 1,
                    serde_json::to_vec(&json!({"result":{"plan_name":"stale-decoy"}})).unwrap(),
                )
                .unwrap();
                write_frame(&mut server, &decoy).await.unwrap();
            }
            respond(&mut server, &request, ty, response).await;
            body
        };
        let (reply, received) = tokio::time::timeout(TIMEOUT, async {
            tokio::join!(call(&mut client, operation, params), fake)
        })
        .await
        .expect("fake management exchange timed out");
        (reply.unwrap(), received, state)
    }

    async fn respond(
        stream: &mut tokio::io::DuplexStream,
        request: &Frame,
        ty: FrameType,
        body: Value,
    ) {
        let response = Frame::build(
            ty,
            request.header.flags,
            request.header.channel,
            request.header.epoch,
            request.header.corr,
            serde_json::to_vec(&body).unwrap(),
        )
        .unwrap();
        write_frame(stream, &response).await.unwrap();
    }

    #[tokio::test]
    async fn stale_correlation_reply_is_ignored_before_real_refusal() {
        let (reply, received, _) =
            drive_with_stale_reply("spend.subscription.set", json!({}), true).await;
        assert_eq!(
            received,
            json!({"method":"spend.subscription.set","params":{}})
        );
        let (code, message) =
            reply.expect_err("stale-decoy success must not replace the real refusal");
        assert_eq!(code, "malformed");
        assert_eq!(message, "subscription params: missing field `account_ref`");
    }

    #[tokio::test]
    async fn unknown_operation_reaches_module_and_returns_its_refusal() {
        let (reply, received, _) = drive("invented.future.operation", json!({"future":42})).await;
        assert_eq!(
            received,
            json!({"method":"invented.future.operation","params":{"future":42}})
        );
        assert_eq!(
            reply.unwrap_err(),
            (
                "unknown_method".into(),
                "unknown method 'invented.future.operation'".into()
            )
        );
    }

    #[tokio::test]
    async fn operator_param_reaches_module_and_is_refused_without_write() {
        let mut params = plan();
        params["operator"] = json!("forged");
        let (reply, received, state) = drive("spend.subscription.set", params.clone()).await;
        assert_eq!(received["params"], params);
        assert_eq!(reply.unwrap_err().0, "malformed");
        let count: i64 = state
            .with_conn(|conn| {
                conn.query_row("SELECT COUNT(*) FROM account_subscription", [], |row| {
                    row.get(0)
                })
                .map_err(|error| error.to_string())
            })
            .unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn subscription_notice_printed_only_after_success() {
        for params in [plan(), json!({})] {
            let (reply, received, _) = drive("spend.subscription.set", params.clone()).await;
            assert_eq!(received["params"], params);
            let success = reply.is_ok();
            let mut out = Vec::new();
            let mut err = Vec::new();
            render("spend.subscription.set", reply, &mut out, &mut err).unwrap();
            let err = String::from_utf8(err).unwrap();
            let sentence = REPRICE_NOTICE;
            assert_eq!(err.contains(sentence), success);
            if success {
                let response: Value = serde_json::from_slice(&out).unwrap();
                assert_eq!(response["result"]["account_ref"], "acct");
            }
        }
    }

    #[tokio::test]
    async fn refusal_exits_nonzero_with_code_and_message_on_stderr() {
        let (reply, _, _) = drive("spend.event.reprice", json!({})).await;
        let (code, message) = reply.clone().unwrap_err();
        let mut out = Vec::new();
        let mut err = Vec::new();
        assert_ne!(
            render("spend.event.reprice", reply, &mut out, &mut err).unwrap(),
            0
        );
        assert!(out.is_empty());
        assert_eq!(
            String::from_utf8(err).unwrap(),
            format!("{code}: {message}\n")
        );
    }
}
