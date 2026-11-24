use std::collections::HashSet;
use std::os::{fd::OwnedFd, unix::fs::MetadataExt};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use astrocyte_core::ingest::{record_event, EventRecordRequest};
use cortexkit_store::{open_sqlite, Isolation, StorageBackend, StorageDescriptor};
use rusqlite::{Connection, OpenFlags, OptionalExtension};
use serde::Serialize;
use serde_json::{json, Value};
use subc_os::registration_channel::{DAEMON_PID_ENV, REGISTRATION_FD_ENV};
use subc_protocol::claim::{
    Claim, ClaimMintReply, ClaimMintRequest, ClaimPresentReply, ClaimPresentRequest, ClaimPurpose,
};
use subc_protocol::{Flags, Frame, FrameType, ModuleHelloAckBody, ModuleHelloBody, Priority};
use subc_transport::auth::authenticate_server;
use subc_transport::connection_file::{self, ConnectionInfo, Endpoint, KEY_LEN, SCHEMA_VERSION};
use subc_transport::{read_frame, write_frame};
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UnixStream};
use tokio::task::JoinSet;

const BUDGET: Duration = Duration::from_secs(20);
const NONCE: &str = "supervised-peer-fixture-nonce";

type Claims = Arc<Mutex<HashSet<String>>>;

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn seed_event(data_home: &Path, event_at: i64) -> Connection {
    let path = cortexkit_store_types::sqlite_store_path(data_home.to_str().unwrap(), "astrocyte");
    let descriptor = StorageDescriptor {
        module_id: "astrocyte".into(),
        storage_namespace: "astrocyte".into(),
        isolation: Isolation::Module,
        backend: StorageBackend::Sqlite { path: path.clone() },
    };
    let store = open_sqlite(&descriptor).unwrap();
    astrocyte_core::store::migrate_store(&store).unwrap();
    drop(store);
    let mut conn = Connection::open(&path).unwrap();
    let event: EventRecordRequest = serde_json::from_value(json!({
        "producer":"pi", "producer_ref":"attested-pricing", "session":"session",
        "provider":"anthropic", "model":"claude", "occurred_at_ms":event_at,
        "charge_basis":"metered_api",
        "measurements":[{"role":"input", "unit":"token", "kind":"billed", "source":"usage.input",
            "quantity_units":2000000, "quantity_exponent":0}]
    }))
    .unwrap();
    record_event(&mut conn, &event, event_at).unwrap();
    Connection::open_with_flags(&path, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap()
}

#[tokio::test]
async fn supervised_startup_prices_with_only_the_supervisor_connection_file() {
    let directory = tempfile::tempdir().unwrap();
    let home = directory.path();
    let path = home.join("moved-daemon-location.json");
    let data_home = home.join("data");
    let event_at = chrono::Utc::now().timestamp_millis() - 1_000;
    let ledger = seed_event(&data_home, event_at);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let connection = ConnectionInfo {
        schema: SCHEMA_VERSION,
        wire_version: None,
        endpoints: vec![Endpoint {
            host: "127.0.0.1".into(),
            port: listener.local_addr().unwrap().port(),
        }],
        key: vec![7; KEY_LEN],
        daemon_id: [9; 16],
        pid: std::process::id(),
        daemon_ver: "0.41.0".into(),
    };
    connection_file::write_atomic(&path, &connection).unwrap();

    let (server, client) = std::os::unix::net::UnixStream::pair().unwrap();
    server.set_nonblocking(true).unwrap();
    let registration = UnixStream::from_std(server).unwrap();
    let fd: OwnedFd = client.into();
    let file = std::fs::File::from(fd);
    let inode = file.metadata().unwrap().ino();
    // No desired file: both old and new binaries start with intentional-empty
    // config, so the assertion distinguishes actual pricing from config parsing.
    let stderr = home.join("child.stderr");
    let mut child = ChildGuard(
        Command::new(super::ckdev_binary())
            .arg("--subc")
            .arg(&path)
            .current_dir(home)
            .env_clear()
            .env("HOME", home)
            .env("XDG_DATA_HOME", &data_home)
            .env("XDG_CONFIG_HOME", home.join("config"))
            .env("SUBC_MODULE_ID", "astrocyte")
            .env("SUBC_LAUNCH_NONCE", NONCE)
            .env(REGISTRATION_FD_ENV, format!("0:{inode}"))
            .env(DAEMON_PID_ENV, std::process::id().to_string())
            .stdin(Stdio::from(file))
            .stdout(Stdio::null())
            .stderr(std::fs::File::create(&stderr).unwrap())
            .spawn()
            .unwrap(),
    );

    let daemon = serve_daemon(registration, listener, connection, event_at);
    tokio::pin!(daemon);
    let priced = async {
        loop {
            let row: Option<(String, Option<i64>, Option<String>)> = ledger
                .query_row(
                    "SELECT outcome,charge_nanos,unpriced_reason FROM priced_charge",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .optional()
                .unwrap();
            if let Some(row) = row {
                break row;
            }
            assert!(
                child.0.try_wait().unwrap().is_none(),
                "module exited: {}",
                std::fs::read_to_string(&stderr).unwrap()
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    };
    let charge = tokio::time::timeout(BUDGET, async {
        tokio::select! {
            row = priced => row,
            () = &mut daemon => panic!("the daemon fixture stopped before pricing"),
        }
    })
    .await
    .expect("no charge committed through the attested Fusiform route");
    assert_eq!(charge, ("priced".into(), Some(6_000), None));
    let fetched_at: String = ledger
        .query_row("SELECT fetched_at FROM capacity_window", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(fetched_at, "2026-09-05T09:15:00Z");
}

async fn serve_daemon(
    mut registration: UnixStream,
    listener: TcpListener,
    connection: ConnectionInfo,
    event_at: i64,
) {
    authenticate_server(
        &mut registration,
        &connection.key,
        &connection.daemon_id,
        &connection.daemon_ver,
        BUDGET,
    )
    .await
    .unwrap();
    let hello = read_frame(&mut registration).await.unwrap().unwrap();
    assert_eq!(hello.header.ty, FrameType::Hello);
    let manifest: ModuleHelloBody = serde_json::from_slice(&hello.body).unwrap();
    assert_eq!(manifest.manifest.module_id, "astrocyte");
    reply(
        &mut registration,
        &hello,
        FrameType::HelloAck,
        ModuleHelloAckBody {
            negotiated_ver: subc_protocol::PROTOCOL_VERSION,
            subc_ops: vec!["claim.mint".into()],
            subc_capabilities: vec![],
            storage: None,
            machine_id: None,
        },
    )
    .await;

    let claims = Claims::default();
    let mut consumers = JoinSet::new();
    consumers.spawn(serve_claim_mints(registration, claims.clone()));
    loop {
        tokio::select! {
            socket = listener.accept() => {
                let (stream, _) = socket.unwrap();
                consumers.spawn(serve_consumer(stream, connection.clone(), claims.clone(), event_at));
            }
            Some(result) = consumers.join_next() => { result.unwrap(); }
        }
    }
}

async fn serve_claim_mints(mut registration: UnixStream, claims: Claims) {
    let mut issued = 0_u64;
    while let Some(frame) = read_frame(&mut registration).await.unwrap() {
        if frame.header.ty != FrameType::Request {
            continue;
        }
        let mint: ClaimMintRequest = serde_json::from_slice(&frame.body).unwrap();
        assert_eq!(mint.purpose, ClaimPurpose::SelfConnection);
        issued += 1;
        let claim = format!("{issued:022}");
        assert!(claims.lock().unwrap().insert(claim.clone()));
        reply(
            &mut registration,
            &frame,
            FrameType::Response,
            ClaimMintReply::new(Claim::new(claim), 0),
        )
        .await;
    }
}

async fn serve_consumer(
    mut stream: TcpStream,
    connection: ConnectionInfo,
    claims: Claims,
    event_at: i64,
) {
    if authenticate_server(
        &mut stream,
        &connection.key,
        &connection.daemon_id,
        &connection.daemon_ver,
        BUDGET,
    )
    .await
    .is_err()
    {
        return;
    }
    let Some(present) = read_frame(&mut stream).await.unwrap() else {
        return;
    };
    let request: ClaimPresentRequest = serde_json::from_slice(&present.body)
        .expect("the first consumer request must present a self claim");
    assert!(claims.lock().unwrap().remove(request.claim.expose_secret()));
    reply(
        &mut stream,
        &present,
        FrameType::Response,
        ClaimPresentReply::new(),
    )
    .await;

    let Some(open) = astrocyte_module::test_support::read_peer_request(&mut stream).await else {
        return;
    };
    let route: Value = serde_json::from_slice(&open.body).unwrap();
    assert_eq!(route["op"], "route.open");
    assert!(
        route.get("consumer_identity").is_none(),
        "claim-bound routes must not send a legacy nonce"
    );
    let module = route["target"]["module_id"].as_str().unwrap();
    let role = match module {
        "fusiform" => "tool_provider",
        "insula" => "management_surface",
        _ => {
            reply(
                &mut stream,
                &open,
                FrameType::Error,
                json!({"code":"unknown_module","message":"not installed in fixture"}),
            )
            .await;
            return;
        }
    };
    assert_eq!(route["target"]["kind"], role);
    reply(
        &mut stream,
        &open,
        FrameType::Response,
        json!({"op":"route.open","route_channel":7,"route_epoch":1}),
    )
    .await;
    while let Some(frame) = read_frame(&mut stream).await.unwrap() {
        if frame.header.ty != FrameType::Request {
            continue;
        }
        let request: Value = serde_json::from_slice(&frame.body).unwrap();
        let response = match module {
            "fusiform" => answer_catalog(&request, event_at),
            "insula" => {
                assert_eq!(request, json!({"method":"usage.get","params":{}}));
                json!({"result":[{"provider":"codex","apiProvider":"openai","account":"account-a",
                    "source":"oauth","fetchedAt":"2026-09-05T09:15:00Z","usage":{"primary":{"usedPercent":42.0}}}]})
            }
            _ => unreachable!(),
        };
        reply(&mut stream, &frame, FrameType::Response, response).await;
    }
}

fn answer_catalog(request: &Value, event_at: i64) -> Value {
    match request["name"].as_str().unwrap() {
        "catalog.status" => json!({"source":"supervised-catalog","catalog_version":987,
            "model_count":1,"era_count":1,"recent_polls":[],"history_begins_at_ms":1}),
        "catalog.get" => {
            let at = &request["arguments"]["at_ms"];
            // A current valuation read is not evidence for the event's dated charge.
            let models = if at == event_at {
                json!({"anthropic/claude":{"rate.input":{
                    "state":"priced","units":3000,"exponent":9,"currency":"USD","unit_provenance":{"kind":"stated"}
                }}})
            } else {
                json!({})
            };
            json!({"source":"supervised-catalog","resolved_at_ms":event_at,
                "catalog_version":987,"models":models,"withheld":[],"uncertain":[],"overridden":[]})
        }
        "catalog.history" => {
            json!({"source":"supervised-catalog","provider_id":request["arguments"]["provider_id"],
            "model_id":request["arguments"]["model_id"],"fact_prefix":"rate.","served_by_key":{},"eras":[]})
        }
        name => panic!("unexpected Fusiform call: {name}"),
    }
}

async fn reply(
    stream: &mut (impl AsyncWrite + Unpin),
    request: &Frame,
    kind: FrameType,
    body: impl Serialize,
) {
    let frame = Frame::build(
        kind,
        Flags::new(false, Priority::Passive, false),
        request.header.channel,
        request.header.epoch,
        request.header.corr,
        serde_json::to_vec(&body).unwrap(),
    )
    .unwrap();
    write_frame(stream, &frame).await.unwrap();
    stream.flush().await.unwrap();
}
