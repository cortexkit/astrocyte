//! Bounds for tests that wait on threads, child processes and fake peers.
//!
//! A test may fail, but it must never hang, and work it started must not
//! outlive it. Every wait here has a deadline, and that deadline exists only
//! to turn a hang into a failure with a message: it is far longer than any
//! healthy run needs, even on a heavily loaded machine, so it never decides
//! whether a correct run passes.

use std::io::Read;
use std::process::{Command, Output, Stdio};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

/// How long a test waits for something that a healthy run does in well under
/// a second. Long enough that load on a shared runner cannot reach it, short
/// enough that a genuine hang fails within a minute instead of stalling the
/// whole suite until CI cancels it with no message.
pub const HANG_GUARD: Duration = Duration::from_secs(60);

/// How often bounded waits look again. Short against `HANG_GUARD`, and long
/// enough not to burn a core on a loaded runner.
const POLL: Duration = Duration::from_millis(10);

#[cfg(test)]
pub(crate) mod thalamus_payer {
    use crate::refresher::thalamus_payer_source::ThalamusPayerSource;
    use crate::state::{test_state, SharedState};
    use rusqlite::{params, Connection};
    use serde_json::Value;
    use std::path::Path;
    use tempfile::TempDir;

    const DDL: &str = r#"
CREATE TABLE payer_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE payer_session (harness TEXT NOT NULL, session TEXT NOT NULL,
 owner_principal TEXT, declared_at_ms INTEGER NOT NULL, PRIMARY KEY(harness,session));
CREATE TABLE payer_record (seq INTEGER PRIMARY KEY AUTOINCREMENT, attempt_id TEXT NOT NULL UNIQUE,
 harness TEXT NOT NULL, session TEXT NOT NULL, request_kind TEXT NOT NULL, step_ref TEXT,
 recorded_at_ms INTEGER NOT NULL, provider TEXT NOT NULL, model TEXT NOT NULL, charge_basis TEXT,
 auth_selection TEXT NOT NULL, account_ref TEXT, outcome TEXT, provider_request_id TEXT,
 provider_response_id TEXT, usage_json TEXT);
INSERT INTO payer_meta VALUES ('schema_version','1'),('incarnation','first');
"#;

    pub(crate) fn open_store(home: &Path) -> Connection {
        let path = home.join("cortexkit/thalamus-harness/payer/payer.db");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let store = Connection::open(path).unwrap();
        store.execute_batch(DDL).unwrap();
        store
    }

    pub(crate) fn open_pi_store(home: &Path) -> Connection {
        let store = open_store(home);
        store
            .execute(
                "INSERT INTO payer_session VALUES ('pi','session-a','owner-a',1000)",
                [],
            )
            .unwrap();
        store
    }

    pub(crate) fn fixture() -> (TempDir, ThalamusPayerSource, Connection, SharedState) {
        let home = TempDir::new().unwrap();
        let source = ThalamusPayerSource::from_data_home(home.path().to_path_buf());
        let store = open_pi_store(home.path());
        (home, source, store, test_state())
    }

    pub(crate) fn insert_recorded_row(store: &Connection, row: &Value) {
        let text = |name: &str| row[name].as_str().map(str::to_owned);
        store
            .execute(
                "INSERT INTO payer_record(attempt_id,harness,session,request_kind,step_ref,
                     recorded_at_ms,provider,model,charge_basis,auth_selection,account_ref,outcome,
                     provider_request_id,provider_response_id,usage_json)
                     VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15)",
                params![
                    text("attempt_id"),
                    text("harness"),
                    text("session"),
                    text("request_kind"),
                    text("step_ref"),
                    row["recorded_at_ms"].as_i64(),
                    text("provider"),
                    text("model"),
                    text("charge_basis"),
                    text("auth_selection"),
                    text("account_ref"),
                    text("outcome"),
                    text("provider_request_id"),
                    text("provider_response_id"),
                    text("usage_json"),
                ],
            )
            .unwrap();
    }
}

/// Bind a fake daemon and publish its connection file before starting the peer
/// thread, so client setup never races fixture publication.
pub(crate) fn fake_daemon(
    tag: &str,
    key_byte: u8,
    daemon_byte: u8,
) -> (
    std::net::TcpListener,
    std::path::PathBuf,
    subc_transport::connection_file::ConnectionInfo,
) {
    use subc_transport::connection_file::{
        self, ConnectionInfo, Endpoint, KEY_LEN, SCHEMA_VERSION,
    };
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let port = listener.local_addr().unwrap().port();
    let path = std::env::temp_dir().join(format!(
        "astrocyte-{tag}-{}-{port}.json",
        std::process::id()
    ));
    let connection = ConnectionInfo {
        schema: SCHEMA_VERSION,
        wire_version: None,
        endpoints: vec![Endpoint {
            host: "127.0.0.1".into(),
            port,
        }],
        key: vec![key_byte; KEY_LEN],
        daemon_id: [daemon_byte; 16],
        pid: std::process::id(),
        daemon_ver: "0.39.0".into(),
    };
    connection_file::write_atomic(&path, &connection).unwrap();
    (listener, path, connection)
}

/// Waits until `done` returns true. Panics naming `what` if it is still false
/// after `HANG_GUARD`.
#[track_caller]
pub fn wait_until(what: &str, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + HANG_GUARD;
    while !done() {
        assert!(
            Instant::now() < deadline,
            "{what} did not happen within {HANG_GUARD:?}"
        );
        std::thread::sleep(POLL);
    }
}

/// Waits for `handle` to finish within `HANG_GUARD`, returning whether it did.
fn finished_within_guard<T>(handle: &JoinHandle<T>) -> bool {
    let deadline = Instant::now() + HANG_GUARD;
    while !handle.is_finished() {
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(POLL);
    }
    true
}

/// Joins `handle`. Panics naming `what` if the thread is still running after
/// `HANG_GUARD`, and re-raises the thread's own panic if it panicked, so the
/// failure carries that thread's message rather than a bare join error.
#[track_caller]
pub fn join_within<T>(handle: JoinHandle<T>, what: &str) -> T {
    assert!(
        finished_within_guard(&handle),
        "{what} was still running {HANG_GUARD:?} after the test waited for it to finish"
    );
    match handle.join() {
        Ok(value) => value,
        Err(panic) => std::panic::resume_unwind(panic),
    }
}

/// A background thread the test stops and joins when it ends, however it
/// ends. On the normal path the test calls `stop_and_join`. If the test
/// panics first, dropping this signals the thread to stop and waits for it
/// (bounded), so a failed assertion does not leave the thread running against
/// state the next test may share, or spinning for the rest of the run.
pub struct Background<T> {
    what: &'static str,
    stop: Option<Box<dyn FnOnce() + Send>>,
    handle: Option<JoinHandle<T>>,
}

impl<T> Background<T> {
    /// `stop` is how the thread is told to finish: set its stop flag, drop
    /// the sender it waits on, close its connection.
    pub fn new(
        what: &'static str,
        handle: JoinHandle<T>,
        stop: impl FnOnce() + Send + 'static,
    ) -> Self {
        Self {
            what,
            stop: Some(Box::new(stop)),
            handle: Some(handle),
        }
    }

    /// Tells the thread to stop without waiting for it.
    pub fn signal_stop(&mut self) {
        if let Some(stop) = self.stop.take() {
            stop();
        }
    }

    /// Tells the thread to stop and joins it within `HANG_GUARD`, returning
    /// what it returned.
    pub fn stop_and_join(mut self) -> T {
        self.signal_stop();
        let handle = self.handle.take().expect("joined once");
        join_within(handle, self.what)
    }
}

impl<T> Drop for Background<T> {
    fn drop(&mut self) {
        let Some(handle) = self.handle.take() else {
            return;
        };
        self.signal_stop();
        // Already unwinding or not, a drop must not panic again: report a
        // thread that ignored its stop and leave it, since nothing else can
        // end a thread.
        if !finished_within_guard(&handle) {
            eprintln!(
                "{} was still running {HANG_GUARD:?} after it was told to stop; leaving it",
                self.what
            );
        }
    }
}

/// Runs `command` to completion like `Command::output`, but kills the child
/// and panics naming `what` if it has not exited within `HANG_GUARD`, with
/// whatever it printed so far.
#[track_caller]
pub fn output_within(command: &mut Command, what: &str) -> Output {
    let mut child = command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap_or_else(|error| panic!("could not start {what}: {error}"));
    // Drain both pipes while waiting: a child that fills a pipe nobody reads
    // blocks on its write and never exits.
    let drain = |pipe: Option<Box<dyn Read + Send>>| {
        std::thread::spawn(move || {
            let mut bytes = Vec::new();
            if let Some(mut pipe) = pipe {
                let _ = pipe.read_to_end(&mut bytes);
            }
            bytes
        })
    };
    let stdout = drain(child.stdout.take().map(|pipe| Box::new(pipe) as _));
    let stderr = drain(child.stderr.take().map(|pipe| Box::new(pipe) as _));
    let deadline = Instant::now() + HANG_GUARD;
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            // The pipes close when the child dies, so both readers end.
            let stdout = join_within(stdout, "the child's stdout reader");
            let stderr = join_within(stderr, "the child's stderr reader");
            panic!(
                "{what} did not exit within {HANG_GUARD:?} and was killed\nstdout:\n{}\nstderr:\n{}",
                String::from_utf8_lossy(&stdout),
                String::from_utf8_lossy(&stderr)
            );
        }
        std::thread::sleep(POLL);
    };
    Output {
        status,
        stdout: join_within(stdout, "the child's stdout reader"),
        stderr: join_within(stderr, "the child's stderr reader"),
    }
}

// Binary tests use the same fixtures and loop hooks as the library tests.
pub use crate::refresher::{
    hold_pricing_pass_for_test, log_capacity_failed, log_config_retained, log_pricing_failed,
    log_producer_drain_failed, reject_startup_bytes_for_test, start_refresher,
    start_refresher_with_opencode,
};
pub use crate::state::{leased_test_state, test_state};

pub mod catalog {
    pub use crate::fusiform_client::fixtures::{event, CatalogPeer};
}

pub mod opencode {
    pub use crate::refresher::opencode_source::fixtures::{
        opencode_runtime_row, slow_page_database, wait_until_drain_reaches_the_page_statement,
        STOP_BOUND,
    };
}

/// Read a fixture request, answering the SDK's optional description probe without counting it as a peer call.
pub async fn read_peer_request<S>(stream: &mut S) -> Option<subc_protocol::Frame>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    use subc_protocol::{Frame, FrameType};
    use tokio::io::AsyncWriteExt;
    loop {
        let request = subc_transport::read_frame(stream).await.ok()??;
        if request.header.ty != FrameType::Request {
            continue;
        }
        let body: serde_json::Value = serde_json::from_slice(&request.body).unwrap();
        if body["op"] != "server.describe" {
            return Some(request);
        }
        let response = Frame::build(
            FrameType::Error,
            request.header.flags,
            0,
            0,
            request.header.corr,
            br#"{"code":"unknown_op","message":"fixture has no description"}"#.to_vec(),
        )
        .unwrap();
        subc_transport::write_frame(stream, &response).await.ok()?;
        stream.flush().await.ok()?;
    }
}
