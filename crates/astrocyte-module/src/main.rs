#![forbid(unsafe_code)]

//! `ck-astrocyte`: the serving, startup, and manifest boundary for Astrocyte.
//!
//! Startup has one ordering rule: desired configuration is classified and
//! hydrated before the subc connection is attempted. A malformed first
//! configuration therefore refuses startup instead of serving a placeholder.

#[cfg(test)]
use astrocyte_module::test_support;
use astrocyte_module::{config, ops, state};

use std::{
    collections::HashMap,
    error::Error,
    ffi::OsString,
    fmt,
    io::Write,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use astrocyte_core::store::MIGRATIONS;
use cortexkit_store::{open_sqlite, Isolation, StorageBackend, StorageDescriptor};
use cortexkit_store_types::module_store_path;
use serde::Deserialize;
use serde_json::{json, Value};
use state::{now_ms, PublicProjection, SharedState};
use subc_protocol::{
    manifest::{
        build_provenance_from_source, BuildGitShaAbsenceReason, BuildGitShaSource, Concurrency,
        ConsumerRole, GitTreeState, LaunchNonceSource, ManagementOperation,
        ManagementOperationKind, ModuleManifest, ProviderRole, SelfSignalDeclaration,
        SelfSignalEffect, SelfSignalKind, SignalAnchor, SignalCadence, TrustTier,
    },
    session::{
        HealthStatus, ModuleControlRequest, ModuleControlResponse, MODULE_CONTROL_OP_HEALTH_CHECK,
    },
    ErrorBody, Flags, Frame, FrameType, ModuleHelloAckBody, ModuleHelloBody, Priority,
    PROTOCOL_VERSION, SUBC_MODULE_ID_ENV,
};
use subc_transport::{authenticate_client, connection_file, read_frame, write_frame};
use tokio::{
    io::{AsyncRead, AsyncWrite, AsyncWriteExt, BufWriter},
    net::TcpStream,
    sync::mpsc,
};

use config::DEFAULT_MODULE_ID;
const HELLO_CORR: u64 = 1;
const EGRESS_BUFFER: usize = 64;

fn main() -> Result<std::process::ExitCode, Box<dyn Error>> {
    // `--version` answers before logging, the launch nonce or any config is
    // touched. SubC's placement script runs it to warm a freshly placed binary,
    // unsupervised and with no `--subc`; reaching startup there would log a
    // false module failure.
    if std::env::args_os()
        .nth(1)
        .is_some_and(|arg| arg == "--version")
    {
        println!("{}", version_line());
        return Ok(std::process::ExitCode::SUCCESS);
    }
    // This branch intentionally precedes --subc parsing, store acquisition, and
    // every network operation: config checking is a pure preflight command.
    if let Some(config_path) = parse_check_config_arg(std::env::args_os().skip(1))? {
        let stdout = std::io::stdout();
        check_config(&config_path, &mut stdout.lock())?;
        return Ok(std::process::ExitCode::SUCCESS);
    }

    // Logging and the store both live under the data home, so a home that
    // resolves to a relative path must be refused before either is created.
    // Logging isn't up yet, so the refusal goes to stderr, which the
    // supervisor captures.
    if let Err(message) = absolute_data_home(std::path::PathBuf::from(
        cortexkit_store_types::resolve_data_home(),
    )) {
        eprintln!("ck-astrocyte: {message}");
        return Ok(std::process::ExitCode::FAILURE);
    }
    let _log = init_logging()?;
    // Take the launch nonce before anything else starts. When the daemon hands
    // it over on an inherited descriptor, the first read closes that
    // descriptor, and every later call gets the cached answer. A handoff that
    // went wrong stops the module here, as subc-client-rs does: a HELLO
    // without the nonce would be refused, and reading the environment copy
    // instead would hide the broken handoff.
    if let Err(error) = subc_os::launch_nonce() {
        log_terminal_error(&error);
        return Ok(std::process::ExitCode::FAILURE);
    }
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let outcome = runtime.block_on(run_main());
    // Dropping the runtime drops every task still holding the shared state,
    // so the store is closed before `finish_process` can end the process.
    drop(runtime);
    Ok(finish_process(outcome))
}

/// Turn how serving ended into how the process ends.
///
/// A SIGTERM stop ends by SIGTERM itself. The supervisor treats a clean exit
/// as a deliberate stop and leaves the module down, but respawns a module
/// that died by a signal. Before the handler existed SIGTERM killed the
/// process outright, and the supervisor restarted it; exiting 0 after the
/// clean stop would turn every stray SIGTERM into an outage. So the clean
/// stop runs to completion first (refresher joined, both log lines written,
/// store closed by the caller), and only then does the process die by the
/// signal it was sent. EOF and GOODBYE still exit 0, and errors still exit 1.
///
/// Callers must have dropped the shared state before calling this, because a
/// SIGTERM outcome does not return.
fn finish_process(outcome: Result<LoopEnd, ModuleError>) -> std::process::ExitCode {
    match outcome {
        Ok(LoopEnd::Sigterm) => die_by_sigterm(),
        Ok(LoopEnd::Eof | LoopEnd::Goodbye) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            log_terminal_error(&error);
            std::process::ExitCode::FAILURE
        }
    }
}

/// Restore SIGTERM's default disposition and raise it, so the parent sees
/// death by signal 15 exactly as it did before `sigterm_received` installed a
/// handler. The logger writes each line to its file synchronously with no
/// buffer, so nothing logged before this call is lost.
#[cfg(unix)]
fn die_by_sigterm() -> ! {
    // With the default disposition restored, raising SIGTERM ends the
    // process; `emulate_default_handler` falls back to abort if it somehow
    // returns. Reaching the line below means even that failed.
    let result = signal_hook::low_level::emulate_default_handler(signal_hook::consts::SIGTERM);
    tracing::error!(target: "astrocyte", ?result, "re-raising SIGTERM did not end the process");
    std::process::abort()
}

#[cfg(not(unix))]
fn die_by_sigterm() -> ! {
    // There is no SIGTERM to re-raise; the non-unix `sigterm_received`
    // never resolves, so this is unreachable there.
    std::process::exit(1)
}

/// Refuse a data home that isn't absolute.
///
/// The shared data-home lookup ends in a relative `.local/share` when no home
/// variable is set, for example under a supervisor that exports none. Joined
/// against the working directory, that would create a second, empty ledger
/// and its logs wherever the process was launched, and that ledger would look
/// healthy. The config path and the BROCA store path already refuse the same
/// case.
fn absolute_data_home(home: std::path::PathBuf) -> Result<std::path::PathBuf, String> {
    if !home.is_absolute() {
        return Err(format!(
            "cannot resolve the data directory: the data home resolved to the relative \
             {home:?}, which means none of XDG_DATA_HOME, HOME (or on Windows APPDATA and \
             USERPROFILE) is set; a relative path would create a second, empty ledger under \
             the working directory"
        ));
    }
    Ok(home)
}

fn log_terminal_error(error: &dyn fmt::Display) {
    tracing::error!(target: "astrocyte", %error, "module stopped");
}

/// Initialise the fleet logger with THIS module's own view of its identity.
///
/// `init_from_env` REFUSES when SUBC_MODULE_ID is absent, while this module's
/// own resolver (`ModuleConfig::from_env`) FALLS BACK to the compiled-in id.
/// Calling the crate's function directly would put two readers of one variable
/// in one binary that disagree about whether its absence is fatal. SYNAPSE
/// found the same pair in their seat, where it panicked; here it would refuse
/// by naming a variable no operator sets by hand, when the real mistake is a
/// missing --subc.
///
/// The supervised path is unchanged --- with the variable present this IS
/// `init_from_env`, including the CK_LOG_MAX_AGE_DAYS and
/// CK_LOG_ALARM_SEGMENT_MB knobs the daemon injects. Only the absent arm is
/// answered locally, with the id we already know.
///
/// This also removes a trap with an ordinary trigger: `--check-config` runs
/// unsupervised and survives today only because it returns BEFORE logging is
/// initialised. Moving initialisation earlier is the most common refactor
/// there is, and under the bare call it would have broken the deploy preflight.
fn init_logging() -> Result<cortexkit_log::Handle, cortexkit_log::InitError> {
    match cortexkit_log::init_from_env() {
        Err(cortexkit_log::InitError::ModuleIdNotInEnvironment) => {
            cortexkit_log::init(cortexkit_log::Config::for_module(DEFAULT_MODULE_ID))
        }
        outcome => outcome,
    }
}

async fn run_main() -> Result<LoopEnd, ModuleError> {
    let module_config = ModuleConfig::from_env()?;
    let state = open_state()?;
    run(module_config, state).await
}

fn check_config(config_path: &Path, output: &mut dyn Write) -> Result<(), ModuleError> {
    match config::classify_read_result(std::fs::read(config_path)) {
        config::ConfigReadOutcome::Contents(contents) => config::decode_bytes(&contents)
            .map(|_| ())
            .map_err(|error| {
                ModuleError::Message(format!(
                    "config file was read but could not be decoded: {error}"
                ))
            }),
        config::ConfigReadOutcome::Missing => writeln!(
            output,
            "no config file is present; the daemon treats this as intentional empty"
        )
        .map_err(|error| ModuleError::Message(format!("report config check: {error}"))),
        config::ConfigReadOutcome::Unreadable(error) => Err(ModuleError::Message(format!(
            "config file could not be read ({:?}): {error}",
            error.kind()
        ))),
    }
}

fn open_state() -> Result<Arc<SharedState>, ModuleError> {
    open_state_at_store(&module_store_path(DEFAULT_MODULE_ID))
}

fn storage_descriptor_at_store(store_path: &str) -> StorageDescriptor {
    StorageDescriptor {
        module_id: DEFAULT_MODULE_ID.to_string(),
        storage_namespace: "astrocyte".to_string(),
        isolation: Isolation::Module,
        backend: StorageBackend::Sqlite {
            path: store_path.to_string(),
        },
    }
}

fn open_state_at_store(store_path: &str) -> Result<Arc<SharedState>, ModuleError> {
    let descriptor = storage_descriptor_at_store(store_path);
    let store = open_sqlite(&descriptor)
        .map_err(|error| ModuleError::Message(format!("open store: {error:?}")))?;
    // The lease is acquired by open_sqlite before migration. No serving socket
    // is opened until both the lease and migration have succeeded.
    let outcome = astrocyte_core::store::migrate_store(&store)
        .map_err(|error| ModuleError::Message(format!("migrate: {error:?}")))?;
    refuse_store_ahead_of_binary(&outcome)?;
    SharedState::from_store(store)
        .map(Arc::new)
        .map_err(ModuleError::Message)
}

/// The shared migrator deliberately skips every migration at or below the
/// store's recorded version and returns Ok: binary rollback against a newer
/// store is a supported recovery path fleet-wide, and the migrator cannot know
/// whether a given module's chain is additive. What it now reports is whether
/// it declined -- a run with nothing to apply and a run that skipped because
/// the store is ahead used to return the same unit value. Astrocyte's chain is
/// NOT rollback-safe (v2 replaced the v1 schema wholesale), so a store ahead
/// of this binary would serve and fail at the first query against a table it
/// never created: the process up, health reachable, no request answerable.
/// Refuse before any socket opens, naming both versions so the operator reads
/// a store/binary mismatch rather than a query defect.
fn refuse_store_ahead_of_binary(
    outcome: &cortexkit_store::MigrationOutcome,
) -> Result<(), ModuleError> {
    if outcome.store_ahead() {
        return Err(ModuleError::Message(format!(
            "store schema version {} is ahead of this binary's highest migration {}; \
             refusing to serve a store this binary does not understand",
            outcome.recorded, outcome.chain_max
        )));
    }
    Ok(())
}

struct ModuleConfig {
    connection_file_path: PathBuf,
    module_id: String,
}

impl ModuleConfig {
    fn from_env() -> Result<Self, ModuleError> {
        let connection_file_path = parse_subc_arg(std::env::args_os().skip(1))?;
        let module_id = std::env::var(SUBC_MODULE_ID_ENV)
            .ok()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| DEFAULT_MODULE_ID.to_string());
        Ok(Self {
            connection_file_path,
            module_id,
        })
    }
}

async fn run(config: ModuleConfig, state: Arc<SharedState>) -> Result<LoopEnd, ModuleError> {
    let startup_state = Arc::clone(&state);
    let stream = connect_after_startup(
        move || astrocyte_module::hydrate_startup(&startup_state),
        || connect_to_subc(&config.connection_file_path),
    )
    .await?;
    // Installed before HELLO is sent, so any SIGTERM that reaches a module
    // the daemon has seen takes the clean stop below. A SIGTERM during
    // hydration or connect still has the default disposition: nothing has
    // started that needs stopping.
    let sigterm = sigterm_received()?;
    let (read_half, write_half) = tokio::io::split(stream);
    serve_connection(
        read_half,
        write_half,
        &config,
        state,
        astrocyte_module::start_refresher,
        sigterm,
    )
    .await
}

/// Resolves when the process receives SIGTERM.
///
/// launchd stopping the daemon SIGTERMs its whole process group, which
/// includes this module, and the supervisor's own teardown sends SIGTERM
/// before SIGKILL. Without a handler the default disposition ends the
/// process at once and skips the refresher stop and join that daemon EOF
/// runs. With it, SIGTERM ends `module_loop` and shutdown continues on that
/// same path.
///
/// tokio keeps its handler installed for the life of the process, so once
/// this future has fired (or been dropped) any further SIGTERM is recorded
/// and ignored: a second signal while stopping neither kills the process
/// nor starts a second join. The join is bounded on its own, and SIGKILL
/// remains the supervisor's backstop.
#[cfg(unix)]
fn sigterm_received() -> Result<impl std::future::Future<Output = ()>, ModuleError> {
    use tokio::signal::unix::{signal, SignalKind};
    let mut terminate = signal(SignalKind::terminate())
        .map_err(|error| ModuleError::Message(format!("install SIGTERM handler: {error}")))?;
    Ok(async move {
        // `None` means the signal driver is gone (runtime shutting down),
        // which is not a request to stop.
        if terminate.recv().await.is_none() {
            std::future::pending::<()>().await;
        }
    })
}

#[cfg(not(unix))]
fn sigterm_received() -> Result<impl std::future::Future<Output = ()>, ModuleError> {
    Ok(std::future::pending::<()>())
}

type RefresherThread = std::thread::JoinHandle<Result<(), String>>;

/// Serve until the daemon closes, a stop signal arrives, or an internal crash
/// is observed, then stop and join the refresher through one shutdown path.
/// The injected starter lets tests use the real serving owner with a slow or
/// crashed refresher. Clean stops return their `LoopEnd`; crashes return errors.
async fn serve_connection<R, W, Start, Stop>(
    mut read_half: R,
    write_half: W,
    config: &ModuleConfig,
    state: Arc<SharedState>,
    start_refresher: Start,
    stop_signal: Stop,
) -> Result<LoopEnd, ModuleError>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin + Send + 'static,
    Stop: std::future::Future<Output = ()>,
    Start: FnOnce(
            Arc<SharedState>,
            Arc<std::sync::atomic::AtomicBool>,
        ) -> Result<RefresherThread, String>
        + Send
        + 'static,
{
    let (tx, rx) = mpsc::channel::<Frame>(EGRESS_BUFFER);
    let writer = tokio::spawn(drain_writer(write_half, rx));

    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let starting_state = Arc::clone(&state);
    let starting_stop = Arc::clone(&stop);
    let refresher_thread =
        tokio::task::spawn_blocking(move || start_refresher(starting_state, starting_stop))
            .await
            .map_err(|error| ModuleError::Message(format!("refresher initialization: {error}")))?
            .map_err(|error| ModuleError::Message(format!("refresher thread: {error}")))?;

    // Joining on a blocking worker both observes an unexpected thread end
    // while serving and keeps the executor available during deliberate stops.
    let mut refresher_join = tokio::task::spawn_blocking(move || {
        refresher_thread
            .join()
            .map_err(|_| ModuleError::Message("internal crash: refresher thread panicked".into()))?
            .map_err(ModuleError::Message)
    });
    let mut joined = None;
    let loop_result = tokio::select! {
        biased;
        cause = state.crashed() => Err(ModuleError::Message(format!("internal crash: {cause}"))),
        result = &mut refresher_join => {
            let cause = match &result {
                Ok(Ok(())) => "refresher ended without a stop request".to_string(),
                Ok(Err(error)) => error.to_string(),
                Err(error) => format!("refresher monitor: {error}"),
            };
            joined = Some(result);
            Err(ModuleError::Message(format!("internal crash: {cause}")))
        }
        result = module_loop(
        &mut read_half,
        tx.clone(),
        config,
        Arc::clone(&state),
        stop_signal,
        ) => result,
    };
    let cause = match &loop_result {
        Ok(end) => end.as_str(),
        Err(_) => "error",
    };
    drop(tx);
    // These two lines bracket shutdown. When the daemon's drain reports that
    // some module still held work, they show whether astrocyte had already
    // stopped by that instant and how long its refresher took to stop.
    let stopping_at = std::time::Instant::now();
    // `cause` says what ended the loop: eof, goodbye, sigterm, or error.
    tracing::info!(target: "astrocyte", cause, clean = loop_result.is_ok(),
        "stopping refresher");
    // Every refresher stage honours this flag, including a SQLite statement
    // already running, so the join below takes seconds, well inside the
    // supervisor's drain window, rather than however long the stage would
    // have run.
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let refresher_result = match joined {
        Some(result) => result,
        None => refresher_join.await,
    }
    .map_err(|error| ModuleError::Message(format!("refresher monitor: {error}")))
    .and_then(|result| result);
    tracing::info!(target: "astrocyte",
        stop_ms = u64::try_from(stopping_at.elapsed().as_millis()).unwrap_or(u64::MAX),
        "refresher stopped, nothing further will be written");

    let writer_result = writer
        .await
        .map_err(|error| ModuleError::Message(error.to_string()));
    let loop_result = loop_result.and_then(|end| refresher_result.map(|()| end));
    let loop_result = match state.crash_cause() {
        Some(cause) => Err(ModuleError::Message(format!("internal crash: {cause}"))),
        None => loop_result,
    };
    match (loop_result, writer_result) {
        (Err(loop_error), _) => Err(loop_error),
        (Ok(end), Ok(Ok(()))) => Ok(end),
        (Ok(_), Ok(Err(writer_error))) => Err(writer_error),
        (Ok(_), Err(join_error)) => Err(join_error),
    }
}

async fn connect_after_startup<T, Startup, Connect, ConnectFuture>(
    startup: Startup,
    connect: Connect,
) -> Result<T, ModuleError>
where
    Startup: FnOnce() -> Result<(), String> + Send + 'static,
    Connect: FnOnce() -> ConnectFuture,
    ConnectFuture: std::future::Future<Output = Result<T, ModuleError>>,
{
    tokio::task::spawn_blocking(startup)
        .await
        .map_err(|error| ModuleError::Message(format!("startup hydration: {error}")))?
        .map_err(ModuleError::Message)?;
    connect().await
}

async fn connect_to_subc(connection_file_path: &Path) -> Result<TcpStream, ModuleError> {
    let connection = connection_file::read(connection_file_path)
        .map_err(|error| ModuleError::Message(format!("reading connection file: {error}")))?;
    let endpoint = connection
        .endpoints
        .first()
        .ok_or_else(|| ModuleError::Message("connection file has no endpoints".into()))?;
    let address = format!("{}:{}", endpoint.host, endpoint.port);
    let mut stream = TcpStream::connect(&address)
        .await
        .map_err(|error| ModuleError::Message(format!("connect {address}: {error}")))?;
    authenticate_client(&mut stream, &connection, std::time::Duration::from_secs(2))
        .await
        .map_err(|error| ModuleError::Message(format!("authenticate: {error}")))?;
    Ok(stream)
}

/// What ended `module_loop` cleanly; logged as the shutdown `cause`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LoopEnd {
    /// The daemon's connection reached EOF.
    Eof,
    /// The daemon sent GOODBYE on the control channel.
    Goodbye,
    /// The process received SIGTERM.
    Sigterm,
}

impl LoopEnd {
    fn as_str(self) -> &'static str {
        match self {
            Self::Eof => "eof",
            Self::Goodbye => "goodbye",
            Self::Sigterm => "sigterm",
        }
    }
}

/// Only the waits for the daemon's next frame race `stop_signal`; a frame
/// already being handled finishes before the loop returns.
async fn module_loop<R, Stop>(
    read_half: &mut R,
    writer: mpsc::Sender<Frame>,
    config: &ModuleConfig,
    state: Arc<SharedState>,
    stop_signal: Stop,
) -> Result<LoopEnd, ModuleError>
where
    R: AsyncRead + Unpin,
    Stop: std::future::Future<Output = ()>,
{
    tokio::pin!(stop_signal);
    send_hello(&writer, config, &state.desired_config(), Some(&state)).await?;
    tokio::select! {
        ack = expect_hello_ack(read_half) => {
            ack?;
        }
        () = &mut stop_signal => return Ok(LoopEnd::Sigterm),
    }
    let public_projection = state.public_projection();
    let flow_routes = FlowRoutes::default();
    loop {
        let frame = tokio::select! {
            frame = read_frame(read_half) => {
                frame.map_err(|error| ModuleError::Message(error.to_string()))?
            }
            () = &mut stop_signal => return Ok(LoopEnd::Sigterm),
        };
        let Some(frame) = frame else {
            return Ok(LoopEnd::Eof);
        };
        if !handle_frame(frame, &writer, &state, &public_projection, &flow_routes).await? {
            return Ok(LoopEnd::Goodbye);
        }
    }
}

async fn drain_writer<W>(write_half: W, mut rx: mpsc::Receiver<Frame>) -> Result<(), ModuleError>
where
    W: AsyncWrite + Unpin,
{
    let mut writer = BufWriter::new(write_half);
    while let Some(frame) = rx.recv().await {
        write_frame(&mut writer, &frame)
            .await
            .map_err(|error| ModuleError::Message(error.to_string()))?;
        while let Ok(frame) = rx.try_recv() {
            write_frame(&mut writer, &frame)
                .await
                .map_err(|error| ModuleError::Message(error.to_string()))?;
        }
        writer
            .flush()
            .await
            .map_err(|error| ModuleError::Message(error.to_string()))?;
    }
    writer
        .flush()
        .await
        .map_err(|error| ModuleError::Message(error.to_string()))?;
    Ok(())
}

/// HELLO is sent once per connection, so the manifest declares the desired
/// configuration in force at this moment. Startup hydrates that configuration
/// before the subc connection exists, so it is always the real one here. A
/// later reload that adds or removes a peer is NOT re-announced: the daemon
/// keeps the self-signals and consumers declared here until the module
/// reconnects.
async fn send_hello(
    writer: &mpsc::Sender<Frame>,
    config: &ModuleConfig,
    desired: &config::AstrocyteConfig,
    state: Option<&SharedState>,
) -> Result<(), ModuleError> {
    let launch_nonce = subc_os::launch_nonce()
        .map_err(|error| ModuleError::Message(format!("launch nonce: {error}")))?
        .map(|nonce| (nonce.value().to_string(), nonce_source(nonce.source())));
    if let (Some(state), Some((nonce, _))) = (state, launch_nonce.as_ref()) {
        state.install_quota_bus_access(
            config.connection_file_path.clone(),
            config.module_id.clone(),
            nonce.clone(),
        );
    }
    let body = serde_json::to_vec(&hello_body(&config.module_id, desired, launch_nonce))
        .map_err(ModuleError::Json)?;
    let frame = Frame::build(FrameType::Hello, control_flags(), 0, 0, HELLO_CORR, body)
        .map_err(|error| ModuleError::Message(error.to_string()))?;
    send(writer, frame).await
}

/// The provenance name for where subc-os read the nonce from. A source added
/// to subc-os after this was written keeps its own name on the wire, so the
/// field never claims a source the nonce did not come from.
fn nonce_source(source: subc_os::LaunchNonceSource) -> LaunchNonceSource {
    match source {
        subc_os::LaunchNonceSource::Fd => LaunchNonceSource::Fd,
        subc_os::LaunchNonceSource::Env => LaunchNonceSource::Env,
        other => LaunchNonceSource::ForwardCompatibleUnknown(other.as_str().to_string()),
    }
}

/// The HELLO body, with the launch nonce and the place it was read from.
///
/// The daemon hands the nonce over either on an inherited descriptor or in
/// `SUBC_LAUNCH_NONCE`, and the provenance block states which one this
/// process read. With no nonce there is nothing to state, and the field stays
/// absent rather than naming a source nothing was read from.
fn hello_body(
    module_id: &str,
    desired: &config::AstrocyteConfig,
    launch_nonce: Option<(String, LaunchNonceSource)>,
) -> ModuleHelloBody {
    let mut manifest = manifest(module_id, desired);
    let launch_nonce = launch_nonce.map(|(value, source)| {
        if let Some(provenance) = manifest.provenance.as_mut() {
            provenance.launch_nonce_source = Some(source);
        }
        value
    });
    ModuleHelloBody {
        manifest,
        protocol_ver: PROTOCOL_VERSION,
        control_ops: Some(vec![MODULE_CONTROL_OP_HEALTH_CHECK.to_string()]),
        launch_nonce,
    }
}

async fn expect_hello_ack<R>(reader: &mut R) -> Result<ModuleHelloAckBody, ModuleError>
where
    R: AsyncRead + Unpin,
{
    let frame = read_frame(reader)
        .await
        .map_err(|error| ModuleError::Message(error.to_string()))?
        .ok_or_else(|| ModuleError::Message("connection closed before HELLO_ACK".into()))?;
    match frame.header.ty {
        FrameType::HelloAck => serde_json::from_slice(&frame.body).map_err(ModuleError::Json),
        FrameType::Error => {
            let body =
                serde_json::from_slice::<ErrorBody>(&frame.body).map_err(ModuleError::Json)?;
            Err(ModuleError::Message(format!(
                "subc rejected HELLO: {} — {}",
                body.code, body.message
            )))
        }
        ty => Err(ModuleError::Message(format!(
            "unexpected frame {ty:?} awaiting HELLO_ACK"
        ))),
    }
}

/// The flow each route was bound under, keyed by route channel and holding
/// the bind's epoch, for this connection only. Astrocyte checks no caller
/// identity on its writes, so a route a flow opened is refused every
/// mutating operation (see `handle_route_request`); reads stay served.
#[derive(Default)]
struct FlowRoutes(Mutex<HashMap<u16, (u32, String)>>);

impl FlowRoutes {
    /// Every bind replaces what the channel held before, so a channel reused
    /// by a route opened without a flow is never refused by a stale entry.
    fn record_bind(&self, channel: u16, epoch: u32, flow_id: Option<String>) {
        let mut routes = self.0.lock().unwrap_or_else(|poison| poison.into_inner());
        match flow_id {
            Some(flow_id) => {
                routes.insert(channel, (epoch, flow_id));
            }
            None => {
                routes.remove(&channel);
            }
        }
    }

    fn flow_of(&self, channel: u16, epoch: u32) -> Option<String> {
        let routes = self.0.lock().unwrap_or_else(|poison| poison.into_inner());
        routes
            .get(&channel)
            .filter(|(bound_epoch, _)| *bound_epoch == epoch)
            .map(|(_, flow_id)| flow_id.clone())
    }
}

/// The operations that change the money ledger. The module manifest built in
/// `manifest()` declares exactly these as `Mutate`, and a flow-stamped route
/// is refused exactly these.
fn is_mutating_operation(name: &str) -> bool {
    matches!(
        name,
        ops::OP_EVENT_INGEST
            | ops::OP_CORRECTION_INGEST
            | ops::OP_CHARGEABILITY_SETTLE
            | ops::OP_STATEMENT_IMPORT
            | ops::OP_SUBSCRIPTION_SET
            | ops::OP_SUBSCRIPTION_CLOSE
            | ops::OP_EVENT_REPRICE
            | ops::OP_BASIS_CORRECT
            | ops::OP_MULTIPLIER_RECOMPUTE
    )
}

async fn handle_frame(
    frame: Frame,
    writer: &mpsc::Sender<Frame>,
    state: &Arc<SharedState>,
    public_projection: &PublicProjection,
    flow_routes: &FlowRoutes,
) -> Result<bool, ModuleError> {
    match frame.header.ty {
        FrameType::Ping if frame.header.channel == 0 => {
            let pong = Frame::build_with_version(
                frame.header.ver,
                FrameType::Pong,
                frame.header.flags,
                0,
                0,
                frame.header.corr,
                Vec::new(),
            )
            .map_err(|error| ModuleError::Message(error.to_string()))?;
            send(writer, pong).await?;
            Ok(true)
        }
        FrameType::Goodbye if frame.header.channel == 0 => Ok(false),
        FrameType::Goodbye => Ok(true),
        FrameType::Request if frame.header.channel == 0 => {
            handle_control_request(frame, writer, public_projection, flow_routes).await?;
            Ok(true)
        }
        FrameType::Request => {
            let writer = writer.clone();
            let state = Arc::clone(state);
            let flow = flow_routes.flow_of(frame.header.channel, frame.header.epoch);
            let (ver, channel, epoch, corr) = (
                frame.header.ver,
                frame.header.channel,
                frame.header.epoch,
                frame.header.corr,
            );
            tokio::spawn(async move {
                let handler_writer = writer.clone();
                let handler_state = Arc::clone(&state);
                let result = tokio::spawn(async move {
                    handle_route_request(frame, &handler_writer, &handler_state, flow).await
                })
                .await;
                let (error, crashed) = match result {
                    Ok(Ok(())) => return,
                    Ok(Err(error)) => (error.to_string(), false),
                    Err(error) => (format!("route handler crashed: {error}"), true),
                };
                // Keep the fallback independent of the failed reply's payload:
                // an oversized refusal must not produce another oversized one.
                if crashed {
                    tracing::warn!(target: "astrocyte", channel, corr, %error, "route handler failed; attempting error reply before stopping");
                } else {
                    tracing::error!(target: "astrocyte", channel, corr, %error, "route handler failed");
                }
                if let Err(error) = send_route_error(
                    &writer,
                    ver,
                    channel,
                    epoch,
                    corr,
                    "internal",
                    "request could not be completed",
                )
                .await
                {
                    tracing::error!(target: "astrocyte", channel, corr, %error, "fallback error reply failed");
                }
                if crashed {
                    state.report_crash(error);
                }
            });
            Ok(true)
        }
        _ => Ok(true),
    }
}

async fn handle_control_request(
    frame: Frame,
    writer: &mpsc::Sender<Frame>,
    public_projection: &PublicProjection,
    flow_routes: &FlowRoutes,
) -> Result<(), ModuleError> {
    // A control request this build can't decode (a newer daemon's field or
    // op, such as a scope attribute added after this build's subc-protocol)
    // is refused on its own correlation id, and the module keeps serving.
    // Returning the decode error instead would end the serve loop and stop
    // the module, and every retry after the restart would stop it again.
    let request = match serde_json::from_slice::<ModuleControlRequest>(&frame.body) {
        Ok(request) => request,
        Err(error) => {
            tracing::warn!(
                target: "astrocyte",
                error = %error,
                "refused a control request this build cannot decode"
            );
            let body = serde_json::to_vec(&ErrorBody {
                code: "invalid_request".to_string(),
                message: format!("control request not understood by this build: {error}"),
                detail: None,
            })
            .map_err(ModuleError::Json)?;
            let refusal = Frame::build_with_version(
                frame.header.ver,
                FrameType::Error,
                control_flags(),
                0,
                0,
                frame.header.corr,
                body,
            )
            .map_err(|error| ModuleError::Message(error.to_string()))?;
            return send(writer, refusal).await;
        }
    };
    let response_body = match request {
        ModuleControlRequest::RouteBind {
            route_channel,
            epoch,
            scope,
            ..
        } => {
            let flow_id = scope.and_then(|stamp| stamp.attributes.flow_id);
            flow_routes.record_bind(route_channel, epoch, flow_id);
            ModuleControlResponse::RouteBindAck {}
        }
        ModuleControlRequest::HealthCheck {} => health_report(public_projection),
    };
    let body = serde_json::to_vec(&response_body).map_err(ModuleError::Json)?;
    let response = Frame::build_with_version(
        frame.header.ver,
        FrameType::Response,
        control_flags(),
        0,
        0,
        frame.header.corr,
        body,
    )
    .map_err(|error| ModuleError::Message(error.to_string()))?;
    send(writer, response).await
}

fn health_report(public_projection: &PublicProjection) -> ModuleControlResponse {
    let now_ms = now_ms();
    let health = public_projection.health(now_ms);
    let status = match health.status {
        astrocyte_module::state::SpendHealth::Ok => HealthStatus::Ok,
        astrocyte_module::state::SpendHealth::Degraded => HealthStatus::Degraded,
    };
    let metrics = json!({
        "degradedReason": health.degraded_reason,
        "capacityRuntime": health.capacity_runtime,
        "spendLastAttemptMs": health.spend_last_attempt_ms,
    });
    ModuleControlResponse::HealthCheck {
        status,
        detail: health.degraded_reason,
        metrics: Some(metrics),
    }
}

#[derive(Debug, Deserialize)]
struct RouteRequest {
    method: String,
    #[serde(default)]
    params: Value,
}

async fn handle_route_request(
    frame: Frame,
    writer: &mpsc::Sender<Frame>,
    state: &Arc<SharedState>,
    flow: Option<String>,
) -> Result<(), ModuleError> {
    let channel = frame.header.channel;
    let epoch = frame.header.epoch;
    let corr = frame.header.corr;
    let ver = frame.header.ver;
    let request: RouteRequest = match serde_json::from_slice(&frame.body) {
        Ok(request) => request,
        Err(error) => {
            return send_route_error(
                writer,
                ver,
                channel,
                epoch,
                corr,
                "invalid_request",
                &format!("request body not decodable: {error}"),
            )
            .await;
        }
    };
    // A flow acts for its owner without the owner present, and astrocyte
    // checks no caller identity on its writes. Until flows have a defined
    // place in the ledger, a route a flow opened may read but never write;
    // it is refused before dispatch, so nothing is touched.
    if let Some(flow_id) = flow {
        if is_mutating_operation(&request.method) {
            return send_route_error(
                writer,
                ver,
                channel,
                epoch,
                corr,
                "flow_scopes_unsupported",
                &format!(
                    "{} changes the ledger and is not served to a flow (flow {flow_id})",
                    request.method
                ),
            )
            .await;
        }
    }
    // Dispatch performs synchronous SQLite work and may bridge the fusiform
    // socket through its current-thread runtime. Leave the daemon executor
    // before entering that blocking boundary.
    match tokio::task::block_in_place(|| ops::dispatch(state, &request.method, request.params)) {
        Ok(result) => {
            let body =
                serde_json::to_vec(&json!({ "result": result })).map_err(ModuleError::Json)?;
            let response = Frame::build_with_version(
                ver,
                FrameType::Response,
                Flags::new(false, Priority::Interactive, false),
                channel,
                epoch,
                corr,
                body,
            )
            .map_err(|error| ModuleError::Message(error.to_string()))?;
            send(writer, response).await
        }
        Err((code, message)) => {
            send_route_error(writer, ver, channel, epoch, corr, &code, &message).await
        }
    }
}

async fn send_route_error(
    writer: &mpsc::Sender<Frame>,
    ver: u8,
    channel: u16,
    epoch: u32,
    corr: u64,
    code: &str,
    message: &str,
) -> Result<(), ModuleError> {
    let body = serde_json::to_vec(&ErrorBody {
        code: code.to_string(),
        message: message.to_string(),
        detail: None,
    })
    .map_err(ModuleError::Json)?;
    let frame = Frame::build_with_version(
        ver,
        FrameType::Error,
        Flags::new(false, Priority::Interactive, false),
        channel,
        epoch,
        corr,
        body,
    )
    .map_err(|error| ModuleError::Message(error.to_string()))?;
    send(writer, frame).await
}

async fn send(writer: &mpsc::Sender<Frame>, frame: Frame) -> Result<(), ModuleError> {
    writer
        .send(frame)
        .await
        .map_err(|_| ModuleError::Message("egress channel closed".into()))
}

fn control_flags() -> Flags {
    Flags::new(false, Priority::Passive, false)
}

/// The manifest is the public operation authority. Its operation entries and
/// the versioned schema artifact are assembled from one closed name list.
///
/// No `bindings` block. subc-protocol 0.19 made it optional because the daemon
/// reads it on no production path, and the block this used to carry declared
/// `StorageScope::Project` for a store that is machine-global -- a value chosen
/// to satisfy a required field, not to describe the module. Absent is the
/// honest form until the daemon reads it and the enum can say what is true.
///
/// `consumes` and `self_signals` describe what astrocyte will do under
/// `desired`, the configuration in force when HELLO is sent: see
/// [`declared_consumers`] and [`self_signal_declarations`].
fn manifest(module_id: &str, desired: &config::AstrocyteConfig) -> ModuleManifest {
    let operations = ops::PUBLIC_OPERATION_NAMES
        .iter()
        .map(|name| ManagementOperation {
            name: (*name).to_string(),
            kind: if is_mutating_operation(name) {
                ManagementOperationKind::Mutate
            } else {
                ManagementOperationKind::Query
            },
            description: Some(format!("Astrocyte operation {name}.")),
        })
        .collect();
    // Provenance is derived at build time by crates/astrocyte-module/build.rs,
    // which resolves HEAD from .git without spawning git and asks git only
    // whether the tree is clean. A clean tree publishes the sha; a dirty tree
    // DECLINES it (a sha that does not describe the bytes is worse than none);
    // no git binary means the sha was derived but its cleanliness was not, and
    // the open reason enum says exactly that rather than "never derived",
    // which would be false.
    let revision = env!("CK_ASTROCYTE_BUILD_GIT_SHA");
    let build_git_sha_source = match env!("CK_ASTROCYTE_BUILD_TREE_STATE") {
        "clean" => BuildGitShaSource::Git {
            revision,
            tree_state: GitTreeState::Clean,
        },
        "dirty" => BuildGitShaSource::Git {
            revision,
            tree_state: GitTreeState::Dirty,
        },
        "unknown" => BuildGitShaSource::NeverDerived,
        "no_git_dir" => BuildGitShaSource::NoGitDir,
        state => panic!("crates/astrocyte-module/build.rs emitted unknown tree state {state:?}"),
    };
    let store_schema_version = MIGRATIONS
        .iter()
        .map(|migration| migration.version)
        .max()
        .expect("MIGRATIONS is never empty")
        .to_string();
    let mut provenance = build_provenance_from_source(
        build_git_sha_source,
        Some(env!("CK_ASTROCYTE_BUILD_LOCK_DIGEST")),
        Some(&store_schema_version),
    )
    .expect("crates/astrocyte-module/build.rs emitted invalid manifest provenance");
    if env!("CK_ASTROCYTE_BUILD_TREE_STATE") == "unknown" {
        provenance.build_git_sha_absence_reason = Some(
            BuildGitShaAbsenceReason::ForwardCompatibleUnknown("tree_state_unknown".to_string()),
        );
    }
    ModuleManifest::builder(module_id.to_string(), env!("CARGO_PKG_VERSION").to_string())
        .trust_tier(Some(TrustTier::FirstParty))
        .provides(vec![ProviderRole::ManagementSurface {
            concurrency: Concurrency::Serial,
            operations,
            config_schema: ops::operation_schemas(),
            observability: Vec::new(),
            identity_scope: Vec::new(),
        }])
        .consumes(declared_consumers(desired))
        .capabilities(None)
        .self_signals(Some(self_signal_declarations(desired)))
        .provenance(Some(provenance))
        .build()
}

/// The modules astrocyte calls under `desired`, and no others. Quota ingestion
/// also uses ckbus through an attested route on the daemon connection file.
/// BROCA never is: its usage is read from its store, not over subc.
/// No configured peer means no consumer role at all.
fn declared_consumers(desired: &config::AstrocyteConfig) -> Vec<ConsumerRole> {
    let of: Vec<String> = [
        ("fusiform", desired.fusiform_connection_file.is_some()),
        ("insula", desired.qta_connection_file.is_some()),
        ("ckbus", desired.qta_connection_file.is_some()),
    ]
    .into_iter()
    .filter(|(_, configured)| *configured)
    .map(|(module, _)| module.to_string())
    .collect();
    if of.is_empty() {
        Vec::new()
    } else {
        vec![ConsumerRole::ServiceClient { of }]
    }
}

/// Every call astrocyte makes against another module's surface under
/// `desired`. The list is always present, even when empty, because a present
/// list is a survey answer: it claims to be complete, and an empty one claims
/// astrocyte touches no other surface.
///
/// The harness readers and BROCA's store reader are deliberately absent. They
/// read local files the producers write; they call no module and shape no
/// module's surface.
///
/// Names are stable identifiers that analysts key on; do not rename them.
fn self_signal_declarations(desired: &config::AstrocyteConfig) -> Vec<SelfSignalDeclaration> {
    // The tick interval bounds fallback polling and discovery retries.
    let tick_cadence = || SignalCadence::Derived {
        source: "desired_config.poll_interval_secs".to_string(),
    };
    let mut declarations = Vec::new();
    if desired.qta_connection_file.is_some() {
        // insula is the module that tells self-generated traffic from real
        // traffic, so this poll is the entry it most needs to see.
        declarations.push(SelfSignalDeclaration {
            name: "astrocyte-insula-usage-poll".to_string(),
            kind: SelfSignalKind::Poller,
            effect: SelfSignalEffect::Observe,
            anchored_to: SignalAnchor::FixedInterval,
            cadence: Some(tick_cadence()),
            domain: Some("insula".to_string()),
            note: Some("usage.get on owed snapshots; healthy-bus backstop is ten minutes or one cadence before the freshness horizon, whichever is sooner. Without the bus, poll every refresher tick.".to_string()),
        });
        declarations.push(SelfSignalDeclaration {
            name: "astrocyte-insula-quota-body".to_string(),
            kind: SelfSignalKind::Poller,
            effect: SelfSignalEffect::Observe,
            anchored_to: SignalAnchor::Event {
                event: "insula.quota_sampled".to_string(),
            },
            cadence: None,
            domain: Some("insula".to_string()),
            note: Some(
                "events_get fetches the immutable body of an in-sequence quota notice.".to_string(),
            ),
        });
        declarations.push(SelfSignalDeclaration {
            name: "astrocyte-ckbus-credentials".to_string(),
            kind: SelfSignalKind::Poller,
            effect: SelfSignalEffect::Mutate,
            anchored_to: SignalAnchor::Event { event: "quota_bus_session".to_string() },
            cadence: None,
            domain: Some("ckbus".to_string()),
            note: Some("ckbus.credential, ckbus.credential_renew and ckbus.nonce_sign authenticate the quota reader; failures retry with bounded backoff.".to_string()),
        });
    }
    if desired.fusiform_connection_file.is_some() {
        // Rate lookups are reads (Observe) driven by events, not by a clock,
        // so the anchor is the event and there is no cadence to declare.
        //
        // Kind: `Poller`, the kind for a read of another surface. `Busy` is
        // reserved for drain-quiescence gauges and must not be used for
        // work that is not one; `Sweep`, `Cron`, `Keepalive`, `Watchdog` and
        // `Heartbeat` describe other shapes; `Other` is the decoder's
        // fallback for kinds it does not know, so a reader would treat it as
        // unclassified. The `Event` anchor is what tells a reader this poller
        // is event-driven rather than periodic.
        declarations.push(SelfSignalDeclaration {
            name: "astrocyte-fusiform-catalog-lookup".to_string(),
            kind: SelfSignalKind::Poller,
            effect: SelfSignalEffect::Observe,
            anchored_to: SignalAnchor::Event {
                event: "usage_event_needs_rate".to_string(),
            },
            cadence: None,
            domain: Some("fusiform".to_string()),
            note: Some(
                "One catalog.get per usage event that needs a vendor rate, not batched: \
                 each event the pricing pass takes from its queue or coverage backfill, \
                 and each spend.event.reprice or spend.basis.correct request. Skipped \
                 for events that are subscription-covered, not metered, undated, \
                 missing a quantity or model identity, or before the catalog's history \
                 boundary."
                    .to_string(),
            ),
        });
        // Rate lookups need the catalog's history boundary first, and that
        // boundary is read from catalog.status, with a catalog.get fallback
        // for older producers. Discovery runs at startup and once per tick
        // until it succeeds. That retry is periodic and has no
        // triggering event, so it is declared separately.
        declarations.push(SelfSignalDeclaration {
            name: "astrocyte-fusiform-history-status".to_string(),
            kind: SelfSignalKind::Poller,
            effect: SelfSignalEffect::Observe,
            anchored_to: SignalAnchor::FixedInterval,
            cadence: Some(tick_cadence()),
            domain: Some("fusiform".to_string()),
            note: Some(
                "One catalog.status at startup, repeated once per refresher tick \
                 only until the history boundary is discovered; silent afterwards."
                    .to_string(),
            ),
        });
        declarations.push(SelfSignalDeclaration {
            name: "astrocyte-fusiform-history-probe".to_string(),
            kind: SelfSignalKind::Poller,
            effect: SelfSignalEffect::Observe,
            anchored_to: SignalAnchor::FixedInterval,
            cadence: Some(tick_cadence()),
            domain: Some("fusiform".to_string()),
            note: Some(
                "One pre-history catalog.get per discovery attempt only when catalog.status \
                 omits the boundary; silent after discovery."
                    .to_string(),
            ),
        });
        // The daily subscription multiplier reads fusiform on a clock, not per
        // event: once per quota provider per UTC day, on the first tick after
        // 00:00Z. It makes no call when no quota provider is declared, or when
        // the committed generation does not record the member.
        let multiplier_declared = matches!(
            &desired.subscription_multiplier,
            config::MultiplierSetting::Recorded(multiplier)
                if !multiplier.quota_providers.is_empty()
        );
        if multiplier_declared {
            let daily = || {
                Some(SignalCadence::Literal {
                    interval_ms: 86_400_000,
                })
            };
            declarations.push(SelfSignalDeclaration {
                name: "astrocyte-fusiform-plan-prices".to_string(),
                kind: SelfSignalKind::Poller,
                effect: SelfSignalEffect::Observe,
                anchored_to: SignalAnchor::FixedInterval,
                cadence: daily(),
                domain: Some("fusiform".to_string()),
                note: Some(
                    "One plan.prices per declared quota provider per UTC day, for the \
                     subscription multiplier, and one per quota provider a \
                     spend.multiplier.recompute request recomputes; \
                     skipped when every account of that provider has a configured price \
                     override."
                        .to_string(),
                ),
            });
            declarations.push(SelfSignalDeclaration {
                name: "astrocyte-fusiform-multiplier-rates".to_string(),
                kind: SelfSignalKind::Poller,
                effect: SelfSignalEffect::Observe,
                anchored_to: SignalAnchor::FixedInterval,
                cadence: daily(),
                domain: Some("fusiform".to_string()),
                note: Some(
                    "One catalog.get per model in a quota provider's subscription scope, per \
                     declared quota provider per UTC day, to value that day's multiplier \
                     observation at list prices, and the same set again for each quota \
                     provider a spend.multiplier.recompute request recomputes."
                        .to_string(),
                ),
            });
        }
    }
    declarations
}

/// One line naming this build: the crate version, then the commit build.rs
/// stamped and whether the working tree matched it when it was built.
fn version_line() -> String {
    let commit = match (
        env!("CK_ASTROCYTE_BUILD_GIT_SHA"),
        env!("CK_ASTROCYTE_BUILD_TREE_STATE"),
    ) {
        ("", _) => "commit unknown".to_string(),
        (sha, "clean") => format!("commit {sha}"),
        (sha, "dirty") => format!("commit {sha}, built from a dirty tree"),
        (sha, _) => format!("commit {sha}, tree state unknown"),
    };
    format!("ck-astrocyte {} ({commit})", env!("CARGO_PKG_VERSION"))
}

fn parse_check_config_arg(
    args: impl IntoIterator<Item = OsString>,
) -> Result<Option<PathBuf>, ModuleError> {
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        if arg == "--check-config" {
            let value = args.next().ok_or_else(|| {
                ModuleError::Message("--check-config requires a path argument".into())
            })?;
            return Ok(Some(PathBuf::from(value)));
        }
    }
    Ok(None)
}

fn parse_subc_arg(args: impl IntoIterator<Item = OsString>) -> Result<PathBuf, ModuleError> {
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        if arg == "--subc" {
            let value = args
                .next()
                .ok_or_else(|| ModuleError::Message("--subc requires a path argument".into()))?;
            return Ok(PathBuf::from(value));
        }
    }
    Err(ModuleError::Message(
        "missing required --subc <connection-file> argument".into(),
    ))
}

#[derive(Debug)]
enum ModuleError {
    Message(String),
    Json(serde_json::Error),
}

impl fmt::Display for ModuleError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Message(message) => write!(formatter, "{message}"),
            Self::Json(error) => write!(formatter, "json: {error}"),
        }
    }
}

impl Error for ModuleError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{self as refresher, test_state};

    async fn ack_daemon(daemon: &mut tokio::io::DuplexStream) {
        let hello = read_frame(daemon).await.unwrap().unwrap();
        assert_eq!(hello.header.ty, FrameType::Hello);
        let ack = Frame::build(
            FrameType::HelloAck,
            control_flags(),
            0,
            0,
            0,
            serde_json::to_vec(&ModuleHelloAckBody {
                negotiated_ver: 1,
                subc_ops: Vec::new(),
                subc_capabilities: Vec::new(),
                storage: None,
                machine_id: None,
            })
            .unwrap(),
        )
        .unwrap();
        write_frame(daemon, &ack).await.unwrap();
        daemon.flush().await.unwrap();
    }

    fn test_module_config() -> ModuleConfig {
        ModuleConfig {
            connection_file_path: PathBuf::from("/nonexistent/subc.json"),
            module_id: DEFAULT_MODULE_ID.into(),
        }
    }

    #[tokio::test]
    async fn hydration_keeps_executor_available_before_connecting() {
        let (pulse, pulsed) = std::sync::mpsc::channel();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            let _ = pulse.send(());
        });
        let hydrated = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let finished = Arc::clone(&hydrated);
        connect_after_startup(
            move || {
                pulsed
                    .recv_timeout(std::time::Duration::from_secs(1))
                    .map_err(|_| "hydration blocked the executor".to_string())?;
                finished.store(true, std::sync::atomic::Ordering::SeqCst);
                Ok(())
            },
            || async move {
                assert!(hydrated.load(std::sync::atomic::Ordering::SeqCst));
                Ok(())
            },
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn refresher_initialization_keeps_executor_available_before_hello() {
        let (module, mut daemon) = tokio::io::duplex(64 * 1024);
        let (pulse, pulsed) = std::sync::mpsc::channel();
        let ready = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let initialized = Arc::clone(&ready);
        let peer = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            let _ = pulse.send(());
            ack_daemon(&mut daemon).await;
            assert!(
                ready.load(std::sync::atomic::Ordering::SeqCst),
                "HELLO preceded initialization"
            );
        });
        let (read, write) = tokio::io::split(module);
        let result = serve_connection(
            read,
            write,
            &test_module_config(),
            Arc::new(test_state()),
            move |_, stop| {
                pulsed
                    .recv_timeout(std::time::Duration::from_secs(1))
                    .map_err(|_| "refresher initialization blocked the executor".to_string())?;
                initialized.store(true, std::sync::atomic::Ordering::SeqCst);
                Ok(std::thread::spawn(move || {
                    while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                        std::thread::park_timeout(std::time::Duration::from_millis(10));
                    }
                    Ok(())
                }))
            },
            std::future::pending::<()>(),
        )
        .await;
        peer.abort();
        assert!(matches!(result.unwrap(), LoopEnd::Eof));
    }

    #[tokio::test]
    async fn unbracketed_ipv6_endpoint_connects_and_authenticates() {
        use subc_transport::auth::authenticate_server;
        use subc_transport::connection_file::{ConnectionInfo, Endpoint, KEY_LEN, SCHEMA_VERSION};
        let listener = tokio::net::TcpListener::bind("[::1]:0").await.unwrap();
        let connection = ConnectionInfo {
            schema: SCHEMA_VERSION,
            wire_version: None,
            endpoints: vec![Endpoint {
                host: "::1".into(),
                port: listener.local_addr().unwrap().port(),
            }],
            key: vec![7; KEY_LEN],
            daemon_id: [9; 16],
            pid: std::process::id(),
            daemon_ver: "test".into(),
        };
        let directory = tempfile::TempDir::new().unwrap();
        let path = directory.path().join("connection.json");
        connection_file::write_atomic(&path, &connection).unwrap();
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            authenticate_server(
                &mut stream,
                &connection.key,
                &connection.daemon_id,
                &connection.daemon_ver,
                std::time::Duration::from_secs(2),
            )
            .await
            .unwrap();
            stream.write_all(b"authenticated").await.unwrap();
        });
        let result = connect_to_subc(&path).await;
        if result.is_err() {
            peer.abort();
        }
        let mut stream = result.unwrap();
        let mut bytes = [0; 13];
        tokio::io::AsyncReadExt::read_exact(&mut stream, &mut bytes)
            .await
            .unwrap();
        assert_eq!(&bytes, b"authenticated");
        peer.await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn crashed_refresher_ends_serving_with_nonzero_exit() {
        for panic_thread in [true, false] {
            let (module, mut daemon) = tokio::io::duplex(64 * 1024);
            let (crash, crashed) = std::sync::mpsc::channel::<()>();
            let serving = tokio::spawn(async move {
                let (read, write) = tokio::io::split(module);
                serve_connection(
                    read,
                    write,
                    &test_module_config(),
                    Arc::new(test_state()),
                    move |_, _| {
                        Ok(std::thread::spawn(move || {
                            crashed.recv().unwrap();
                            if panic_thread {
                                panic!("injected refresher panic");
                            }
                            Ok(())
                        }))
                    },
                    std::future::pending::<()>(),
                )
                .await
            });
            ack_daemon(&mut daemon).await;
            crash.send(()).unwrap();
            let result = tokio::time::timeout(std::time::Duration::from_secs(3), serving)
                .await
                .expect("serving must end when its refresher crashes")
                .unwrap();
            assert!(result
                .as_ref()
                .unwrap_err()
                .to_string()
                .contains("refresher"));
            assert_eq!(finish_process(result), std::process::ExitCode::FAILURE);
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn panicked_leased_write_rolls_back_and_ends_serving_with_crash_cause() {
        let directory = tempfile::TempDir::new().unwrap();
        let state = Arc::new(state::leased_test_state(&directory));
        state
            .with_conn(|conn| {
                conn.execute_batch("CREATE TABLE crash_probe(id INTEGER)")
                    .map_err(|error| error.to_string())
            })
            .unwrap();
        let (module, mut daemon) = tokio::io::duplex(64 * 1024);
        let serving_state = Arc::clone(&state);
        let forced_stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let refresher_forced_stop = Arc::clone(&forced_stop);
        let serving = tokio::spawn(async move {
            let (read, write) = tokio::io::split(module);
            serve_connection(
                read,
                write,
                &test_module_config(),
                serving_state,
                |_, stop| {
                    Ok(std::thread::spawn(move || {
                        while !stop.load(std::sync::atomic::Ordering::Relaxed)
                            && !refresher_forced_stop.load(std::sync::atomic::Ordering::Relaxed)
                        {
                            std::thread::park_timeout(std::time::Duration::from_millis(10));
                        }
                        Ok(())
                    }))
                },
                std::future::pending::<()>(),
            )
            .await
        });
        ack_daemon(&mut daemon).await;
        let panicking = Arc::clone(&state);
        assert!(
            tokio::task::spawn_blocking(move || panicking.panic_write_for_test())
                .await
                .is_err()
        );
        let reader = rusqlite::Connection::open(directory.path().join("ledger.db")).unwrap();
        let rows: i64 = reader
            .query_row("SELECT COUNT(*) FROM crash_probe", [], |row| row.get(0))
            .unwrap();
        assert_eq!(rows, 0, "the panicked transaction must roll back");
        let result = tokio::time::timeout(std::time::Duration::from_secs(3), serving).await;
        forced_stop.store(true, std::sync::atomic::Ordering::Relaxed);
        let result = result
            .expect("serving must end after a write panic")
            .unwrap();
        assert!(result.as_ref().unwrap_err().to_string().contains("crash"));
        assert_eq!(finish_process(result), std::process::ExitCode::FAILURE);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn near_limit_route_failure_gets_correlated_internal_error_frame() {
        let state = Arc::new(test_state());
        let (tx, mut rx) = mpsc::channel(EGRESS_BUFFER);
        let frame = Frame::build(
            FrameType::Request,
            control_flags(),
            2,
            3,
            42,
            serde_json::to_vec(&json!({"method": "x".repeat(64 * 1024 * 1024 - 32)})).unwrap(),
        )
        .unwrap();
        handle_frame(
            frame,
            &tx,
            &state,
            &state.public_projection(),
            &FlowRoutes::default(),
        )
        .await
        .unwrap();
        // The 64 MiB method name is decoded, echoed into an error message and serialized
        // before the oversize reply falls back to `internal`, which takes several seconds in
        // a debug build on a loaded runner. This deadline only stops a hang; the assertions
        // below are about what the reply contains, not how fast it arrives.
        let response = tokio::time::timeout(std::time::Duration::from_secs(60), rx.recv())
            .await
            .expect("failed route must send a reply")
            .unwrap();
        assert_eq!(
            (
                response.header.ty,
                response.header.channel,
                response.header.epoch,
                response.header.corr
            ),
            (FrameType::Error, 2, 3, 42)
        );
        let body: ErrorBody = serde_json::from_slice(&response.body).unwrap();
        assert_eq!(body.code, "internal");
        assert!(response.body.len() < 1024);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn handler_panic_replies_before_serving_ends_with_nonzero_exit() {
        let directory = tempfile::TempDir::new().unwrap();
        let state = Arc::new(state::leased_test_state(&directory));
        state.panic_next_read_for_test();
        let (module, mut daemon) = tokio::io::duplex(64 * 1024);
        let forced_stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let refresher_forced_stop = Arc::clone(&forced_stop);
        let serving = tokio::spawn(async move {
            let (read, write) = tokio::io::split(module);
            serve_connection(
                read,
                write,
                &test_module_config(),
                state,
                |_, stop| {
                    Ok(std::thread::spawn(move || {
                        while !stop.load(std::sync::atomic::Ordering::Relaxed)
                            && !refresher_forced_stop.load(std::sync::atomic::Ordering::Relaxed)
                        {
                            std::thread::park_timeout(std::time::Duration::from_millis(10));
                        }
                        Ok(())
                    }))
                },
                std::future::pending::<()>(),
            )
            .await
        });
        ack_daemon(&mut daemon).await;
        let frame = Frame::build(
            FrameType::Request,
            control_flags(),
            2,
            3,
            43,
            serde_json::to_vec(&json!({"method": ops::OP_REPORT,
                "params": {"grouping": [], "grain": "day", "timezone": "UTC"}}))
            .unwrap(),
        )
        .unwrap();
        write_frame(&mut daemon, &frame).await.unwrap();
        daemon.flush().await.unwrap();
        let response =
            tokio::time::timeout(std::time::Duration::from_secs(3), read_frame(&mut daemon)).await;
        if response.is_err() {
            forced_stop.store(true, std::sync::atomic::Ordering::Relaxed);
        }
        let response = response
            .expect("a panicked handler must reply")
            .unwrap()
            .unwrap();
        assert_eq!(
            (response.header.ty, response.header.corr),
            (FrameType::Error, 43)
        );
        let error: ErrorBody = serde_json::from_slice(&response.body).unwrap();
        assert_eq!(error.code, "internal");
        let result = tokio::time::timeout(std::time::Duration::from_secs(3), serving).await;
        forced_stop.store(true, std::sync::atomic::Ordering::Relaxed);
        let result = result.expect("handler panic must end serving").unwrap();
        assert!(result
            .as_ref()
            .unwrap_err()
            .to_string()
            .contains("route handler crashed"));
        assert_eq!(finish_process(result), std::process::ExitCode::FAILURE);
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn reprice_management_frame_reaches_the_configured_live_catalog() {
        use crate::test_support::catalog::{event, CatalogPeer};
        use crate::test_support::{wait_until, Background, HANG_GUARD};
        use std::sync::atomic::AtomicBool;

        let event_at = 1_000;
        let resolved_at_ms = 45_678;
        let peer = CatalogPeer::start(event_at - 1, 9, resolved_at_ms);
        let state = Arc::new(test_state());
        let mut config = crate::config::intentional_empty().config;
        config.fusiform_connection_file = Some(peer.path.to_string_lossy().into_owned());
        state.publish_desired(config, 0);
        let stop = Arc::new(AtomicBool::new(false));
        // The refresher's first loop iteration prices whatever is queued. If
        // the background pass priced this event first, the frame would find it
        // already priced and the frame's own resolution, the path under test,
        // would never run. Hold the pass for the whole test so the frame is
        // the only pricer; the guard releases the hold on drop.
        let _pricing_hold = refresher::hold_pricing_pass_for_test(&state);
        // Stopped and joined when the test ends, also when an assertion
        // fails first, so a failure does not leave the refresher running.
        let refresher_stop = Arc::clone(&stop);
        let refresher = Background::new(
            "the refresher",
            refresher::start_refresher(Arc::clone(&state), Arc::clone(&stop)).unwrap(),
            move || refresher_stop.store(true, std::sync::atomic::Ordering::Relaxed),
        );
        // The frame must resolve against the catalog the refresher installed,
        // so wait until discovery has installed it. Startup already makes the
        // first attempt before start_refresher returns, so this normally
        // observes it immediately; the bound turns a discovery regression
        // into a failure rather than a hang. Without an installed catalog the
        // frame would answer unpriced rows and the assertions below would
        // fail, so this wait changes no assertion's meaning.
        wait_until("the refresher installing the configured catalog", || {
            state.catalog_client().is_some()
        });
        let event_id = crate::ops::dispatch(
            &state,
            crate::ops::OP_EVENT_INGEST,
            serde_json::to_value(event(event_at)).unwrap(),
        )
        .unwrap()["event_id"]
            .as_str()
            .unwrap()
            .to_string();
        let request = Frame::build(
            FrameType::Request,
            Flags::new(false, Priority::Interactive, false),
            7,
            1,
            42,
            serde_json::to_vec(&json!({
                "method": crate::ops::OP_EVENT_REPRICE,
                "params": {"event_id": event_id}
            }))
            .unwrap(),
        )
        .unwrap();
        let (writer, mut responses) = tokio::sync::mpsc::channel(1);

        crate::handle_route_request(request, &writer, &state, None)
            .await
            .unwrap();

        let response = tokio::time::timeout(HANG_GUARD, responses.recv())
            .await
            .expect("the reprice frame was never answered")
            .expect("the response channel closed without an answer");
        assert_eq!(
            response.header.ty,
            FrameType::Response,
            "{}",
            String::from_utf8_lossy(&response.body)
        );
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
        refresher.stop_and_join().unwrap();
        let requests = peer.finish();
        assert_eq!(requests[0]["name"], "catalog.status");
        assert_eq!(
            requests
                .get(1)
                .map(|request| &request["arguments"]["at_ms"]),
            Some(&json!(0)),
            "discovery's text probe must follow status: {requests:?}"
        );
        assert!(
            requests[2..]
                .iter()
                .any(|request| request["arguments"]["at_ms"] == event_at),
            "the frame's resolution never reached the configured catalog: {requests:?}"
        );
    }

    /// subc-protocol 0.19 made trust_tier and bindings optional, which turned
    /// both from compiler-enforced declarations into conventions: deleting the
    /// setter compiles and every other test stays green. This test is what
    /// defends the manifest's two claims -- trust_tier is declared because it
    /// is TRUE, bindings is absent because the old value was NOT -- so a tidy
    /// that removes the one or reinstates the other fails by name.
    /// The manifest is where a consumer reads which operations mutate the
    /// store, and that classification is a separate claim from the surface's
    /// size and from its name set. A test counting fifteen operations passes
    /// while a write is published as a read, and a test counting eight writes
    /// passes on the wrong eight -- so both sides are asserted BY NAME here.
    /// Moving any one name across the write/read boundary fails this test.
    #[test]
    fn manifest_publishes_the_writes_and_reads_by_name() {
        let manifest = manifest("astrocyte", &config::intentional_empty().config);
        let operations = manifest
            .provides
            .iter()
            .find_map(|role| match role {
                ProviderRole::ManagementSurface { operations, .. } => Some(operations),
                _ => None,
            })
            .expect("the manifest declares a management surface");
        let named = |kind: ManagementOperationKind| {
            operations
                .iter()
                .filter(|operation| operation.kind == kind)
                .map(|operation| operation.name.as_str())
                .collect::<std::collections::BTreeSet<_>>()
        };
        assert_eq!(
            named(ManagementOperationKind::Mutate),
            std::collections::BTreeSet::from([
                "spend.basis.correct",
                "spend.chargeability.settle",
                "spend.correction.ingest",
                "spend.event.ingest",
                "spend.event.reprice",
                "spend.multiplier.recompute",
                "spend.statement.import",
                "spend.subscription.close",
                "spend.subscription.set",
            ])
        );
        assert_eq!(
            named(ManagementOperationKind::Query),
            std::collections::BTreeSet::from([
                "budget.verdict",
                "spend.charge.explain",
                "spend.ledger.coverage",
                "spend.multiplier.pace",
                "spend.multiplier.report",
                "spend.reconcile",
                "spend.report",
                "spend.subscription.report",
                "spend.workclass.report",
            ])
        );
    }

    #[test]
    fn manifest_declares_first_party_and_no_bindings() {
        let manifest = manifest("astrocyte", &config::intentional_empty().config);
        assert_eq!(manifest.trust_tier, Some(TrustTier::FirstParty));
        assert!(
            manifest.bindings.is_none(),
            "bindings must stay absent: the only value the enum can express is false here"
        );
        // And on the wire, where a serialized default would be a fabrication.
        let wire = serde_json::to_value(&manifest).unwrap();
        assert_eq!(wire["module_id"], DEFAULT_MODULE_ID);
        assert_eq!(wire["trust_tier"], serde_json::json!("first_party"));
        assert!(
            wire.get("bindings").is_none(),
            "bindings serialized: {wire}"
        );
    }

    #[test]
    fn hello_states_where_the_nonce_was_read_only_when_it_sends_one() {
        let desired = config::intentional_empty().config;
        for (source, wire) in [
            (LaunchNonceSource::Fd, "fd"),
            (LaunchNonceSource::Env, "env"),
        ] {
            let sent = hello_body("astrocyte", &desired, Some(("nonce-1".to_string(), source)));
            assert_eq!(sent.launch_nonce.as_deref(), Some("nonce-1"));
            let body = serde_json::to_value(&sent).unwrap();
            assert_eq!(
                body["manifest"]["provenance"]["launch_nonce_source"],
                serde_json::json!(wire)
            );
        }

        let unsent = hello_body("astrocyte", &desired, None);
        assert_eq!(unsent.launch_nonce, None);
        let wire = serde_json::to_value(&unsent).unwrap();
        assert!(
            wire["manifest"]["provenance"]
                .get("launch_nonce_source")
                .is_none(),
            "a source was named for a nonce that was never read: {wire}"
        );
    }

    #[test]
    fn nonce_sources_keep_their_own_names() {
        assert_eq!(
            nonce_source(subc_os::LaunchNonceSource::Fd),
            LaunchNonceSource::Fd
        );
        assert_eq!(
            nonce_source(subc_os::LaunchNonceSource::Env),
            LaunchNonceSource::Env
        );
    }

    fn desired(qta: bool, fusiform: bool) -> config::AstrocyteConfig {
        let file = |set: bool| set.then(|| "/tmp/peer.json".to_string());
        config::AstrocyteConfig {
            poll_interval_secs: 45,
            qta_connection_file: file(qta),
            fusiform_connection_file: file(fusiform),
            reasoning_billed_as_output: Vec::new(),
            subscription_multiplier: config::intentional_empty().config.subscription_multiplier,
            harness_ingest: config::HarnessIngest::all_on(),
        }
    }

    fn self_signal_names(manifest: &ModuleManifest) -> Vec<String> {
        manifest
            .self_signals
            .as_ref()
            .expect("self_signals is always declared")
            .iter()
            .map(|signal| signal.name.clone())
            .collect()
    }

    fn consumed_modules(manifest: &ModuleManifest) -> Vec<String> {
        manifest
            .consumes
            .iter()
            .flat_map(|role| match role {
                ConsumerRole::ServiceClient { of } => of.clone(),
                other => panic!("unexpected consumer role {other:?}"),
            })
            .collect()
    }

    /// The self-signal list is a completeness claim, so every call astrocyte
    /// makes against another module under this configuration must be in it
    /// by name, and the consumers must be exactly the modules it calls.
    #[test]
    fn manifest_declares_every_peer_call_when_all_peers_are_configured() {
        let manifest = manifest("astrocyte", &desired(true, true));
        assert_eq!(
            self_signal_names(&manifest),
            [
                "astrocyte-insula-usage-poll",
                "astrocyte-insula-quota-body",
                "astrocyte-ckbus-credentials",
                "astrocyte-fusiform-catalog-lookup",
                "astrocyte-fusiform-history-status",
                "astrocyte-fusiform-history-probe",
            ]
        );
        assert_eq!(consumed_modules(&manifest), ["fusiform", "insula", "ckbus"]);
        let wire = serde_json::to_value(&manifest).unwrap();
        let signals = wire["self_signals"].as_array().unwrap();
        let lookup = signals
            .iter()
            .find(|signal| signal["name"] == "astrocyte-fusiform-catalog-lookup")
            .unwrap();
        assert_eq!(lookup["kind"], "poller");
        assert_eq!(lookup["effect"], "observe");
        assert_eq!(
            lookup["anchored_to"],
            json!({"event": {"event": "usage_event_needs_rate"}})
        );
        assert!(lookup.get("cadence").is_none(), "{lookup}");
        assert_eq!(lookup["domain"], "fusiform");
        let status = signals
            .iter()
            .find(|signal| signal["name"] == "astrocyte-fusiform-history-status")
            .unwrap();
        assert_eq!(status["kind"], "poller");
        assert_eq!(status["effect"], "observe");
        assert_eq!(status["anchored_to"], "fixed_interval");
        assert_eq!(status["domain"], "fusiform");
        assert!(status["note"].as_str().unwrap().contains("catalog.status"));
    }

    /// A declared quota provider adds the daily multiplier's two fusiform
    /// calls, plan.prices and its rate lookups, on a literal daily cadence.
    #[test]
    fn manifest_declares_the_daily_multiplier_calls_when_a_quota_provider_is_declared() {
        let mut desired = desired(false, true);
        let decoded =
            config::decode(include_str!("../../../docs/prod-config/astrocyte-v2.jsonc")).unwrap();
        desired.subscription_multiplier = decoded.config.subscription_multiplier;
        let manifest = manifest("astrocyte", &desired);
        assert_eq!(
            self_signal_names(&manifest),
            [
                "astrocyte-fusiform-catalog-lookup",
                "astrocyte-fusiform-history-status",
                "astrocyte-fusiform-history-probe",
                "astrocyte-fusiform-plan-prices",
                "astrocyte-fusiform-multiplier-rates",
            ]
        );
        let wire = serde_json::to_value(&manifest).unwrap();
        let plan_prices = wire["self_signals"]
            .as_array()
            .unwrap()
            .iter()
            .find(|signal| signal["name"] == "astrocyte-fusiform-plan-prices")
            .unwrap();
        assert_eq!(plan_prices["kind"], "poller");
        assert_eq!(plan_prices["effect"], "observe");
        assert_eq!(plan_prices["anchored_to"], "fixed_interval");
        assert_eq!(
            plan_prices["cadence"],
            json!({"literal": {"interval_ms": 86_400_000}})
        );
        assert_eq!(plan_prices["domain"], "fusiform");
    }

    /// With no peer configured astrocyte calls nobody: the list is present and
    /// empty (an affirmative "none", not an un-adopted block), and no consumer
    /// role is declared.
    #[test]
    fn manifest_declares_no_peer_calls_when_no_peer_is_configured() {
        let manifest = manifest("astrocyte", &desired(false, false));
        assert!(self_signal_names(&manifest).is_empty());
        assert!(consumed_modules(&manifest).is_empty());
        let wire = serde_json::to_value(&manifest).unwrap();
        assert_eq!(wire["self_signals"], json!([]));
        assert!(wire.get("consumes").is_none(), "{wire}");
    }

    /// BROCA is read from its store, never called over subc, so no ingest
    /// setting makes it a consumer or gives it a self-signal: with it
    /// ingested, excluded, or all ingestion off, the peer calls are the same.
    #[test]
    fn manifest_never_declares_a_broca_call() {
        for ingest in [
            config::HarnessIngest::all_on(),
            config::HarnessIngest {
                enabled: true,
                exclude: vec!["broca".into()],
            },
            config::HarnessIngest {
                enabled: false,
                exclude: Vec::new(),
            },
        ] {
            let mut desired = desired(true, true);
            desired.harness_ingest = ingest.clone();
            let manifest = manifest("astrocyte", &desired);
            assert_eq!(
                self_signal_names(&manifest),
                [
                    "astrocyte-insula-usage-poll",
                    "astrocyte-insula-quota-body",
                    "astrocyte-ckbus-credentials",
                    "astrocyte-fusiform-catalog-lookup",
                    "astrocyte-fusiform-history-status",
                    "astrocyte-fusiform-history-probe",
                ],
                "{ingest:?}"
            );
            assert_eq!(
                consumed_modules(&manifest),
                ["fusiform", "insula", "ckbus"],
                "{ingest:?}"
            );
        }
    }

    /// Quota ingestion declares its fallback poll, event-body fetch and attested
    /// credential lifecycle; no catalog calls are introduced.
    #[test]
    fn manifest_declares_quota_poll_events_and_credentials_when_only_qta_is_configured() {
        let manifest = manifest("astrocyte", &desired(true, false));
        assert_eq!(
            self_signal_names(&manifest),
            [
                "astrocyte-insula-usage-poll",
                "astrocyte-insula-quota-body",
                "astrocyte-ckbus-credentials"
            ]
        );
        assert_eq!(consumed_modules(&manifest), ["insula", "ckbus"]);
        let wire = serde_json::to_value(&manifest).unwrap();
        assert_eq!(
            wire["consumes"],
            json!([{"role": "service_client", "of": ["insula", "ckbus"]}])
        );
        let poll = &wire["self_signals"][0];
        assert_eq!(poll["name"], "astrocyte-insula-usage-poll");
        assert_eq!(poll["kind"], "poller");
        assert_eq!(poll["effect"], "observe");
        assert_eq!(poll["anchored_to"], "fixed_interval");
        assert_eq!(
            poll["cadence"],
            json!({"derived": {"source": "desired_config.poll_interval_secs"}})
        );
        assert_eq!(poll["domain"], "insula");
    }

    #[test]
    fn check_config_accepts_the_closed_live_document() {
        let path =
            std::env::temp_dir().join(format!("astrocyte-check-{}.jsonc", std::process::id()));
        std::fs::write(
            &path,
            br#"{"poll_interval_secs":60,"qta_connection_file":null,"fusiform_connection_file":null,"reasoning_billed_as_output":[],"subscription_multiplier":{"api_twins":{},"plan_price_overrides":{},"quota_providers":{}},"harness_ingest":{"enabled":true,"exclude":[]}}"#,
        )
        .unwrap();
        let mut output = Vec::new();
        check_config(&path, &mut output).unwrap();
        assert!(output.is_empty());
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn check_config_preflight_rejects_retired_catalog_file() {
        let path = std::env::temp_dir().join(format!(
            "astrocyte-check-unknown-{}.jsonc",
            std::process::id()
        ));
        std::fs::write(
            &path,
            br#"{"poll_interval_secs":60,"catalog_file":null,"qta_connection_file":null,"fusiform_connection_file":null,"reasoning_billed_as_output":[],"subscription_multiplier":{"api_twins":{},"plan_price_overrides":{},"quota_providers":{}},"harness_ingest":{"enabled":true,"exclude":[]}}"#,
        )
        .unwrap();
        assert!(check_config(&path, &mut Vec::new()).is_err());
        std::fs::remove_file(path).unwrap();
    }

    /// The deploy preflight names each `harness_ingest` refusal, so an
    /// operator reads which rule the file broke.
    #[test]
    fn check_config_names_each_harness_ingest_refusal() {
        let path = std::env::temp_dir().join(format!(
            "astrocyte-check-ingest-{}.jsonc",
            std::process::id()
        ));
        let document = |ingest: &str| {
            format!(
                r#"{{"poll_interval_secs":60,"qta_connection_file":null,"fusiform_connection_file":null,"reasoning_billed_as_output":[],"subscription_multiplier":{{"api_twins":{{}},"plan_price_overrides":{{}},"quota_providers":{{}}}}{ingest}}}"#
            )
        };
        for (ingest, needle) in [
            (
                r#","harness_ingest":{"enabled":true,"exclude":["cluade-code"]}"#,
                "harness_ingest.exclude names unknown producer 'cluade-code'",
            ),
            (
                r#","harness_ingest":{"enabled":true,"exclude":["omp","omp"]}"#,
                "harness_ingest.exclude names producer 'omp' more than once",
            ),
            (
                r#","harness_ingest":{"enabled":false,"exclude":["omp"]}"#,
                "harness_ingest.enabled is false but harness_ingest.exclude is not empty",
            ),
            ("", "desired root is missing member 'harness_ingest'"),
            (
                r#","harness_ingest":{"enabled":true,"exclude":[],"only":[]}"#,
                "unknown field `only`",
            ),
        ] {
            std::fs::write(&path, document(ingest)).unwrap();
            let error = check_config(&path, &mut Vec::new())
                .unwrap_err()
                .to_string();
            assert!(
                error.contains("could not be decoded") && error.contains(needle),
                "{needle}: {error}"
            );
        }
        std::fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn startup_failed_hydration_refuses_before_transport_attempt() {
        let state = Arc::new(test_state());
        let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let transport_attempts = Arc::clone(&attempts);

        let result = connect_after_startup(
            move || {
                refresher::reject_startup_bytes_for_test(&state, br#"{"poll_interval_secs":60}"#)
            },
            || async move {
                transport_attempts.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                Ok::<(), ModuleError>(())
            },
        )
        .await;

        assert!(result.is_err());
        assert_eq!(attempts.load(std::sync::atomic::Ordering::Relaxed), 0);
    }

    #[test]
    fn dispatch_wires_a_public_operation_to_the_writer_fence() {
        let state = test_state();
        let result = ops::dispatch(&state, ops::OP_EVENT_INGEST, json!({"bad":true}));
        assert!(result.is_err());
        let count: i64 = state
            .with_conn(|conn| {
                conn.query_row("SELECT COUNT(*) FROM usage_event", [], |row| row.get(0))
                    .map_err(|error| error.to_string())
            })
            .unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn event_reprice_is_callable_from_a_management_surface_frame() {
        let state = Arc::new(test_state());
        let event = json!({
            "producer":"producer",
            "producer_lineage":"lineage",
            "producer_ref":"wire-reprice",
            "provider":"provider",
            "model":"model",
            "occurred_at_ms":10,
            "charge_basis":"metered_api",
            "measurements":[{
                "role":"input","unit":"token","kind":"raw","source":"usage",
                "quantity_units":1,"quantity_exponent":0
            }]
        });
        let event_id = ops::dispatch(&state, ops::OP_EVENT_INGEST, event).unwrap()["event_id"]
            .as_str()
            .unwrap()
            .to_string();
        let request = Frame::build(
            FrameType::Request,
            Flags::new(false, Priority::Interactive, false),
            7,
            1,
            42,
            serde_json::to_vec(&json!({
                "method": ops::OP_EVENT_REPRICE,
                "params": {"event_id": event_id}
            }))
            .unwrap(),
        )
        .unwrap();
        let (writer, mut responses) = mpsc::channel(1);

        handle_route_request(request, &writer, &state, None)
            .await
            .unwrap();

        let response = tokio::time::timeout(crate::test_support::HANG_GUARD, responses.recv())
            .await
            .expect("the reprice frame was never answered")
            .expect("the response channel closed without an answer");
        assert_eq!(response.header.ty, FrameType::Response);
        let body: Value = serde_json::from_slice(&response.body).unwrap();
        assert_eq!(body["result"]["measurements_repriced"], 1);
        let reason: String = state
            .with_conn(|conn| {
                conn.query_row(
                    "SELECT unpriced_reason FROM priced_charge WHERE event_key=(SELECT event_key FROM usage_event WHERE event_id=?1)",
                    [&event_id],
                    |row| row.get(0),
                )
                .map_err(|error| error.to_string())
            })
            .unwrap();
        // The test state has no catalog, so the stored reason must say nobody
        // looked — never fusiform's own "looked and found nothing" reason.
        assert_eq!(reason, "catalog_unconfigured");
    }

    /// Every graceful stop in this module hangs off one line: `module_loop`
    /// RETURNING when the daemon's connection closes (EOF). That return is
    /// what flips the refresher's stop flag and joins it. A loop that
    /// reconnected, or waited, instead of returning would leave the refresher
    /// running against a daemon that is gone and every teardown hook unrun --
    /// silently, because nothing else observes the difference. Four modules
    /// implement this shape independently and subc's own fence covers only the
    /// SDK's serve(); astrocyte hand-rolls its loop, so this is the fence for
    /// this seat. Bounded so the mutation (make the EOF arm wait instead of
    /// return) fails by name in seconds rather than hanging the suite.
    #[tokio::test(flavor = "multi_thread")]
    async fn module_loop_returns_on_daemon_eof_rather_than_reconnecting_or_waiting() {
        let state = Arc::new(test_state());
        let config = ModuleConfig {
            connection_file_path: PathBuf::from("/nonexistent/subc-connection.json"),
            module_id: DEFAULT_MODULE_ID.to_string(),
        };
        // The module's read half and a handle we control as "the daemon".
        let (mut module_read, mut daemon_write) = tokio::io::duplex(64 * 1024);
        let (tx, mut rx) = mpsc::channel::<Frame>(EGRESS_BUFFER);
        // Drain the module's egress so send_hello never blocks on a full channel.
        let egress = tokio::spawn(async move { while rx.recv().await.is_some() {} });

        let ack = Frame::build(
            FrameType::HelloAck,
            Flags::new(false, Priority::Interactive, false),
            0,
            0,
            0,
            serde_json::to_vec(&ModuleHelloAckBody {
                negotiated_ver: 1,
                subc_ops: Vec::new(),
                subc_capabilities: Vec::new(),
                storage: None,
                machine_id: None,
            })
            .unwrap(),
        )
        .unwrap();
        write_frame(&mut daemon_write, &ack).await.unwrap();
        daemon_write.flush().await.unwrap();

        let looped = tokio::spawn(async move {
            module_loop(
                &mut module_read,
                tx,
                &config,
                state,
                std::future::pending::<()>(),
            )
            .await
        });
        // The daemon goes away: drop our end. No GOODBYE, no route.closing --
        // the same thing a daemon cut looks like from the module's side.
        drop(daemon_write);

        let result = tokio::time::timeout(std::time::Duration::from_secs(5), looped)
            .await
            .expect(
                "module_loop did not return within 5s of the daemon's connection closing. \
                 The refresher's stop flag and join hang off this return; if the loop now \
                 reconnects or waits, teardown never runs on a daemon cut, silently.",
            )
            .unwrap();
        assert!(
            matches!(result, Ok(LoopEnd::Eof)),
            "EOF is a clean return, not an error: {result:?}"
        );
        egress.abort();
    }

    /// A control request this build can't decode must be refused on its own
    /// correlation id while the module keeps serving. The real case is a
    /// newer daemon's `route.bind` whose scope carries an attribute this
    /// build's subc-protocol doesn't know (`ScopeAttributes` refuses unknown
    /// fields); an unknown control op takes the same decode path. Before the
    /// fix the decode error ended `module_loop`, which stops the module.
    #[tokio::test(flavor = "multi_thread")]
    async fn undecodable_control_request_is_refused_and_the_module_keeps_serving() {
        let state = Arc::new(test_state());
        let config = ModuleConfig {
            connection_file_path: PathBuf::from("/nonexistent/subc-connection.json"),
            module_id: DEFAULT_MODULE_ID.to_string(),
        };
        let (mut module_read, mut daemon_write) = tokio::io::duplex(64 * 1024);
        let (tx, mut rx) = mpsc::channel::<Frame>(EGRESS_BUFFER);
        let control = |corr: u64, body: &str| {
            Frame::build(
                FrameType::Request,
                Flags::new(false, Priority::Passive, false),
                0,
                0,
                corr,
                body.as_bytes().to_vec(),
            )
            .unwrap()
        };
        let ack = Frame::build(
            FrameType::HelloAck,
            Flags::new(false, Priority::Interactive, false),
            0,
            0,
            0,
            serde_json::to_vec(&ModuleHelloAckBody {
                negotiated_ver: 1,
                subc_ops: Vec::new(),
                subc_capabilities: Vec::new(),
                storage: None,
                machine_id: None,
            })
            .unwrap(),
        )
        .unwrap();
        write_frame(&mut daemon_write, &ack).await.unwrap();
        write_frame(
            &mut daemon_write,
            &control(7, r#"{"op":"route.future_op","flow_id":"x"}"#),
        )
        .await
        .unwrap();
        write_frame(&mut daemon_write, &control(8, r#"{"op":"health.check"}"#))
            .await
            .unwrap();
        daemon_write.flush().await.unwrap();

        let looped = tokio::spawn(async move {
            module_loop(
                &mut module_read,
                tx,
                &config,
                state,
                std::future::pending::<()>(),
            )
            .await
        });

        // Skip the module's HELLO; then the refusal, then the health answer.
        let mut replies = Vec::new();
        while replies.len() < 2 {
            let frame = tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
                .await
                .expect("no reply within 5 s: the module stopped serving after the bad request")
                .expect("egress closed: the module stopped serving after the bad request");
            if frame.header.ty != FrameType::Hello {
                replies.push(frame);
            }
        }
        assert_eq!(replies[0].header.ty, FrameType::Error);
        assert_eq!(replies[0].header.corr, 7);
        let refusal: ErrorBody = serde_json::from_slice(&replies[0].body).unwrap();
        assert_eq!(refusal.code, "invalid_request");
        assert_eq!(replies[1].header.ty, FrameType::Response);
        assert_eq!(replies[1].header.corr, 8);
        assert!(
            matches!(
                serde_json::from_slice::<ModuleControlResponse>(&replies[1].body).unwrap(),
                ModuleControlResponse::HealthCheck { .. }
            ),
            "the request after the refusal must still be answered"
        );
        assert!(
            !looped.is_finished(),
            "module_loop ended after a bad control request"
        );

        drop(daemon_write);
        let result = tokio::time::timeout(std::time::Duration::from_secs(5), looped)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(result, Ok(LoopEnd::Eof)), "{result:?}");
    }

    /// A route a flow opened may read but never write: every mutating
    /// operation on it is refused `flow_scopes_unsupported` before dispatch,
    /// so nothing reaches the ledger, while the same write on a route opened
    /// without a flow goes through. Asserted by the rows the writes leave.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_flow_stamped_route_may_read_but_every_write_is_refused() {
        use subc_protocol::scope::{ScopeAttributes, ScopeKind, ScopeStamp};
        use subc_protocol::{BindIdentity, Principal, RouteTarget};

        let state = Arc::new(test_state());
        let config = ModuleConfig {
            connection_file_path: PathBuf::from("/nonexistent/subc-connection.json"),
            module_id: DEFAULT_MODULE_ID.to_string(),
        };
        let (mut module_read, mut daemon_write) = tokio::io::duplex(256 * 1024);
        let (tx, mut rx) = mpsc::channel::<Frame>(EGRESS_BUFFER);
        let frame = |ty: FrameType, channel: u16, corr: u64, body: Vec<u8>| {
            // Control channel 0 carries epoch 0; routes carry the bind's epoch.
            let epoch = if channel == 0 { 0 } else { 1 };
            Frame::build(
                ty,
                Flags::new(false, Priority::Interactive, false),
                channel,
                epoch,
                corr,
                body,
            )
            .unwrap()
        };
        let bind = |channel: u16, flow_id: Option<&str>| {
            let scope = flow_id.map(|flow_id| {
                ScopeStamp::new(
                    Principal::Reserved {
                        module_id: "prefrontal-core".to_string(),
                    },
                    "scope-1",
                    1,
                    ScopeKind::Worker,
                )
                .with_attributes(ScopeAttributes::new().with_flow_id(Some(flow_id.to_string())))
                .with_owner_authorized(true)
            });
            serde_json::to_vec(&ModuleControlRequest::RouteBind {
                route_channel: channel,
                epoch: 1,
                target: RouteTarget::ManagementSurface {
                    module_id: DEFAULT_MODULE_ID.to_string(),
                },
                identity: BindIdentity::new("/tmp/p", "h", "s"),
                principal: None,
                consumer_capabilities: None,
                role_versions: None,
                admission_facts: None,
                scope,
                call_mode: None,
            })
            .unwrap()
        };
        let set = |account: &str| {
            serde_json::to_vec(&json!({"method": ops::OP_SUBSCRIPTION_SET, "params": {
                "account_ref": account, "provider": "provider", "plan_name": "plan",
                "recurring_cost_minor": 100, "currency": "USD", "amount_exponent": 2,
                "period": "month", "effective_from": 1}}))
            .unwrap()
        };
        let read = serde_json::to_vec(&json!({"method": "budget.verdict", "params": {}})).unwrap();

        let ack = Frame::build(
            FrameType::HelloAck,
            Flags::new(false, Priority::Interactive, false),
            0,
            0,
            0,
            serde_json::to_vec(&ModuleHelloAckBody {
                negotiated_ver: 1,
                subc_ops: Vec::new(),
                subc_capabilities: Vec::new(),
                storage: None,
                machine_id: None,
            })
            .unwrap(),
        )
        .unwrap();
        write_frame(&mut daemon_write, &ack).await.unwrap();
        // Channel 5 is a flow's route; channel 6 was opened without one.
        // Binds go on control channel 0. `module_loop` handles control frames
        // in arrival order, so both binds are recorded before either route's
        // requests are dispatched.
        write_frame(
            &mut daemon_write,
            &frame(FrameType::Request, 0, 1, bind(5, Some("flow-a"))),
        )
        .await
        .unwrap();
        write_frame(
            &mut daemon_write,
            &frame(FrameType::Request, 0, 2, bind(6, None)),
        )
        .await
        .unwrap();
        write_frame(
            &mut daemon_write,
            &frame(FrameType::Request, 5, 10, set("flow-acct")),
        )
        .await
        .unwrap();
        write_frame(
            &mut daemon_write,
            &frame(FrameType::Request, 5, 11, read.clone()),
        )
        .await
        .unwrap();
        write_frame(
            &mut daemon_write,
            &frame(FrameType::Request, 6, 12, set("owner-acct")),
        )
        .await
        .unwrap();
        write_frame(
            &mut daemon_write,
            &frame(
                FrameType::Request,
                5,
                13,
                serde_json::to_vec(&json!({
                    "method": ops::OP_MULTIPLIER_PACE, "params": {"astrocyte_probe": true}
                }))
                .unwrap(),
            ),
        )
        .await
        .unwrap();
        daemon_write.flush().await.unwrap();

        let loop_state = Arc::clone(&state);
        let looped = tokio::spawn(async move {
            module_loop(
                &mut module_read,
                tx,
                &config,
                loop_state,
                std::future::pending::<()>(),
            )
            .await
        });

        // Route replies are answered on spawned tasks, so collect by corr.
        let mut replies = HashMap::new();
        while replies.len() < 4 {
            let reply = tokio::time::timeout(std::time::Duration::from_secs(10), rx.recv())
                .await
                .expect("no reply within 10 s")
                .expect("egress closed");
            if reply.header.channel != 0 {
                replies.insert(reply.header.corr, reply);
            }
        }
        let refused = &replies[&10];
        assert_eq!(
            refused.header.ty,
            FrameType::Error,
            "a flow's write was served"
        );
        let refusal: ErrorBody = serde_json::from_slice(&refused.body).unwrap();
        assert_eq!(refusal.code, "flow_scopes_unsupported");
        let pace_refusal: ErrorBody = serde_json::from_slice(&replies[&13].body).unwrap();
        assert_eq!(
            pace_refusal.code, "malformed",
            "a flow's pace request must reach dispatch"
        );
        assert!(pace_refusal.message.starts_with("multiplier pace params: "));
        assert_eq!(
            replies[&11].header.ty,
            FrameType::Response,
            "a flow's read was refused"
        );
        assert_eq!(
            replies[&12].header.ty,
            FrameType::Response,
            "a write on a route opened without a flow was refused: {}",
            String::from_utf8_lossy(&replies[&12].body)
        );
        let accounts: Vec<String> = state
            .with_conn(|conn| {
                let mut statement = conn
                    .prepare("SELECT account_ref FROM account_subscription ORDER BY account_ref")
                    .map_err(|error| error.to_string())?;
                let rows = statement
                    .query_map([], |row| row.get(0))
                    .map_err(|error| error.to_string())?;
                rows.collect::<Result<_, _>>()
                    .map_err(|error| error.to_string())
            })
            .unwrap();
        assert_eq!(accounts, vec!["owner-acct".to_string()]);

        drop(daemon_write);
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), looped).await;
    }

    /// The whole shutdown path under the condition that used to kill the
    /// module: the daemon closes the connection while the refresher is inside
    /// an OpenCode page statement that would run for hours. `serve_connection`
    /// must see EOF, stop and join the refresher, and return well inside the
    /// supervisor's 30 s drain window. The test above proves the loop returns
    /// on EOF with no refresher attached; it cannot show that the join after
    /// it is bounded. The drain-level tests prove the drain returns and
    /// commits nothing; they cannot show that the stop flag reaches the drain
    /// through the module's own shutdown.
    #[test]
    fn daemon_eof_during_a_slow_opencode_read_joins_the_refresher_promptly() {
        use crate::test_support::opencode as opencode_fixture;

        let temp = tempfile::TempDir::new().unwrap();
        let opencode_database = opencode_fixture::slow_page_database(temp.path());
        let probed_database = opencode_database.clone();
        let state = Arc::new(test_state());
        let (module_side, mut daemon_side) = tokio::io::duplex(64 * 1024);
        // Dropping the sender is the daemon's close signal.
        let (close_daemon, daemon_closes) = std::sync::mpsc::channel::<()>();
        let (served, returned) = std::sync::mpsc::channel();
        let serve_state = Arc::clone(&state);
        // The module runs on its own thread and runtime so that a join that
        // never finishes fails the bound below instead of hanging the suite.
        let module = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap();
            let result = runtime.block_on(async move {
                // Sent by the daemon once it has decided to close; the
                // refresher starter below waits for it.
                let (daemon_closing, closing_seen) = std::sync::mpsc::channel::<()>();
                let (read_half, write_half) = tokio::io::split(module_side);
                let serving = tokio::spawn(async move {
                    let config = ModuleConfig {
                        connection_file_path: PathBuf::from("/nonexistent/subc-connection.json"),
                        module_id: DEFAULT_MODULE_ID.to_string(),
                    };
                    serve_connection(
                        read_half,
                        write_half,
                        &config,
                        serve_state,
                        move |state, stop| {
                            let refresher = refresher::start_refresher_with_opencode(
                                state,
                                stop,
                                opencode_database,
                            );
                            // `serve_connection` sends HELLO only after this
                            // starter returns. Holding it until the daemon has
                            // decided to close makes the daemon's close race
                            // the module's HELLO every run, not only on a
                            // loaded runner. A daemon that closed before
                            // reading HELLO would make that write fail with a
                            // broken pipe, which `serve_connection` reports as
                            // an error rather than a clean EOF.
                            let _ = closing_seen.recv();
                            refresher
                        },
                        std::future::pending::<()>(),
                    )
                    .await
                });
                // Closed on the test's signal, or when the test drops the
                // sender by failing first: either way the daemon goes away, so
                // the module stops its refresher rather than outliving the test.
                let _ = tokio::task::spawn_blocking(move || daemon_closes.recv()).await;
                drop(daemon_closing);
                // A real daemon sends HELLO_ACK only in answer to the module's
                // HELLO, so by the time it can close, HELLO has been written.
                // Then it goes away with no GOODBYE, as in a daemon stop.
                ack_daemon(&mut daemon_side).await;
                drop(daemon_side);
                serving.await.unwrap()
            });
            let _ = served.send(result);
        });
        let mut module =
            crate::test_support::Background::new("the module thread", module, move || {
                drop(close_daemon)
            });

        opencode_fixture::wait_until_drain_reaches_the_page_statement(&probed_database);
        assert!(
            returned.try_recv().is_err(),
            "the module returned before the daemon closed, so no slow stage was interrupted"
        );
        module.signal_stop();
        let result = returned
            .recv_timeout(opencode_fixture::STOP_BOUND)
            .unwrap_or_else(|_| {
                panic!(
                    "the module did not stop and join its refresher within {:?} of the daemon closing",
                    opencode_fixture::STOP_BOUND
                )
            });
        module.stop_and_join();

        assert!(result.is_ok(), "EOF is a clean return: {result:?}");
        let (lineage, consumed, _, last_success, _, _) =
            opencode_fixture::opencode_runtime_row(&state);
        assert_eq!(
            (lineage, consumed, last_success),
            (None, None, None),
            "the interrupted page must not advance the cursor"
        );
        assert_eq!(state.producer_degraded_reason("opencode"), None);
    }

    /// How the parent ends a child running production `run`.
    #[cfg(unix)]
    #[derive(Clone, Copy, Debug)]
    enum ChildStop {
        /// SIGTERM the child while the fake daemon holds the connection open.
        Sigterm,
        /// The fake daemon closes the connection with no GOODBYE.
        DaemonEof,
    }

    /// What the parent observed of a child that has exited.
    #[cfg(unix)]
    struct ChildEnd {
        status: std::process::ExitStatus,
        /// The child's stdout and stderr, for failure messages.
        output: String,
        /// The child's whole log file.
        log: String,
        /// The message body of each log line, in order.
        bodies: Vec<String>,
    }

    #[cfg(unix)]
    impl ChildEnd {
        /// Both shutdown lines are present, the first naming `cause` and the
        /// second coming after it.
        fn assert_shutdown_lines(&self, cause: &str) {
            let stopping = self
                .bodies
                .iter()
                .position(|body| body.starts_with("stopping refresher"))
                .unwrap_or_else(|| panic!("no stopping line in the child's log:\n{}", self.log));
            assert!(
                self.bodies[stopping].contains(&format!("cause={cause}")),
                "the stopping line must name {cause} as the cause: {}",
                self.bodies[stopping]
            );
            assert!(
                self.bodies[stopping + 1..]
                    .iter()
                    .any(|body| body
                        .starts_with("refresher stopped, nothing further will be written")),
                "the refresher was not joined after the stopping line:\n{}",
                self.log
            );
        }
    }

    /// Runs production `run` (store, hydration, connect, authenticate, HELLO,
    /// the real refresher) and then `finish_process`, the same ending `main`
    /// takes, in a child process against a fake daemon, and ends the child as
    /// `stop` says. The child is this test binary re-executed as the test
    /// named `name`, because a unit test cannot name the `ck-astrocyte`
    /// executable; it calls `run` directly with the `--subc` path already
    /// parsed.
    ///
    /// Returns `None` inside the child, where the calling test must simply
    /// return, and what the parent observed otherwise.
    #[cfg(unix)]
    fn serve_production_run_in_a_child(name: &str, stop: ChildStop) -> Option<ChildEnd> {
        use subc_transport::auth::authenticate_server;
        use subc_transport::connection_file::{ConnectionInfo, Endpoint, KEY_LEN, SCHEMA_VERSION};

        const CHILD_ENV: &str = "ASTROCYTE_SERVE_CHILD_CONNECTION_FILE";
        const LOG_PATH_PREFIX: &str = "ASTROCYTE_LOG_PATH=";
        // Well under the supervisor's 30 s drain window.
        const EXIT_BOUND: std::time::Duration = std::time::Duration::from_secs(10);

        if let Some(connection_file_path) = std::env::var_os(CHILD_ENV) {
            let handle = super::init_logging().unwrap();
            println!("{LOG_PATH_PREFIX}{}", handle.path().display());
            let runtime = tokio::runtime::Runtime::new().unwrap();
            let result = runtime.block_on(async {
                let state = open_state().unwrap();
                let config = ModuleConfig {
                    connection_file_path: PathBuf::from(connection_file_path),
                    module_id: DEFAULT_MODULE_ID.to_string(),
                };
                run(config, state).await
            });
            // As in `main`: the runtime goes before `finish_process`.
            drop(runtime);
            let ended = format!("{result:?}");
            // A SIGTERM ending does not return from here.
            let code = finish_process(result);
            assert!(
                code == std::process::ExitCode::SUCCESS,
                "serving ended with {ended} and main would exit with {code:?}"
            );
            return None;
        }

        let temp = tempfile::TempDir::new().unwrap();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
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
            daemon_ver: "test".into(),
        };
        let connection_file_path = temp.path().join("subc-connection.json");
        connection_file::write_atomic(&connection_file_path, &connection).unwrap();

        // The fake daemon: authenticate, take HELLO, answer HELLO_ACK, then
        // hold the connection open until released, and close it then.
        let (handshaken, handshake_done) = std::sync::mpsc::channel::<()>();
        let (release_daemon, daemon_released) = std::sync::mpsc::channel::<()>();
        // The accept deadline below must start when the child exists, not
        // when this thread does: on a loaded runner building and spawning the
        // child can take a long while, and that is not the child failing to
        // connect.
        let (child_spawned, child_exists) = std::sync::mpsc::channel::<()>();
        let daemon = std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async move {
                    let listener = tokio::net::TcpListener::from_std(listener).unwrap();
                    // A closed channel means the parent failed before
                    // spawning; there is no child to serve.
                    if tokio::task::spawn_blocking(move || child_exists.recv())
                        .await
                        .unwrap()
                        .is_err()
                    {
                        return;
                    }
                    let (mut stream, _) =
                        tokio::time::timeout(std::time::Duration::from_secs(30), listener.accept())
                            .await
                            .expect("the child never connected")
                            .unwrap();
                    authenticate_server(
                        &mut stream,
                        &connection.key,
                        &connection.daemon_id,
                        &connection.daemon_ver,
                        std::time::Duration::from_secs(5),
                    )
                    .await
                    .unwrap();
                    let hello = read_frame(&mut stream).await.unwrap().unwrap();
                    assert_eq!(hello.header.ty, FrameType::Hello);
                    let ack = Frame::build(
                        FrameType::HelloAck,
                        Flags::new(false, Priority::Interactive, false),
                        0,
                        0,
                        HELLO_CORR,
                        serde_json::to_vec(&ModuleHelloAckBody {
                            negotiated_ver: 1,
                            subc_ops: Vec::new(),
                            subc_capabilities: Vec::new(),
                            storage: None,
                            machine_id: None,
                        })
                        .unwrap(),
                    )
                    .unwrap();
                    write_frame(&mut stream, &ack).await.unwrap();
                    stream.flush().await.unwrap();
                    handshaken.send(()).unwrap();
                    tokio::task::spawn_blocking(move || daemon_released.recv())
                        .await
                        .unwrap()
                        .unwrap();
                    drop(stream);
                });
        });

        let stdout_path = temp.path().join("child.stdout");
        let stderr_path = temp.path().join("child.stderr");
        let home = temp.path().join("home");
        std::fs::create_dir_all(&home).unwrap();
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", &format!("tests::{name}"), "--nocapture"])
            .env(CHILD_ENV, &connection_file_path)
            .env("SUBC_MODULE_ID", DEFAULT_MODULE_ID)
            .env("CK_LOG", "info")
            .env("HOME", &home)
            .env("XDG_DATA_HOME", temp.path().join("data"))
            .env("XDG_CONFIG_HOME", temp.path().join("config"))
            .stdout(std::fs::File::create(&stdout_path).unwrap())
            .stderr(std::fs::File::create(&stderr_path).unwrap())
            .spawn()
            .unwrap();
        child_spawned.send(()).unwrap();
        let child_output = || {
            format!(
                "child stdout:\n{}\nchild stderr:\n{}",
                std::fs::read_to_string(&stdout_path).unwrap_or_default(),
                std::fs::read_to_string(&stderr_path).unwrap_or_default()
            )
        };

        if handshake_done
            .recv_timeout(std::time::Duration::from_secs(30))
            .is_err()
        {
            let _ = child.kill();
            panic!(
                "the child never completed the HELLO handshake\n{}",
                child_output()
            );
        }
        // The handler is installed before HELLO is sent, so the child is now
        // serving with the connection open and the refresher running.
        match stop {
            ChildStop::Sigterm => {
                let killed = std::process::Command::new("kill")
                    .args(["-TERM", &child.id().to_string()])
                    .status()
                    .unwrap();
                assert!(killed.success(), "kill -TERM failed");
            }
            ChildStop::DaemonEof => release_daemon.send(()).unwrap(),
        }
        let stopped_at = std::time::Instant::now();
        let status = loop {
            if let Some(status) = child.try_wait().unwrap() {
                break status;
            }
            if stopped_at.elapsed() > EXIT_BOUND {
                let _ = child.kill();
                panic!(
                    "the child did not exit within {EXIT_BOUND:?} of {stop:?}\n{}",
                    child_output()
                );
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        };
        // Already sent, and the receiver gone, in the EOF case.
        let _ = release_daemon.send(());
        daemon.join().unwrap();

        let output = child_output();
        let stdout = std::fs::read_to_string(&stdout_path).unwrap();
        let log_path = stdout
            .lines()
            .find_map(|line| line.strip_prefix(LOG_PATH_PREFIX))
            .unwrap_or_else(|| panic!("the child never reported its log path\n{output}"));
        let log = std::fs::read_to_string(log_path).unwrap();
        let bodies = log
            .lines()
            .map(|line| cortexkit_log::parse_line(line).unwrap().body.to_string())
            .collect();
        Some(ChildEnd {
            status,
            output,
            log,
            bodies,
        })
    }

    /// launchd stopping the daemon SIGTERMs the module's process group, and
    /// the supervisor's teardown sends SIGTERM before SIGKILL. Neither closes
    /// the connection first. The module must take the same clean stop as EOF
    /// (refresher stopped and joined, both shutdown lines logged) and then
    /// die by signal 15, because the supervisor respawns a module that died
    /// by a signal but leaves one that exited 0 stopped.
    ///
    /// Both halves are asserted because each alone is what a broken binary
    /// produced: with no handler the module died by signal 15 with no
    /// shutdown lines, and with a handler that exited 0 it logged both lines
    /// and stayed down.
    #[cfg(unix)]
    #[test]
    fn sigterm_while_the_daemon_connection_is_open_stops_cleanly_then_dies_by_sigterm() {
        use std::os::unix::process::ExitStatusExt;

        let Some(end) = serve_production_run_in_a_child(
            "sigterm_while_the_daemon_connection_is_open_stops_cleanly_then_dies_by_sigterm",
            ChildStop::Sigterm,
        ) else {
            return;
        };
        assert_eq!(
            (end.status.signal(), end.status.code()),
            (Some(15), None),
            "after its clean stop the module must die by SIGTERM so the supervisor \
             respawns it: {:?}\n{}",
            end.status,
            end.output
        );
        end.assert_shutdown_lines("sigterm");
    }

    /// Only SIGTERM re-raises. A daemon that closes the connection is a
    /// deliberate stop, and the module must still exit 0 after it.
    #[cfg(unix)]
    #[test]
    fn daemon_eof_stops_cleanly_and_exits_zero() {
        use std::os::unix::process::ExitStatusExt;

        let Some(end) = serve_production_run_in_a_child(
            "daemon_eof_stops_cleanly_and_exits_zero",
            ChildStop::DaemonEof,
        ) else {
            return;
        };
        assert_eq!(
            (end.status.signal(), end.status.code()),
            (None, Some(0)),
            "a daemon EOF is a deliberate stop and must exit 0: {:?}\n{}",
            end.status,
            end.output
        );
        end.assert_shutdown_lines("eof");
    }

    #[test]
    fn store_lease_is_acquired_before_serving_connections() {
        let root = std::env::temp_dir().join(format!("astrocyte-lease-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let path = cortexkit_store::sqlite_store_path(root.to_str().unwrap(), DEFAULT_MODULE_ID);
        let first = open_state_at_store(&path).unwrap();
        let descriptor = storage_descriptor_at_store(&path);
        assert!(matches!(
            open_sqlite(&descriptor),
            Err(cortexkit_store::StoreError::Lease(_))
        ));
        drop(first);
        let _ = std::fs::remove_dir_all(root);
    }

    /// A relative data home is refused with the reason, and an absolute one is
    /// kept unchanged. The startup integration test checks that the refusal
    /// leaves no files behind in the launch directory.
    #[test]
    fn data_home_must_be_absolute() {
        let refusal = super::absolute_data_home(PathBuf::from(".local/share")).unwrap_err();
        assert!(refusal.contains("resolved to the relative"), "{refusal}");
        let home = std::env::temp_dir().join("astrocyte-data-home");
        assert_eq!(super::absolute_data_home(home.clone()).unwrap(), home);
    }

    /// A store recording a schema version above this binary's chain is the
    /// older-binary-under-newer-store shape, and any downgrade. The shared migrator
    /// skips silently there; startup must refuse before any state is built,
    /// naming both versions, and a store AT the chain's head must still open.
    #[test]
    fn store_ahead_of_binary_refuses_startup_by_name() {
        let root = std::env::temp_dir().join(format!("astrocyte-ahead-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let path = cortexkit_store::sqlite_store_path(root.to_str().unwrap(), DEFAULT_MODULE_ID);

        let current = open_state_at_store(&path).expect("a store at the chain head opens");
        drop(current);

        let expected_highest = MIGRATIONS
            .last()
            .expect("the schema migration chain is non-empty")
            .version;
        let ahead_version = i64::from(expected_highest) + 1;
        {
            let conn = rusqlite::Connection::open(&path).unwrap();
            conn.execute(
                "INSERT INTO cortexkit_schema_version (namespace,version,applied_at_unix) VALUES ('astrocyte',?1,0)",
                [ahead_version],
            )
            .unwrap();
        }
        let error = match open_state_at_store(&path) {
            Ok(_) => panic!("a store ahead of the binary must not open"),
            Err(ModuleError::Message(message)) => message,
            Err(other) => panic!("unexpected refusal shape: {other:?}"),
        };
        assert!(
            error.contains(&format!("version {ahead_version}")),
            "{error}"
        );
        assert!(
            error.contains(&format!("highest migration {expected_highest}")),
            "{error}"
        );
        let _ = std::fs::remove_dir_all(root);
    }
}

#[cfg(test)]
mod launch_nonce_tests {
    use super::*;

    /// Scan every workspace crate's Rust source, including tests. The line-level rule
    /// rejects nonce-key literals on the same line as an env::var or var_os call,
    /// and rejects nonce environment constant identifiers anywhere in code.
    /// Comments are ignored; child env/env_remove setters are not reads. There is
    /// no file allowlist, so production files and this test module are both guarded.
    #[test]
    fn shipped_source_never_reads_launch_nonce_environment_directly() {
        fn rust_files(directory: &Path, files: &mut Vec<PathBuf>) {
            for entry in std::fs::read_dir(directory).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    rust_files(&path, files);
                } else if path.extension().is_some_and(|extension| extension == "rs") {
                    files.push(path);
                }
            }
        }

        // Keep comment text out of the scan while preserving newlines for diagnostics.
        fn without_comments(source: &str) -> String {
            let mut chars = source.chars().peekable();
            let mut code = String::new();
            let mut block_depth = 0;
            let mut quoted = false;
            let mut escaped = false;
            while let Some(ch) = chars.next() {
                if block_depth > 0 {
                    if ch == '/' && chars.peek() == Some(&'*') {
                        chars.next();
                        block_depth += 1;
                    } else if ch == '*' && chars.peek() == Some(&'/') {
                        chars.next();
                        block_depth -= 1;
                        code.push(' ');
                    } else if ch == '\n' {
                        code.push(ch);
                    }
                } else if quoted {
                    code.push(ch);
                    if escaped {
                        escaped = false;
                    } else if ch == '\\' {
                        escaped = true;
                    } else if ch == '"' {
                        quoted = false;
                    }
                } else if ch == '/' && chars.peek() == Some(&'/') {
                    for next in chars.by_ref() {
                        if next == '\n' {
                            code.push(next);
                            break;
                        }
                    }
                } else if ch == '/' && chars.peek() == Some(&'*') {
                    chars.next();
                    block_depth = 1;
                    code.push(' ');
                } else {
                    code.push(ch);
                    quoted = ch == '"';
                }
            }
            code
        }

        let root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .parent()
            .unwrap();
        let mut files = Vec::new();
        for entry in std::fs::read_dir(root.join("crates")).unwrap() {
            let src = entry.unwrap().path().join("src");
            if src.is_dir() {
                rust_files(&src, &mut files);
            }
        }
        assert!(!files.is_empty(), "no workspace Rust source was scanned");
        assert!(files.contains(&root.join("crates/astrocyte-module/src/main.rs")));

        let keys = ["SUBC_LAUNCH_NONCE", "SUBC_LAUNCH_NONCE_FD"];
        let constants = [
            concat!("SUBC_LAUNCH_", "NONCE_ENV"),
            concat!("LAUNCH_NONCE_", "ENV"),
            concat!("LAUNCH_NONCE_FD_", "ENV"),
        ];
        // Whether one line reads the launch nonce from the environment: an
        // `env::var(` or `var_os(` call with a nonce key literal on the same line,
        // or any use of a nonce environment-name constant. Whitespace is ignored.
        // Callers pass lines from `without_comments`, so a comment that merely
        // names a key is never flagged.
        let flags_line = |line: &str| {
            let compact: String = line.chars().filter(|ch| !ch.is_whitespace()).collect();
            let reads_env = compact.contains(concat!("env::", "var("))
                || compact.contains(concat!("var_", "os("));
            let nonce_literal = keys
                .iter()
                .any(|key| compact.contains(&format!("\"{key}\"")));
            let nonce_constant = line
                .split(|ch: char| !ch.is_alphanumeric() && ch != '_')
                .any(|token| constants.contains(&token));
            (reads_env && nonce_literal) || nonce_constant
        };

        // Positive control, run every time: reads the scan must catch, built at
        // run time so this file's own text never contains one. A rule change that
        // stops catching any of them fails here, before the scan can pass
        // vacuously. The last line is a child-environment setter, which is not a
        // read and must stay allowed.
        let quoted = |key: &str| format!("\"{key}\"");
        for planted in [
            format!("let n = std::env::{}{});", "var(", quoted(keys[0])),
            format!("let n = std::env::{} {} );", "var_os(", quoted(keys[1])),
            format!("let n = {}::{};", "subc_protocol", constants[0]),
        ] {
            assert!(
                flags_line(&planted),
                "the scan rule missed a planted read: {planted}"
            );
        }
        let setter = format!("command.env_remove({});", quoted(keys[0]));
        assert!(
            !flags_line(&setter),
            "the scan rule flagged a child-env setter: {setter}"
        );

        let mut violations = Vec::new();
        for file in &files {
            let source = std::fs::read_to_string(file).unwrap();
            for (line_index, line) in without_comments(&source).lines().enumerate() {
                if flags_line(line) {
                    violations.push(format!(
                        "{}:{}: {}",
                        file.strip_prefix(root).unwrap().display(),
                        line_index + 1,
                        line.trim()
                    ));
                }
            }
        }
        assert!(
            violations.is_empty(),
            "direct launch-nonce environment access (use subc_os::launch_nonce):\n{}",
            violations.join("\n")
        );
        println!("scanned {} workspace Rust source files", files.len());
    }

    // subc-os reads the nonce once per process and caches it, so each case
    // runs in its own child process with its own environment.
    fn in_child(name: &str, env: &[(&str, &str)], check: impl FnOnce()) {
        if std::env::var("ASTROCYTE_NONCE_TEST").as_deref() == Ok(name) {
            check();
            return;
        }
        let mut command = std::process::Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--exact",
                &format!("launch_nonce_tests::{name}"),
                "--nocapture",
            ])
            .env("ASTROCYTE_NONCE_TEST", name)
            .env_remove("SUBC_LAUNCH_NONCE")
            .env_remove("SUBC_LAUNCH_NONCE_FD");
        for (key, value) in env {
            command.env(key, value);
        }
        let output =
            crate::test_support::output_within(&mut command, &format!("child test {name}"));
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    async fn hello_or_error() -> (Result<(), ModuleError>, Option<ModuleHelloBody>) {
        let config = ModuleConfig {
            connection_file_path: PathBuf::new(),
            module_id: DEFAULT_MODULE_ID.to_string(),
        };
        let (tx, mut rx) = mpsc::channel(1);
        let result = send_hello(&tx, &config, &config::intentional_empty().config, None).await;
        drop(tx);
        let hello = rx
            .recv()
            .await
            .map(|frame| serde_json::from_slice(&frame.body).unwrap());
        (result, hello)
    }

    #[test]
    fn a_nonce_from_the_environment_is_sent_and_named_env() {
        in_child(
            "a_nonce_from_the_environment_is_sent_and_named_env",
            &[("SUBC_LAUNCH_NONCE", "env-nonce")],
            || {
                let runtime = tokio::runtime::Runtime::new().unwrap();
                let (result, hello) = runtime.block_on(hello_or_error());
                result.unwrap();
                let hello = hello.expect("a HELLO frame");
                assert_eq!(hello.launch_nonce.as_deref(), Some("env-nonce"));
                let provenance = hello.manifest.provenance.expect("provenance");
                assert_eq!(provenance.launch_nonce_source, Some(LaunchNonceSource::Env));
            },
        );
    }

    // Unix descriptor handoffs are authoritative: a malformed one cannot fall
    // back to the environment. Windows has no descriptor handoff and uses env.
    #[cfg(unix)]
    #[test]
    fn a_broken_descriptor_handoff_sends_no_hello_and_never_uses_the_environment_copy() {
        in_child(
            "a_broken_descriptor_handoff_sends_no_hello_and_never_uses_the_environment_copy",
            &[
                ("SUBC_LAUNCH_NONCE_FD", "not-an-fd"),
                ("SUBC_LAUNCH_NONCE", "env-nonce"),
            ],
            || {
                let runtime = tokio::runtime::Runtime::new().unwrap();
                let (result, hello) = runtime.block_on(hello_or_error());
                let error = result
                    .expect_err("a broken handoff must refuse")
                    .to_string();
                assert!(error.contains("launch nonce"), "{error}");
                assert!(hello.is_none(), "a HELLO was sent: {hello:?}");
            },
        );
    }

    #[cfg(windows)]
    #[test]
    fn windows_uses_the_environment_nonce_and_ignores_the_descriptor_variable() {
        in_child(
            "windows_uses_the_environment_nonce_and_ignores_the_descriptor_variable",
            &[
                ("SUBC_LAUNCH_NONCE_FD", "not-an-fd"),
                ("SUBC_LAUNCH_NONCE", "env-nonce"),
            ],
            || {
                let runtime = tokio::runtime::Runtime::new().unwrap();
                let (result, hello) = runtime.block_on(hello_or_error());
                result.unwrap();
                let hello = hello.expect("a HELLO frame");
                assert_eq!(hello.launch_nonce.as_deref(), Some("env-nonce"));
                let provenance = hello.manifest.provenance.expect("provenance");
                assert_eq!(provenance.launch_nonce_source, Some(LaunchNonceSource::Env));
            },
        );
    }
}

#[cfg(test)]
mod logging_tests {
    use super::*;
    use crate::test_support as refresher;

    // Global subscribers and environment belong to a process, not a test thread.
    // Each child writes only under its parent's unique temporary data directory.
    fn isolated(name: &str, spec: &str, check: impl FnOnce(cortexkit_log::Handle)) {
        if std::env::var("ASTROCYTE_LOG_TEST").as_deref() == Ok(name) {
            check(cortexkit_log::init_from_env().unwrap());
            return;
        }
        let root =
            std::env::temp_dir().join(format!("astrocyte-logging-{}-{name}", std::process::id()));
        std::fs::create_dir_all(&root).unwrap();
        let output = crate::test_support::output_within(
            std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", &format!("logging_tests::{name}"), "--nocapture"])
                .env("ASTROCYTE_LOG_TEST", name)
                .env("SUBC_MODULE_ID", "astrocyte")
                .env("CK_LOG", spec)
                .env("XDG_DATA_HOME", &root),
            &format!("child test {name}"),
        );
        std::fs::remove_dir_all(root).unwrap();
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[test]
    fn wire_module_id_matches_filterable_logger_root() {
        isolated(
            "wire_module_id_matches_filterable_logger_root",
            "off,astrocyte=debug",
            |handle| {
                let runtime = tokio::runtime::Runtime::new().unwrap();
                let wire_id = runtime.block_on(async {
                    let config = ModuleConfig {
                        connection_file_path: PathBuf::new(),
                        module_id: std::env::var(SUBC_MODULE_ID_ENV).unwrap(),
                    };
                    let (tx, mut rx) = mpsc::channel(1);
                    send_hello(&tx, &config, &config::intentional_empty().config, None)
                        .await
                        .unwrap();
                    // `tx` is still alive, so a HELLO that was never sent
                    // would wait forever without this bound.
                    let frame = tokio::time::timeout(crate::test_support::HANG_GUARD, rx.recv())
                        .await
                        .expect("send_hello returned without sending a HELLO")
                        .expect("the HELLO channel closed without a frame");
                    let hello: ModuleHelloBody = serde_json::from_slice(&frame.body).unwrap();
                    hello.manifest.module_id
                });
                log_terminal_error(&"root probe");
                let contents = std::fs::read_to_string(handle.path()).unwrap();
                let lines: Vec<_> = contents
                    .lines()
                    .map(|line| cortexkit_log::parse_line(line).unwrap())
                    .collect();
                assert_eq!(lines.len(), 1, "{contents}");
                assert_eq!(lines[0].logger, wire_id);
                assert_eq!(wire_id, "astrocyte");
            },
        );
    }

    #[test]
    fn every_declared_logger_emits_its_component() {
        isolated(
            "every_declared_logger_emits_its_component",
            "trace",
            |handle| {
                log_terminal_error(&"root probe");
                refresher::log_producer_drain_failed("broca", "drain probe", &Ok(()));
                refresher::log_pricing_failed("pricing probe");
                refresher::log_capacity_failed("capacity probe");
                refresher::log_config_retained("config probe");
                let contents = std::fs::read_to_string(handle.path()).unwrap();
                let names: Vec<_> = contents
                    .lines()
                    .map(|line| cortexkit_log::parse_line(line).unwrap().logger)
                    .collect();
                assert_eq!(
                    names,
                    [
                        "astrocyte",
                        "astrocyte.drain",
                        "astrocyte.pricing",
                        "astrocyte.capacity",
                        "astrocyte.config"
                    ]
                );
            },
        );
    }

    #[test]
    fn degraded_state_events_are_warn_or_higher() {
        isolated(
            "degraded_state_events_are_warn_or_higher",
            "trace",
            |handle| {
                refresher::log_config_retained("config probe");
                refresher::log_producer_drain_failed("broca", "drain probe", &Ok(()));
                let contents = std::fs::read_to_string(handle.path()).unwrap();
                let lines: Vec<_> = contents
                    .lines()
                    .map(|line| cortexkit_log::parse_line(line).unwrap())
                    .collect();
                assert_eq!(lines.len(), 2, "{contents}");
                for line in lines {
                    assert!(
                        matches!(
                            line.level,
                            cortexkit_log::ParsedLevel::Warn | cortexkit_log::ParsedLevel::Error
                        ),
                        "degraded event below warn: {line:?}"
                    );
                }
            },
        );
    }

    /// The logger and this module must agree about whether an absent
    /// SUBC_MODULE_ID is fatal. Driven through the PRODUCTION `init_logging`
    /// in a child with the variable removed, so it asserts the fallback arm
    /// rather than the crate's behaviour: a bare `init_from_env` refuses here,
    /// and the refusal would blame a variable no operator sets by hand.
    ///
    /// The stakes are not cosmetic --- `--check-config` runs unsupervised in
    /// the deploy recipe and survives only because it returns before logging
    /// is initialised. This test is what stops a later refactor moving
    /// initialisation earlier and breaking the preflight silently.
    #[test]
    fn unsupervised_startup_names_its_own_module_rather_than_refusing() {
        let name = "unsupervised_startup_names_its_own_module_rather_than_refusing";
        if std::env::var("ASTROCYTE_LOG_TEST").as_deref() == Ok(name) {
            let handle = super::init_logging().expect(
                "unsupervised startup must name its own module rather than refusing on \
                 SUBC_MODULE_ID",
            );
            tracing::error!(target: "astrocyte", "unsupervised probe");
            let contents = std::fs::read_to_string(handle.path()).unwrap();
            let line = cortexkit_log::parse_line(contents.lines().next().unwrap()).unwrap();
            assert_eq!(line.logger, DEFAULT_MODULE_ID);
            return;
        }
        let root =
            std::env::temp_dir().join(format!("astrocyte-logging-{}-{name}", std::process::id()));
        std::fs::create_dir_all(&root).unwrap();
        let output = crate::test_support::output_within(
            std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", &format!("logging_tests::{name}"), "--nocapture"])
                .env("ASTROCYTE_LOG_TEST", name)
                .env_remove("SUBC_MODULE_ID")
                .env("CK_LOG", "trace")
                .env("XDG_DATA_HOME", &root),
            &format!("child test {name}"),
        );
        std::fs::remove_dir_all(&root).unwrap();
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}
