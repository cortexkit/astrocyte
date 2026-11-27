use super::*;
use std::{
    os::{fd::OwnedFd, unix::fs::MetadataExt, unix::process::ExitStatusExt},
    process::{Child, Command, Stdio},
    time::Duration,
};
use subc_os::registration_channel::{DAEMON_PID_ENV, REGISTRATION_FD_ENV};
use tokio::net::UnixStream;

const CHILD_PATH: &str = "ASTROCYTE_REGISTRATION_TEST_PATH";
const BUDGET: Duration = Duration::from_secs(15);
const REFRESHER_ERROR: &str = "injected refresher failure during shutdown";
const LOG_PATH_PREFIX: &str = "ASTROCYTE_LOG_PATH=";

#[derive(Clone, Copy)]
enum End {
    Goodbye,
    Eof,
    // Constructed only by the Linux-only reset test.
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    Reset,
    Sigterm,
}

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn spawn_copied_executable(command: &mut Command) -> Child {
    let deadline = std::time::Instant::now() + BUDGET;
    loop {
        match command.spawn() {
            Ok(child) => return child,
            // A parallel test's fork can briefly retain the copy's writable
            // descriptor until exec closes it. Wait for that child to exec.
            Err(error)
                if error.raw_os_error() == Some(libc::ETXTBSY)
                    && std::time::Instant::now() < deadline =>
            {
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(error) => panic!("spawning the copied test executable: {error}"),
        }
    }
}

async fn registration_end(name: &str, end: End, refresher_fails: bool) {
    if let Some(path) = std::env::var_os(CHILD_PATH) {
        let log = init_logging().unwrap();
        println!("{LOG_PATH_PREFIX}{}", log.path().display());
        let outcome = serve_module(
            &ModuleConfig {
                connection_file_path: path.into(),
                module_id: DEFAULT_MODULE_ID.into(),
            },
            Arc::new(test_state()),
            move |_, stop| {
                Ok(std::thread::spawn(move || {
                    while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                        std::thread::sleep(Duration::from_millis(1));
                    }
                    if refresher_fails {
                        Err(REFRESHER_ERROR.into())
                    } else {
                        Ok(())
                    }
                }))
            },
            sigterm_received().unwrap(),
        )
        .await;
        // Only a local close returns; daemon ends must exit inside the SDK.
        assert!(matches!(end, End::Sigterm), "SDK returned: {outcome:?}");
        let code = finish_process(outcome);
        panic!("a SIGTERM outcome must end the process, not return {code:?}");
    }

    let directory = tempfile::TempDir::new().unwrap();
    let path = directory.path().join("connection.json");
    let connection = ConnectionInfo {
        schema: SCHEMA_VERSION,
        wire_version: None,
        endpoints: vec![Endpoint {
            host: "127.0.0.1".into(),
            port: 9,
        }],
        key: vec![7; KEY_LEN],
        daemon_id: [9; 16],
        pid: std::process::id(),
        daemon_ver: "registration-test".into(),
    };
    connection_file::write_atomic(&path, &connection).unwrap();
    let (server, client) = std::os::unix::net::UnixStream::pair().unwrap();
    let fd: OwnedFd = client.into();
    let file = std::fs::File::from(fd);
    let inode = file.metadata().unwrap().ino();
    server.set_nonblocking(true).unwrap();
    let mut server = UnixStream::from_std(server).unwrap();
    let executable = directory.path().join("ckdev-astrocyte");
    std::fs::copy(std::env::current_exe().unwrap(), &executable).unwrap();
    let stdout_path = directory.path().join("child.stdout");
    let stderr_path = directory.path().join("child.stderr");
    let mut child = ChildGuard(spawn_copied_executable(
        Command::new(executable)
            .args([
                "--exact",
                &format!("tests::registration_shutdown::{name}"),
                "--nocapture",
            ])
            .env(CHILD_PATH, &path)
            // Stdio safely places the socket at fd 0 without a pre-exec hook.
            .env(REGISTRATION_FD_ENV, format!("0:{inode}"))
            .env(DAEMON_PID_ENV, std::process::id().to_string())
            .env("SUBC_MODULE_ID", DEFAULT_MODULE_ID)
            .env_remove("SUBC_LAUNCH_NONCE")
            .env_remove("SUBC_LAUNCH_NONCE_FD")
            .env("CK_LOG", "info")
            .env("HOME", directory.path())
            .env("XDG_DATA_HOME", directory.path().join("data"))
            .env("XDG_CONFIG_HOME", directory.path().join("config"))
            .stdin(Stdio::from(file))
            .stdout(std::fs::File::create(&stdout_path).unwrap())
            .stderr(std::fs::File::create(&stderr_path).unwrap()),
    ));
    let output = || {
        format!(
            "stdout:\n{}\nstderr:\n{}",
            std::fs::read_to_string(&stdout_path).unwrap(),
            std::fs::read_to_string(&stderr_path).unwrap(),
        )
    };

    let status = tokio::time::timeout(BUDGET, async {
        authenticate_server(
            &mut server,
            &connection.key,
            &connection.daemon_id,
            &connection.daemon_ver,
            BUDGET,
        )
        .await
        .unwrap();
        let hello = read_frame(&mut server).await.unwrap().unwrap();
        let hello: ModuleHelloBody = serde_json::from_slice(&hello.body).unwrap();
        assert_eq!(hello.manifest.registration_channel, Some(true));
        write_frame(&mut server, &hello_ack()).await.unwrap();
        server.flush().await.unwrap();
        match end {
            End::Goodbye => {
                write_frame(&mut server, &control_frame(FrameType::Goodbye))
                    .await
                    .unwrap();
                server.flush().await.unwrap();
            }
            End::Eof => {}
            End::Reset => {
                // Closing a Unix socket with unread data sends ECONNRESET.
                // Read only PONG's first header byte, leaving the rest unread.
                write_frame(&mut server, &control_frame(FrameType::Ping))
                    .await
                    .unwrap();
                server.flush().await.unwrap();
                tokio::io::AsyncReadExt::read_exact(&mut server, &mut [0_u8; 1])
                    .await
                    .unwrap();
            }
            End::Sigterm => {
                assert!(Command::new("kill")
                    .args(["-TERM", &child.0.id().to_string()])
                    .status()
                    .unwrap()
                    .success());
                loop {
                    let frame = read_frame(&mut server).await.unwrap().unwrap();
                    if frame.header.ty == FrameType::Goodbye {
                        break;
                    }
                }
            }
        }
        drop(server);
        loop {
            if let Some(status) = child.0.try_wait().unwrap() {
                break status;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("registration child did not finish: {}", output()));

    let stdout = std::fs::read_to_string(&stdout_path).unwrap();
    let log_path = stdout
        .lines()
        .find_map(|line| line.strip_prefix(LOG_PATH_PREFIX))
        .unwrap_or_else(|| panic!("child did not report its log: {}", output()));
    let log = std::fs::read_to_string(log_path)
        .unwrap_or_else(|error| panic!("reading child log: {error}; {}", output()));
    let bodies: Vec<_> = log
        .lines()
        .map(|line| cortexkit_log::parse_line(line).unwrap().body.to_string())
        .collect();
    let observed = ChildEnd {
        status,
        output: output(),
        log,
        bodies,
    };
    let cause = match end {
        End::Goodbye => "goodbye",
        End::Eof => "eof",
        End::Reset => "error",
        End::Sigterm => "sigterm",
    };
    observed.assert_shutdown_lines(cause);
    if matches!(end, End::Sigterm) {
        assert_eq!((status.signal(), status.code()), (Some(15), None));
    } else {
        let expected = i32::from(refresher_fails || !matches!(end, End::Goodbye));
        assert_eq!(status.code(), Some(expected), "{}", observed.output);
    }
    if refresher_fails {
        assert!(
            observed.bodies.iter().any(|body| {
                body.starts_with("module stopped") && body.contains(REFRESHER_ERROR)
            }),
            "{}",
            observed.log
        );
    }
    if matches!(end, End::Reset) {
        assert!(
            observed.log.contains("the daemon connection was reset"),
            "{}",
            observed.log
        );
    }
}

fn control_frame(ty: FrameType) -> Frame {
    Frame::build(
        ty,
        Flags::new(false, Priority::Passive, false),
        0,
        0,
        0,
        Vec::new(),
    )
    .unwrap()
}

// Only Linux turns a Unix socket closed with unread data into ECONNRESET for
// the peer. macOS delivers a plain EOF in that case, which the EOF test below
// already covers with the same nonzero exit.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn registration_reset_exits_nonzero_after_joining_refresher() {
    registration_end(
        "registration_reset_exits_nonzero_after_joining_refresher",
        End::Reset,
        false,
    )
    .await;
}

#[tokio::test]
async fn registration_goodbye_exits_zero_after_joining_refresher() {
    registration_end(
        "registration_goodbye_exits_zero_after_joining_refresher",
        End::Goodbye,
        false,
    )
    .await;
}

#[tokio::test]
async fn registration_eof_exits_nonzero_after_joining_refresher() {
    registration_end(
        "registration_eof_exits_nonzero_after_joining_refresher",
        End::Eof,
        false,
    )
    .await;
}

#[tokio::test]
async fn registration_goodbye_preserves_refresher_error_and_logs_it() {
    registration_end(
        "registration_goodbye_preserves_refresher_error_and_logs_it",
        End::Goodbye,
        true,
    )
    .await;
}

#[tokio::test]
async fn registration_eof_logs_refresher_error_before_sdk_exit() {
    registration_end(
        "registration_eof_logs_refresher_error_before_sdk_exit",
        End::Eof,
        true,
    )
    .await;
}

#[tokio::test]
async fn registration_sigterm_closes_locally_then_dies_by_signal() {
    registration_end(
        "registration_sigterm_closes_locally_then_dies_by_signal",
        End::Sigterm,
        false,
    )
    .await;
}
