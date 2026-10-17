//! `ck-astrocyte --version` is what SubC's placement script runs to warm a
//! freshly placed binary: unsupervised, with no `--subc`, no launch nonce and
//! no module id. It must answer and exit without starting any part of the
//! module, so the test drives the real binary in exactly that environment.

mod common;

use common::{ckdev_binary, entries_under, output_within_guard};
use std::process::Command;

#[test]
fn version_prints_one_line_and_starts_nothing() {
    let home = std::env::temp_dir().join(format!("astrocyte-version-{}", std::process::id()));
    std::fs::create_dir_all(&home).unwrap();
    let binary = ckdev_binary();
    let output = output_within_guard(
        Command::new(&binary)
            .arg("--version")
            .env_remove("SUBC_MODULE_ID")
            .env_remove("SUBC_LAUNCH_NONCE")
            .env_remove("SUBC_LAUNCH_NONCE_FD")
            .env("HOME", &home)
            .env("XDG_DATA_HOME", home.join("data"))
            .env("XDG_CONFIG_HOME", home.join("config")),
        "ckdev-astrocyte --version",
    );
    // Logging, config hydration or the store would each create files under
    // the data or config home; a version answer creates none.
    let written = entries_under(&home);
    std::fs::remove_dir_all(&home).unwrap();

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "exit {:?}\n{stdout}\n{stderr}",
        output.status
    );
    assert!(stderr.is_empty(), "--version wrote to stderr: {stderr}");
    assert!(written.is_empty(), "--version wrote files: {written:?}");
    let lines: Vec<&str> = stdout.lines().collect();
    assert_eq!(lines.len(), 1, "--version must print one line: {stdout:?}");
    assert!(
        lines[0].starts_with(&format!(
            "ck-astrocyte {} (commit ",
            env!("CARGO_PKG_VERSION")
        )),
        "unexpected version line: {}",
        lines[0]
    );
    let sha = env!("CK_ASTROCYTE_BUILD_GIT_SHA");
    if !sha.is_empty() {
        assert!(
            lines[0].contains(sha),
            "version line lacks the build commit {sha}: {}",
            lines[0]
        );
    }
}
