//! What the real binary does at startup before it ever reaches the daemon.

mod common;

use common::{ckdev_binary, entries_under, output_within_guard};
use std::process::Command;

/// With no home variable at all, the data home resolves to a relative path.
/// Starting there would put a second, empty ledger and its logs under
/// whatever directory the process was launched from, and that ledger would
/// look healthy. The binary must refuse before it writes anything, so the
/// launch directory is still empty afterwards.
#[test]
fn startup_without_a_home_refuses_before_writing_anything() {
    let cwd = std::env::temp_dir().join(format!("astrocyte-no-home-{}", std::process::id()));
    std::fs::create_dir_all(&cwd).unwrap();
    let binary = ckdev_binary();
    let output = output_within_guard(
        Command::new(&binary)
            .current_dir(&cwd)
            .env_remove("HOME")
            .env_remove("XDG_DATA_HOME")
            .env_remove("XDG_CONFIG_HOME")
            .env_remove("APPDATA")
            .env_remove("USERPROFILE")
            .env_remove("SUBC_MODULE_ID")
            .env_remove("SUBC_LAUNCH_NONCE")
            .env_remove("SUBC_LAUNCH_NONCE_FD"),
        "ckdev-astrocyte started without a home",
    );
    let written = entries_under(&cwd);
    std::fs::remove_dir_all(&cwd).unwrap();

    let stderr = String::from_utf8_lossy(&output.stderr);
    // Check the effect first. Without the guard the binary still fails a
    // moment later, because no supervisor handed it the one-time launch code
    // it needs to connect; only the files it left behind show whether the
    // guard held.
    assert!(
        written.is_empty(),
        "startup without a home wrote under the launch directory: {written:?}"
    );
    assert!(
        !output.status.success(),
        "startup without a home must fail: {stderr}"
    );
    assert!(
        stderr.contains("data home resolved to the relative"),
        "the refusal must say why: {stderr}"
    );
}
