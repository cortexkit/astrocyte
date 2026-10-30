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
    // The status is in the message because a process that never started (on
    // Windows, a loader failure under load) also exits non-zero with empty
    // stderr, and only its exit code tells that apart from the guard.
    assert!(
        stderr.contains("data home resolved to the relative"),
        "the refusal must say why (exit {:?}): {stderr}",
        output.status
    );
}

/// The supervisor always passes `--subc`, so a run without it was started by
/// hand or by another tool. It must refuse on stderr without opening the
/// module's log: otherwise every stray run writes a false "module stopped"
/// error into the same production log the running module writes.
#[test]
fn a_run_without_subc_refuses_without_writing_the_log() {
    let home = std::env::temp_dir().join(format!("astrocyte-bare-run-{}", std::process::id()));
    let data_home = home.join("data");
    std::fs::create_dir_all(&data_home).unwrap();
    let binary = ckdev_binary();
    let output = output_within_guard(
        Command::new(&binary)
            .current_dir(&home)
            .env("HOME", &home)
            .env("XDG_DATA_HOME", &data_home)
            .env("XDG_CONFIG_HOME", home.join("config"))
            .env("XDG_RUNTIME_DIR", home.join("run"))
            .env("APPDATA", &data_home)
            .env("USERPROFILE", &home)
            .env_remove("SUBC_MODULE_ID")
            .env_remove("SUBC_LAUNCH_NONCE")
            .env_remove("SUBC_LAUNCH_NONCE_FD"),
        "ckdev-astrocyte run without --subc",
    );
    let written = entries_under(&data_home);
    std::fs::remove_dir_all(&home).unwrap();

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        written.is_empty(),
        "a run without --subc wrote under the data home: {written:?}"
    );
    assert!(!output.status.success(), "a run without --subc must fail");
    assert!(
        stderr.contains("missing required --subc"),
        "the refusal must say why on stderr: {stderr}"
    );
}

/// `ck` probes every `ck-<name>` in its bin dir with `--ck-domain` to list
/// command domains. Astrocyte offers none, so it must decline with a non-zero
/// exit, print nothing, and leave the data home untouched.
#[test]
fn the_ck_domain_probe_is_declined_silently() {
    let home = std::env::temp_dir().join(format!("astrocyte-ck-domain-{}", std::process::id()));
    let data_home = home.join("data");
    std::fs::create_dir_all(&data_home).unwrap();
    let binary = ckdev_binary();
    let output = output_within_guard(
        Command::new(&binary)
            .arg("--ck-domain")
            .current_dir(&home)
            .env("HOME", &home)
            .env("XDG_DATA_HOME", &data_home)
            .env("XDG_CONFIG_HOME", home.join("config"))
            .env("XDG_RUNTIME_DIR", home.join("run"))
            .env("APPDATA", &data_home)
            .env("USERPROFILE", &home),
        "ckdev-astrocyte --ck-domain",
    );
    let written = entries_under(&data_home);
    std::fs::remove_dir_all(&home).unwrap();

    assert!(
        written.is_empty(),
        "the probe wrote under the data home: {written:?}"
    );
    assert!(!output.status.success(), "the probe must be declined");
    assert!(
        output.stdout.is_empty() && output.stderr.is_empty(),
        "the probe must be declined silently: stdout={:?} stderr={:?}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
