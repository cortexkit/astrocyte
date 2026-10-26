//! `ck-astrocyte --version` is what SubC's placement script runs to warm a
//! freshly placed binary: unsupervised, with no `--subc`, no launch nonce and
//! no module id. It must answer and exit without starting any part of the
//! module, so the test drives the real binary in exactly that environment.

mod common;

use common::{ckdev_binary, entries_under, output_within_guard};
use std::process::Command;

#[cfg(target_os = "macos")]
#[test]
fn copy_root_follows_tmpdir_except_under_var_folders() {
    use common::macos_copy_root;
    use std::path::PathBuf;

    let worker = tempfile::tempdir_in("/tmp").unwrap();
    let real = std::fs::canonicalize(worker.path()).unwrap();
    assert_eq!(
        std::fs::canonicalize(macos_copy_root(Some(worker.path().into()))).unwrap(),
        real
    );
    assert_eq!(macos_copy_root(None), PathBuf::from("/tmp"));
    assert_eq!(
        macos_copy_root(Some("relative/dir".into())),
        PathBuf::from("/tmp")
    );
    assert_eq!(
        macos_copy_root(Some("/var/folders".into())),
        PathBuf::from("/tmp")
    );
}

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
    let fields = lines[0].strip_prefix("ck-astrocyte ").unwrap();
    let (version, provenance) = fields.split_once(" (commit ").unwrap();
    assert_eq!(version, env!("CARGO_PKG_VERSION"));
    let provenance = provenance.strip_suffix(')').unwrap();
    let (sha, suffix) = provenance.split_once(", ").unwrap_or((provenance, ""));
    let build_sha = env!("CK_ASTROCYTE_BUILD_GIT_SHA");
    assert_eq!(
        sha,
        if build_sha.is_empty() {
            "unknown"
        } else {
            build_sha
        }
    );
    assert_eq!(
        suffix,
        match (build_sha, env!("CK_ASTROCYTE_BUILD_TREE_STATE")) {
            ("", _) | (_, "clean") => "",
            (_, "dirty") => "built from a dirty tree",
            _ => "tree state unknown",
        }
    );
}
