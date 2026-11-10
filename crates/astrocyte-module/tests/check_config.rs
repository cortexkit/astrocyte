//! Config preflight must reject retired sources before starting the module.

mod common;

use common::{ckdev_binary, entries_under, output_within_guard};
use std::process::Command;

#[test]
fn check_config_rejects_removed_claude_workers_source() {
    let directory = tempfile::tempdir().unwrap();
    let home = directory.path().join("home");
    std::fs::create_dir(&home).unwrap();
    let config = directory.path().join("astrocyte.jsonc");
    std::fs::write(
        &config,
        r#"{"poll_interval_secs":30,"qta_connection_file":null,"fusiform_connection_file":null,"reasoning_billed_as_output":[],"subscription_multiplier":{"api_twins":{},"plan_price_overrides":{},"quota_providers":{}},"harness_ingest":{"enabled":true,"exclude":["claude-code-workers"]}}"#,
    )
    .unwrap();
    let output = output_within_guard(
        Command::new(ckdev_binary())
            .arg("--check-config")
            .arg(&config)
            .env_remove("SUBC_MODULE_ID")
            .env_remove("SUBC_LAUNCH_NONCE")
            .env_remove("SUBC_LAUNCH_NONCE_FD")
            .env("HOME", &home)
            .env("XDG_DATA_HOME", home.join("data"))
            .env("XDG_CONFIG_HOME", home.join("config")),
        "ckdev-astrocyte --check-config",
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !output.status.success(),
        "retired source was accepted: exit {:?}\n{stdout}\n{stderr}",
        output.status
    );
    assert!(stdout.is_empty(), "unexpected stdout: {stdout}");
    assert!(
        stderr.contains("harness_ingest.exclude names unknown producer 'claude-code-workers'"),
        "{stderr}"
    );
    let (_, known) = stderr.split_once("known producers are ").unwrap();
    assert!(!known.contains("claude-code-workers"), "{stderr}");
    assert!(known.contains("claude-code-thalamus"), "{stderr}");
    assert!(
        entries_under(&home).is_empty(),
        "config preflight started the module"
    );
}
