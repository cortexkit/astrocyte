// Copied from tokscale v4.17.0,
// crates/tokscale-core/src/sessions/mod.rs.
// tokscale is MIT licensed, Copyright (c) 2025 Junho Yeo.
// See ../../LICENSE-tokscale for the full upstream license text.
// ASTROCYTE DIVERGENCE: this reader crate retains the unified record, workspace
// helpers, and only the SQLite-backed OpenCode and Goose and the JSONL Pi,
// Claude Code, Codex, Gemini CLI, Oh My Pi and Kimi CLI source paths.

pub mod claudecode;
pub mod codex;
pub mod gemini;
pub mod goose;
pub mod jsonl_resume;
pub mod kimi;
pub mod omp;
pub mod opencode;
pub(crate) mod opencode_schema;
pub mod pi;
pub mod roles;
mod sqlite_read;
pub(crate) mod utils;

use crate::TokenBreakdown;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum CostSource {
    #[default]
    Unknown,
    ProviderReported,
    Estimated,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct UnifiedMessage {
    pub client: String,
    pub model_id: String,
    pub provider_id: String,
    pub session_id: String,
    pub workspace_key: Option<String>,
    pub workspace_label: Option<String>,
    pub timestamp: i64,
    pub date: String,
    pub tokens: TokenBreakdown,
    pub cost: f64,
    #[serde(default)]
    pub cost_source: CostSource,
    #[serde(default)]
    pub duration_ms: Option<i64>,
    #[serde(default = "default_message_count")]
    pub message_count: i32,
    pub agent: Option<String>,
    pub dedup_key: Option<String>,
    #[serde(default)]
    pub session_title: Option<String>,
    #[serde(default)]
    pub is_turn_start: bool,
    #[serde(default)]
    pub model_attribution_conflicted: bool,
    /// ASTROCYTE DIVERGENCE: true when reported output and reasoning cannot be
    /// normalized into disjoint buckets without inventing a quantity.
    #[serde(default)]
    pub output_reasoning_conflicted: bool,
}

const fn default_message_count() -> i32 {
    1
}

impl UnifiedMessage {
    pub fn new(
        client: impl Into<String>,
        model_id: impl Into<String>,
        provider_id: impl Into<String>,
        session_id: impl Into<String>,
        timestamp: i64,
        tokens: TokenBreakdown,
        cost: f64,
    ) -> Self {
        Self::new_full(
            client,
            model_id,
            provider_id,
            session_id,
            timestamp,
            tokens,
            cost,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_with_agent(
        client: impl Into<String>,
        model_id: impl Into<String>,
        provider_id: impl Into<String>,
        session_id: impl Into<String>,
        timestamp: i64,
        tokens: TokenBreakdown,
        cost: f64,
        agent: Option<String>,
    ) -> Self {
        Self::new_full(
            client,
            model_id,
            provider_id,
            session_id,
            timestamp,
            tokens,
            cost,
            agent,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn new_full(
        client: impl Into<String>,
        model_id: impl Into<String>,
        provider_id: impl Into<String>,
        session_id: impl Into<String>,
        timestamp: i64,
        tokens: TokenBreakdown,
        cost: f64,
        agent: Option<String>,
    ) -> Self {
        Self {
            client: client.into(),
            model_id: model_id.into(),
            provider_id: provider_id.into(),
            session_id: session_id.into(),
            workspace_key: None,
            workspace_label: None,
            timestamp,
            date: timestamp_to_date(timestamp),
            tokens,
            cost,
            cost_source: CostSource::Unknown,
            duration_ms: None,
            message_count: default_message_count(),
            agent,
            dedup_key: None,
            session_title: None,
            is_turn_start: false,
            model_attribution_conflicted: false,
            output_reasoning_conflicted: false,
        }
    }

    pub fn set_workspace(
        &mut self,
        workspace_key: Option<String>,
        workspace_label: Option<String>,
    ) {
        self.workspace_key = workspace_key;
        self.workspace_label = workspace_label;
    }
}

pub fn normalize_workspace_key(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }

    let preserve_unc_prefix = trimmed.starts_with("\\\\") || trimmed.starts_with("//");
    let mut normalized = trimmed.replace('\\', "/");

    if preserve_unc_prefix {
        let body = normalized.trim_start_matches('/');
        let mut collapsed = body.to_string();
        while collapsed.contains("//") {
            collapsed = collapsed.replace("//", "/");
        }
        normalized = format!("//{collapsed}");
    } else {
        while normalized.contains("//") {
            normalized = normalized.replace("//", "/");
        }
    }

    let minimum_len = if preserve_unc_prefix {
        2
    } else if has_windows_drive_prefix(&normalized) {
        3
    } else {
        1
    };
    if normalized.len() > minimum_len {
        normalized = normalized.trim_end_matches('/').to_string();
    }

    if normalized.is_empty() {
        None
    } else {
        Some(normalized)
    }
}

fn has_windows_drive_prefix(key: &str) -> bool {
    let bytes = key.as_bytes();
    bytes.len() >= 3 && bytes[0].is_ascii_alphabetic() && bytes[1..3] == *b":/"
}

pub fn workspace_label_from_key(key: &str) -> Option<String> {
    key.rsplit('/')
        .find(|segment| !segment.is_empty())
        .map(str::to_string)
}

/// Convert Unix milliseconds to a local YYYY-MM-DD date string.
fn timestamp_to_date(timestamp_ms: i64) -> String {
    use chrono::TimeZone;

    // ASTROCYTE DIVERGENCE: tokscale delegates to its bucket-timezone module.
    // This crate has no bucketing policy; Pi preserves the upstream local-date
    // behavior directly until ingestion supplies its own canonical timestamp.
    chrono::Local
        .timestamp_millis_opt(timestamp_ms)
        .single()
        .map(|timestamp| timestamp.format("%Y-%m-%d").to_string())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unified_message_creation() {
        let tokens = TokenBreakdown {
            input: Some(100),
            output: Some(50),
            cache_read: Some(0),
            cache_write: Some(0),
            reasoning: Some(0),
        };

        let msg = UnifiedMessage::new(
            "opencode",
            "claude-3-5-sonnet",
            "anthropic",
            "test-session-id",
            1733011200000,
            tokens,
            0.05,
        );

        assert_eq!(msg.client, "opencode");
        assert_eq!(msg.model_id, "claude-3-5-sonnet");
        assert_eq!(msg.session_id, "test-session-id");
        assert_eq!(msg.date, timestamp_to_date(1733011200000));
        assert_eq!(msg.cost, 0.05);
        assert_eq!(msg.agent, None);
        assert_eq!(msg.workspace_key, None);
        assert_eq!(msg.workspace_label, None);
        assert!(!msg.output_reasoning_conflicted);
    }

    #[test]
    fn test_normalize_workspace_key_normalizes_slashes_and_trailing_separator() {
        assert_eq!(
            normalize_workspace_key(r"C:\Users\alice\repo\"),
            Some("C:/Users/alice/repo".to_string())
        );
        assert_eq!(
            normalize_workspace_key("/Users/alice//repo/"),
            Some("/Users/alice/repo".to_string())
        );
    }

    #[test]
    fn test_normalize_workspace_key_preserves_unc_prefix() {
        assert_eq!(
            normalize_workspace_key(r"\\server\share\repo\"),
            Some("//server/share/repo".to_string())
        );
        assert_eq!(
            normalize_workspace_key("//server//share///repo/"),
            Some("//server/share/repo".to_string())
        );
    }

    #[test]
    fn test_workspace_label_from_key_uses_last_path_segment() {
        assert_eq!(
            workspace_label_from_key("/Users/alice/my-repo"),
            Some("my-repo".to_string())
        );
        assert_eq!(
            workspace_label_from_key("encoded-project-key"),
            Some("encoded-project-key".to_string())
        );
    }
}
