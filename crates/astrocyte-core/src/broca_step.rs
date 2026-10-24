//! BROCA's immutable model-step diagnostics. These facts never enter the spend ledger.

use rusqlite::{params_from_iter, types::Value as SqlValue, Transaction};
use serde::{Deserialize, Serialize};

/// Retention measured from the producer's finished time, not ingestion time.
pub const BROCA_STEP_RETENTION_MS: i64 = 30 * 24 * 60 * 60 * 1_000;
pub const RETENTION_BATCH: usize = 500;

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct BindIdentity {
    pub project_root: String,
    pub harness: String,
    pub session: String,
}

/// Unreported counts stay absent. Unknown usage members are refused because
/// silently dropping a new token role would give incomplete diagnostics.
#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ModelStepUsage {
    pub input_tokens: Option<i64>,
    pub cached_input_tokens: Option<i64>,
    pub cache_write_tokens: Option<i64>,
    pub output_tokens: Option<i64>,
    pub reasoning_tokens: Option<i64>,
}

/// Top-level extensions are deliberately ignored; only the agreed members
/// participate in the insert-once comparison.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct ModelStepFact {
    pub fact_id: String,
    pub run_id: String,
    pub step_id: i64,
    pub lineage_id: Option<String>,
    pub session_ref: Option<String>,
    pub session: BindIdentity,
    pub provider: String,
    pub model: String,
    pub variant: Option<String>,
    pub wire_family: Option<String>,
    #[serde(default)]
    pub usage: ModelStepUsage,
    pub finish_reason: Option<String>,
    pub attempts: Option<i64>,
    pub retried: Option<bool>,
    pub request_body_len: Option<i64>,
    pub request_body_sha256: Option<String>,
    pub started_at_ms: Option<i64>,
    pub first_content_at_ms: Option<i64>,
    pub finished_at_ms: i64,
    pub context_limit: Option<i64>,
    pub cache_policy: Option<serde_json::Value>,
    pub agent_id: Option<String>,
    pub agent_run_id: Option<String>,
}

const COLUMNS: &str = "run_id,step_id,fact_id,lineage_id,session_ref,project_root,harness,session,provider,model,variant,wire_family,input_tokens,cached_input_tokens,cache_write_tokens,output_tokens,reasoning_tokens,finish_reason,attempts,retried,request_body_len,request_body_sha256,started_at_ms,first_content_at_ms,finished_at_ms,context_limit,cache_policy,agent_id,agent_run_id";

impl ModelStepFact {
    fn values(&self) -> Vec<SqlValue> {
        vec![
            self.run_id.clone().into(),
            self.step_id.into(),
            self.fact_id.clone().into(),
            self.lineage_id.clone().into(),
            self.session_ref.clone().into(),
            self.session.project_root.clone().into(),
            self.session.harness.clone().into(),
            self.session.session.clone().into(),
            self.provider.clone().into(),
            self.model.clone().into(),
            self.variant.clone().into(),
            self.wire_family.clone().into(),
            self.usage.input_tokens.into(),
            self.usage.cached_input_tokens.into(),
            self.usage.cache_write_tokens.into(),
            self.usage.output_tokens.into(),
            self.usage.reasoning_tokens.into(),
            self.finish_reason.clone().into(),
            self.attempts.into(),
            self.retried.map(i64::from).into(),
            self.request_body_len.into(),
            self.request_body_sha256.clone().into(),
            self.started_at_ms.into(),
            self.first_content_at_ms.into(),
            self.finished_at_ms.into(),
            self.context_limit.into(),
            self.cache_policy.as_ref().map(ToString::to_string).into(),
            self.agent_id.clone().into(),
            self.agent_run_id.clone().into(),
        ]
    }

    pub fn validate(&self) -> Result<(), String> {
        for (name, value) in COLUMNS.split(',').zip(self.values()) {
            if matches!(value, SqlValue::Integer(n) if n < 0) {
                return Err(format!("model_step {name} is negative"));
            }
        }
        Ok(())
    }
}

/// Insert once, or compare every stored member on a re-read. A contradiction
/// is evidence for the drain to set aside, never an UPDATE or a ledger event.
/// `Ok(Some(reason))` is a contradiction; exact re-reads return `Ok(None)`.
pub fn record_in_tx(
    tx: &Transaction<'_>,
    fact: &ModelStepFact,
    now_ms: i64,
) -> rusqlite::Result<Option<String>> {
    let exists: bool = tx.prepare_cached(
        "SELECT EXISTS(SELECT 1 FROM broca_step WHERE fact_id=?1 OR (run_id=?2 AND step_id=?3))",
    )?.query_row(rusqlite::params![fact.fact_id, fact.run_id, fact.step_id], |row| row.get(0))?;
    let mut values = fact.values();
    if exists {
        let predicate = COLUMNS
            .split(',')
            .map(|name| format!("{name} IS ?"))
            .collect::<Vec<_>>()
            .join(" AND ");
        let same: bool = tx
            .prepare_cached(&format!(
                "SELECT EXISTS(SELECT 1 FROM broca_step WHERE {predicate})"
            ))?
            .query_row(params_from_iter(values), |row| row.get(0))?;
        return Ok((!same).then(|| {
            format!(
                "contradicting model_step re-read for {}#{} (fact {})",
                fact.run_id, fact.step_id, fact.fact_id
            )
        }));
    }
    values.push(now_ms.into());
    let placeholders = vec!["?"; values.len()].join(",");
    tx.prepare_cached(&format!(
        "INSERT INTO broca_step({COLUMNS},recorded_at_ms) VALUES ({placeholders})"
    ))?
    .execute(params_from_iter(values))?;
    Ok(None)
}

/// One maintenance transaction removes at most 500 rows, leaving backlog for
/// later cycles so retention cannot monopolize the writer.
pub fn expire_in_tx(tx: &Transaction<'_>, now_ms: i64) -> rusqlite::Result<usize> {
    let deleted = tx.execute(
        "DELETE FROM broca_step WHERE step_key IN
         (SELECT step_key FROM broca_step WHERE finished_at_ms < ?1
          ORDER BY finished_at_ms,step_key LIMIT ?2)",
        rusqlite::params![
            now_ms.saturating_sub(BROCA_STEP_RETENTION_MS),
            i64::try_from(RETENTION_BATCH).unwrap_or(500)
        ],
    )?;
    if deleted > 0 {
        tx.execute_batch(
            "CREATE TEMP TABLE IF NOT EXISTS broca_step_expired (id INTEGER PRIMARY KEY);
            INSERT OR IGNORE INTO broca_step_expired VALUES(1)",
        )?;
    }
    Ok(deleted)
}
