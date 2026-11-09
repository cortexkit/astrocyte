//! Append-only observations of the plan attached to a vault record.

use rusqlite::{params, Connection, OptionalExtension, Transaction};
use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct VaultPlan {
    pub credential_id: String,
    pub provider: Option<String>,
    pub account_id: Option<String>,
    pub plan_tier: Option<String>,
    pub plan_key: Option<String>,
    pub source: String,
    pub effective_from_ms: i64,
    pub observed_at_ms: i64,
}

/// A new listing is evidence of a change only when its plan key differs.
/// Repeated reads must not move the effective time of the same tier.
pub fn append_in_tx(tx: &Transaction<'_>, plan: &VaultPlan) -> rusqlite::Result<bool> {
    let previous: Option<Option<String>> = tx.query_row(
        "SELECT plan_key FROM vault_plan_history WHERE credential_id=?1 ORDER BY history_id DESC LIMIT 1",
        [&plan.credential_id], |row| row.get(0),
    ).optional()?;
    if previous.as_ref() == Some(&plan.plan_key) {
        return Ok(false);
    }
    tx.execute(
        "INSERT INTO vault_plan_history(credential_id,provider,account_id,plan_tier,plan_key,source,effective_from_ms,observed_at_ms)
         VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
        params![plan.credential_id,plan.provider,plan.account_id,plan.plan_tier,plan.plan_key,plan.source,plan.effective_from_ms,plan.observed_at_ms],
    )?;
    Ok(true)
}

/// Latest effective row for each record, with a deterministic tie-break for
/// observations whose vault timestamp has millisecond resolution.
pub fn at(conn: &Connection, end_ms: i64) -> rusqlite::Result<Vec<VaultPlan>> {
    conn.prepare(
        "SELECT credential_id,provider,account_id,plan_tier,plan_key,source,effective_from_ms,observed_at_ms
         FROM vault_plan_history h WHERE effective_from_ms<?1 AND NOT EXISTS (
           SELECT 1 FROM vault_plan_history n WHERE n.credential_id=h.credential_id
            AND n.effective_from_ms<?1 AND (n.effective_from_ms>h.effective_from_ms
             OR (n.effective_from_ms=h.effective_from_ms AND n.history_id>h.history_id)))
         ORDER BY effective_from_ms,history_id",
    )?.query_map([end_ms], |row| Ok(VaultPlan {
        credential_id: row.get(0)?, provider: row.get(1)?, account_id: row.get(2)?,
        plan_tier: row.get(3)?, plan_key: row.get(4)?, source: row.get(5)?,
        effective_from_ms: row.get(6)?, observed_at_ms: row.get(7)?,
    }))?.collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vault_plan_history_appends_only_changes_and_cannot_be_replaced() {
        let mut conn = crate::test_support::conn();
        let mut plan = VaultPlan {
            credential_id: "cred".into(),
            provider: Some("anthropic".into()),
            account_id: Some("account".into()),
            plan_tier: Some("max_20x".into()),
            plan_key: Some("anthropic/max_20x".into()),
            source: "detected".into(),
            effective_from_ms: 100,
            observed_at_ms: 200,
        };
        let tx = conn.transaction().unwrap();
        assert!(append_in_tx(&tx, &plan).unwrap());
        plan.observed_at_ms = 300;
        assert!(!append_in_tx(&tx, &plan).unwrap());
        plan.plan_key = None;
        plan.effective_from_ms = 400;
        assert!(append_in_tx(&tx, &plan).unwrap());
        tx.commit().unwrap();
        assert_eq!(
            at(&conn, 400).unwrap()[0].plan_key.as_deref(),
            Some("anthropic/max_20x")
        );
        assert_eq!(at(&conn, 401).unwrap()[0].plan_key, None);
        for sql in [
            "UPDATE vault_plan_history SET source='other'",
            "DELETE FROM vault_plan_history",
            "INSERT OR REPLACE INTO vault_plan_history SELECT * FROM vault_plan_history WHERE history_id=1",
        ] { assert!(conn.execute(sql, []).is_err(), "{sql}"); }
        assert_eq!(
            conn.query_row("SELECT count(*) FROM vault_plan_history", [], |row| row
                .get::<_, i64>(0))
                .unwrap(),
            2
        );
    }
}
