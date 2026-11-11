//! Append-only observations of the plan attached to a vault record.

use fusiform_protocol::PlanPricesResponse;
use rusqlite::{params, Connection, OptionalExtension, Transaction};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};

use crate::subscription::{
    self, CloseSubscriptionRequest, SetSubscriptionRequest, SubscriptionPeriod,
};

pub const VAULT_OPERATOR: &str = "vault";

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct Diagnostics {
    pub unknown_plans: Vec<VaultPlan>,
    pub unpriced_plans: Vec<UnpricedPlan>,
    pub hand_vault_differences: Vec<HandDifference>,
    pub chatgpt_account_mismatches: Vec<AccountMismatch>,
    pub vault_record_differences: Vec<RecordDifference>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RecordDifference {
    pub provider: Option<String>,
    pub account_id: String,
    pub records: Vec<VaultPlan>,
    pub winning_plan: Option<String>,
    pub ambiguous: bool,
}

/// Records sharing an account are ordered by their effective observation, not
/// by list order. Equal-time disagreements have no winner.
pub fn account_winners(plans: &[VaultPlan]) -> (Vec<VaultPlan>, Vec<RecordDifference>) {
    let mut groups: BTreeMap<(Option<String>, String), Vec<&VaultPlan>> = BTreeMap::new();
    for plan in plans {
        if let Some(account) = &plan.account_id {
            groups
                .entry((plan.provider.clone(), account.clone()))
                .or_default()
                .push(plan);
        }
    }
    let mut winners = Vec::new();
    let mut differences = Vec::new();
    for ((provider, account_id), mut records) in groups {
        if records.iter().any(|record| record.source != "config") {
            records.retain(|record| record.source != "config");
        }
        records.sort_by_key(|record| (record.effective_from_ms, &record.credential_id));
        let latest = *records.last().unwrap();
        let ambiguous = records.iter().any(|record| {
            record.effective_from_ms == latest.effective_from_ms
                && record.plan_key != latest.plan_key
        });
        if records
            .iter()
            .map(|record| &record.plan_key)
            .collect::<BTreeSet<_>>()
            .len()
            > 1
        {
            differences.push(RecordDifference {
                provider,
                account_id,
                records: records.iter().map(|record| (*record).clone()).collect(),
                winning_plan: (!ambiguous).then(|| latest.plan_key.clone()).flatten(),
                ambiguous,
            });
        }
        if !ambiguous {
            winners.push(latest.clone());
        }
    }
    (winners, differences)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct UnpricedPlan {
    pub plan_key: String,
    pub provider: String,
    pub plan_tier: String,
    pub accounts: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct HandDifference {
    pub provider: String,
    pub account_id: String,
    pub ledger_plan: String,
    pub vault_plan: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AccountMismatch {
    pub credential_id: String,
    pub account_id: String,
    pub account_refs: Vec<String>,
}

/// Build read-only ledger actions after prices are resolved. Only vault-owned
/// plans are eligible to close; `spend.subscription.set` writes a NULL operator,
/// which identifies a hand-recorded plan.
pub fn prepare_reconciliation(
    conn: &Connection,
    plans: &[VaultPlan],
    prices: &PlanPricesResponse,
    now: i64,
) -> Result<Reconciliation, String> {
    let mut diagnostics = Diagnostics::default();
    let mut changes = Vec::new();
    for plan in plans.iter().filter(|plan| plan.source != "config") {
        if plan.plan_key.is_none() {
            diagnostics.unknown_plans.push(plan.clone());
            continue;
        }
        if ledger_price(plan, prices, now).is_none() {
            if let (Some(key), Some(provider), Some(tier)) =
                (&plan.plan_key, &plan.provider, &plan.plan_tier)
            {
                let identity = plan.account_id.as_ref().unwrap_or(&plan.credential_id);
                if let Some(entry) = diagnostics
                    .unpriced_plans
                    .iter_mut()
                    .find(|entry| entry.plan_key == *key)
                {
                    if !entry.accounts.contains(identity) {
                        entry.accounts.push(identity.clone());
                    }
                } else {
                    diagnostics.unpriced_plans.push(UnpricedPlan {
                        plan_key: key.clone(),
                        provider: provider.clone(),
                        plan_tier: tier.clone(),
                        accounts: vec![identity.clone()],
                    });
                }
            }
        }
    }
    let (winners, differences) = account_winners(plans);
    diagnostics.vault_record_differences = differences;
    for plan in &winners {
        if plan.source == "config" {
            continue;
        }
        let Some(provider) = &plan.provider else {
            continue;
        };
        let Some(account) = &plan.account_id else {
            continue;
        };
        let open: Option<(String, i64, Option<String>)> = conn
            .query_row(
                "SELECT plan_name,effective_from_ms,operator FROM account_subscription
             WHERE provider=?1 AND account_ref=?2 AND effective_to_ms IS NULL",
                params![provider, account],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()
            .map_err(|error| error.to_string())?;
        if let Some((ledger, _, operator)) = &open {
            if operator.as_deref() != Some(VAULT_OPERATOR) {
                if plan.plan_key.as_ref() != Some(ledger) {
                    diagnostics.hand_vault_differences.push(HandDifference {
                        provider: provider.clone(),
                        account_id: account.clone(),
                        ledger_plan: ledger.clone(),
                        vault_plan: plan.plan_key.clone(),
                    });
                }
                continue;
            }
        }
        let price = ledger_price(plan, prices, now);
        if open
            .as_ref()
            .is_some_and(|(key, _, _)| plan.plan_key.as_ref() == Some(key))
        {
            continue;
        }
        if open.is_some() {
            changes.push(LedgerChange::Close(CloseSubscriptionRequest {
                provider: provider.clone(),
                account_ref: account.clone(),
                effective_to: plan.effective_from_ms,
            }));
        }
        let Some(price) = price else {
            continue;
        };
        let first_call: Option<i64> = conn
            .query_row(
                "SELECT min(occurred_at_ms) FROM usage_event WHERE provider=?1 AND account_ref=?2",
                params![provider, account],
                |row| row.get(0),
            )
            .map_err(|error| error.to_string())?;
        let Some(first_call) = first_call else {
            continue;
        };
        let has_history: bool = conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM account_subscription WHERE provider=?1 AND account_ref=?2)",
            params![provider,account], |row| row.get(0),
        ).map_err(|error| error.to_string())?;
        let from = if has_history {
            plan.effective_from_ms
        } else {
            first_call
        };
        changes.push(LedgerChange::Set(SetSubscriptionRequest {
            provider: provider.clone(),
            account_ref: account.clone(),
            plan_name: plan.plan_key.clone().unwrap(),
            recurring_cost_minor: price.minor_units,
            currency: price.currency.into(),
            amount_exponent: price.exponent,
            period: price.period,
            effective_from: from,
        }));
    }
    Ok(Reconciliation {
        diagnostics,
        changes,
    })
}

const CHATGPT_MISMATCH_SQL: &str = "
    SELECT DISTINCT account_ref FROM usage_event WHERE provider='openai' AND charge_basis='subscription_included'
     AND auth_selection=?1 AND account_ref IS NOT NULL AND account_ref<>?2 ORDER BY account_ref";

/// Read mismatches for the vault's credentials. The caller caches the results
/// between snapshots and the daily check.
pub fn chatgpt_account_mismatches(
    conn: &Connection,
    plans: &[VaultPlan],
) -> Result<Vec<AccountMismatch>, String> {
    let mut mismatches = Vec::new();
    for plan in plans
        .iter()
        .filter(|plan| plan.provider.as_deref() == Some("openai"))
    {
        let Some(account) = &plan.account_id else {
            continue;
        };
        let refs = conn
            .prepare(CHATGPT_MISMATCH_SQL)
            .map_err(|error| error.to_string())?
            .query_map(params![plan.credential_id, account], |row| row.get(0))
            .map_err(|error| error.to_string())?
            .collect::<rusqlite::Result<Vec<String>>>()
            .map_err(|error| error.to_string())?;
        if !refs.is_empty() {
            mismatches.push(AccountMismatch {
                credential_id: plan.credential_id.clone(),
                account_id: account.clone(),
                account_refs: refs,
            });
        }
    }
    Ok(mismatches)
}

pub struct Reconciliation {
    pub diagnostics: Diagnostics,
    changes: Vec<LedgerChange>,
}

enum LedgerChange {
    Close(CloseSubscriptionRequest),
    Set(SetSubscriptionRequest),
}

impl Reconciliation {
    pub fn has_changes(&self) -> bool {
        !self.changes.is_empty()
    }

    /// Apply actions prepared in this write transaction, so intervening
    /// hand-recorded plans cannot be overwritten by an earlier preview.
    pub fn apply_in_tx(self, tx: &Transaction<'_>) -> Result<Diagnostics, String> {
        for change in self.changes {
            match change {
                LedgerChange::Close(request) => {
                    subscription::close_subscription_in_tx(tx, &request)
                        .map_err(|error| error.message())?;
                }
                LedgerChange::Set(request) => {
                    subscription::set_subscription_in_tx(tx, &request, Some(VAULT_OPERATOR))
                        .map_err(|error| error.message())?;
                }
            };
        }
        Ok(self.diagnostics)
    }
}

struct LedgerPrice<'a> {
    minor_units: i64,
    exponent: u32,
    currency: &'a str,
    period: SubscriptionPeriod,
}

fn ledger_price<'a>(
    plan: &VaultPlan,
    prices: &'a PlanPricesResponse,
    now: i64,
) -> Option<LedgerPrice<'a>> {
    let price = plan
        .plan_key
        .as_ref()
        .and_then(|key| {
            prices
                .prices
                .iter()
                .filter(|price| {
                    format!("{}/{}", price.provider_id, price.tier) == *key
                        && price.boundary_at_ms <= now
                })
                .max_by_key(|price| price.boundary_at_ms)
        })
        .filter(|price| price.refusal_reason.is_none())
        .and_then(|price| price.price.as_ref())?;
    if price.minor_units < 0
        || price.currency.len() != 3
        || !price.currency.bytes().all(|byte| byte.is_ascii_uppercase())
    {
        return None;
    }
    let period = match price.period.as_str() {
        "month" => SubscriptionPeriod::Month,
        "year" => SubscriptionPeriod::Year,
        _ => return None,
    };
    // Normalize only exact decimal representations. The ledger accepts scales
    // zero through nine; never round a sourced recurring amount to fit it.
    let (minor_units, exponent) = if price.minor_units == 0 {
        (0, 0)
    } else if price.exponent < 0 {
        let zeros = u32::try_from(price.exponent.checked_neg()?).ok()?;
        (
            price.minor_units.checked_mul(10_i64.checked_pow(zeros)?)?,
            0,
        )
    } else {
        let mut minor = price.minor_units;
        let mut exponent = u32::try_from(price.exponent).ok()?;
        while exponent > 9 {
            if minor % 10 != 0 {
                return None;
            }
            minor /= 10;
            exponent -= 1;
        }
        (minor, exponent)
    };
    subscription::recurring_cost_nanos(minor_units, exponent).ok()?;
    Some(LedgerPrice {
        minor_units,
        exponent,
        currency: &price.currency,
        period,
    })
}

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

/// Seed plan history from an older stored config's per-account plans. Only
/// accounts with a null `price_override` are accepted: history carries no
/// price override, so a multiplier day once computed with a non-null override
/// would recompute to a different value. Such a config is refused instead.
pub fn seed_config_in_tx(
    tx: &Transaction<'_>,
    value: &serde_json::Value,
    now: i64,
) -> Result<(), String> {
    let seeded: bool = tx
        .query_row("SELECT EXISTS(SELECT 1 FROM vault_plan_seed)", [], |row| {
            row.get(0)
        })
        .map_err(|error| error.to_string())?;
    if seeded {
        return Ok(());
    }
    if let Some(providers) = value
        .get("quota_providers")
        .and_then(serde_json::Value::as_object)
    {
        for provider in providers.values() {
            for field in ["accounts", "credentials"] {
                if let Some(members) = provider.get(field).and_then(serde_json::Value::as_object) {
                    for (identity, plan) in members {
                        if plan
                            .get("price_override")
                            .is_some_and(|value| !value.is_null())
                        {
                            return Err(
                                "stored non-null per-account price_override cannot be seeded"
                                    .into(),
                            );
                        }
                        let key = plan
                            .get("plan")
                            .and_then(serde_json::Value::as_str)
                            .ok_or("stored plan key is missing")?;
                        let (provider, tier) = key
                            .split_once('/')
                            .filter(|(provider, tier)| !provider.is_empty() && !tier.is_empty())
                            .ok_or("stored plan key is invalid")?;
                        let row = VaultPlan {
                            credential_id: if field == "credentials" {
                                identity.clone()
                            } else {
                                format!("config:account:{identity}")
                            },
                            provider: Some(provider.into()),
                            account_id: (field == "accounts").then(|| identity.clone()),
                            plan_tier: Some(tier.into()),
                            plan_key: Some(key.into()),
                            source: "config".into(),
                            effective_from_ms: 0,
                            observed_at_ms: now,
                        };
                        append_in_tx(tx, &row).map_err(|error| error.to_string())?;
                    }
                }
            }
        }
    }
    tx.execute("INSERT INTO vault_plan_seed(singleton) VALUES(1)", [])
        .map_err(|error| error.to_string())?;
    Ok(())
}

/// A new listing is evidence of a change only when its plan key differs.
/// Repeated reads must not move the effective time of the same tier.
pub fn append_in_tx(tx: &Transaction<'_>, plan: &VaultPlan) -> rusqlite::Result<bool> {
    if !differs_from_latest(tx, plan)? {
        return Ok(false);
    }
    tx.execute(
        "INSERT INTO vault_plan_history(credential_id,provider,account_id,plan_tier,plan_key,source,effective_from_ms,observed_at_ms)
         VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
        params![plan.credential_id,plan.provider,plan.account_id,plan.plan_tier,plan.plan_key,plan.source,plan.effective_from_ms,plan.observed_at_ms],
    )?;
    Ok(true)
}

/// Compare a listing without starting a write transaction for an unchanged key.
pub fn differs_from_latest(conn: &Connection, plan: &VaultPlan) -> rusqlite::Result<bool> {
    let previous: Option<Option<String>> = conn
        .query_row(LATEST_PLAN_SQL, [&plan.credential_id], |row| row.get(0))
        .optional()?;
    Ok(previous.as_ref() != Some(&plan.plan_key))
}

const AT_SQL: &str = "WITH RECURSIVE credential(id) AS (
         SELECT (SELECT credential_id FROM vault_plan_history ORDER BY credential_id LIMIT 1)
         UNION ALL
         SELECT (SELECT n.credential_id FROM vault_plan_history n WHERE n.credential_id>credential.id
                 ORDER BY n.credential_id LIMIT 1)
           FROM credential WHERE credential.id IS NOT NULL)
     SELECT h.credential_id,h.provider,h.account_id,h.plan_tier,h.plan_key,h.source,
            h.effective_from_ms,h.observed_at_ms
       FROM credential CROSS JOIN vault_plan_history h
      WHERE h.history_id=(SELECT l.history_id FROM vault_plan_history l
                           WHERE l.credential_id=credential.id AND l.effective_from_ms<?1
                           ORDER BY l.effective_from_ms DESC, l.history_id DESC LIMIT 1)
      ORDER BY h.effective_from_ms,h.history_id";

/// Newest row of one credential by history ID, read through
/// `vault_plan_credential_latest` so an unchanged listing costs one seek.
const LATEST_PLAN_SQL: &str =
    "SELECT plan_key FROM vault_plan_history WHERE credential_id=?1 ORDER BY history_id DESC LIMIT 1";

/// Latest effective row for each record, with a deterministic tie-break for
/// observations whose vault timestamp has millisecond resolution.
///
/// The recursive step jumps from one credential to the next through a
/// credential-leading index, and each credential's row is one backwards seek
/// on `vault_plan_record_history`, so the work follows the number of credentials rather
/// than re-reading a credential's history once per row it holds. The result
/// keeps the order by effective time and history ID.
pub fn at(conn: &Connection, end_ms: i64) -> rusqlite::Result<Vec<VaultPlan>> {
    conn.prepare(AT_SQL)?
        .query_map([end_ms], |row| {
            Ok(VaultPlan {
                credential_id: row.get(0)?,
                provider: row.get(1)?,
                account_id: row.get(2)?,
                plan_tier: row.get(3)?,
                plan_key: row.get(4)?,
                source: row.get(5)?,
                effective_from_ms: row.get(6)?,
                observed_at_ms: row.get(7)?,
            })
        })?
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::multiplier::vm_steps;

    fn history_row(
        conn: &Connection,
        credential: &str,
        account: Option<&str>,
        plan: Option<&str>,
        effective_from_ms: i64,
    ) {
        conn.execute(
            "INSERT INTO vault_plan_history(credential_id,provider,account_id,plan_tier,plan_key,
                 source,effective_from_ms,observed_at_ms)
             VALUES(?1,'anthropic',?2,'tier',?3,'detected',?4,?4)",
            params![credential, account, plan, effective_from_ms],
        )
        .unwrap();
    }

    /// The snapshot as it was first written: every row before the end that no
    /// later-effective row of its credential replaces.
    fn successor_rule_at(conn: &Connection, end_ms: i64) -> Vec<VaultPlan> {
        conn.prepare(
            "SELECT credential_id,provider,account_id,plan_tier,plan_key,source,effective_from_ms,observed_at_ms
             FROM vault_plan_history h WHERE effective_from_ms<?1 AND NOT EXISTS (
               SELECT 1 FROM vault_plan_history n WHERE n.credential_id=h.credential_id
                AND n.effective_from_ms<?1 AND (n.effective_from_ms>h.effective_from_ms
                 OR (n.effective_from_ms=h.effective_from_ms AND n.history_id>h.history_id)))
             ORDER BY effective_from_ms,history_id",
        )
        .unwrap()
        .query_map([end_ms], |row| {
            Ok(VaultPlan {
                credential_id: row.get(0)?,
                provider: row.get(1)?,
                account_id: row.get(2)?,
                plan_tier: row.get(3)?,
                plan_key: row.get(4)?,
                source: row.get(5)?,
                effective_from_ms: row.get(6)?,
                observed_at_ms: row.get(7)?,
            })
        })
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap()
    }

    fn query_plan(conn: &Connection, sql: &str) -> String {
        let mut statement = conn.prepare(&format!("EXPLAIN QUERY PLAN {sql}")).unwrap();
        let unbound = vec![rusqlite::types::Null; statement.parameter_count()];
        let details: Vec<String> = statement
            .query_map(rusqlite::params_from_iter(unbound), |row| row.get(3))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        details.join("\n")
    }

    #[test]
    fn snapshot_matches_the_successor_rule_at_every_end() {
        let conn = crate::test_support::conn();
        // Equal effective times break by history ID; a later row with an
        // absent plan still replaces an earlier known one.
        for (credential, account, plan, effective) in [
            ("c", Some("acct"), Some("anthropic/max"), 5),
            ("a", Some("acct"), Some("anthropic/pro"), 1),
            ("a", Some("acct"), None, 3),
            ("b", None, Some("anthropic/max"), 3),
            ("b", None, Some("anthropic/pro"), 3),
            ("a", Some("acct"), Some("anthropic/max"), 3),
            ("c", Some("acct"), None, 0),
            ("d", None, None, 9),
            ("b", None, Some("anthropic/team"), 2),
        ] {
            history_row(&conn, credential, account, plan, effective);
        }
        for end in [i64::MIN, -1, 0, 1, 2, 3, 4, 5, 6, 9, 10, i64::MAX] {
            assert_eq!(
                at(&conn, end).unwrap(),
                successor_rule_at(&conn, end),
                "end {end}"
            );
        }
        assert_eq!(at(&conn, 10).unwrap().len(), 4);
    }

    #[test]
    fn snapshot_reads_one_row_per_record_not_its_history() {
        let measure = |versions: i64| {
            let conn = crate::test_support::conn();
            for effective in 0..versions {
                let plan = ["anthropic/pro", "anthropic/max"][usize::from(effective % 2 == 0)];
                history_row(&conn, "busy", Some("acct"), Some(plan), effective);
            }
            history_row(
                &conn,
                "busy",
                Some("acct"),
                Some("anthropic/team"),
                1_000_000,
            );
            for other in ["x", "y", "z"] {
                history_row(&conn, other, None, Some("anthropic/pro"), 5);
                history_row(&conn, other, None, None, 7);
            }
            vm_steps(&conn, || at(&conn, 2_000_000).unwrap())
        };
        let (small, small_steps) = measure(100);
        let (large, large_steps) = measure(1_000);
        assert_eq!(small, large);
        assert_eq!(small.len(), 4);
        assert_eq!(
            large_steps, small_steps,
            "{small_steps} -> {large_steps} VM steps"
        );
        let conn = crate::test_support::conn();
        let plan = query_plan(&conn, AT_SQL);
        for expected in [
            "SEARCH h USING INTEGER PRIMARY KEY (rowid=?)",
            "SEARCH l USING COVERING INDEX vault_plan_record_history (credential_id=? AND effective_from_ms<?)",
        ] {
            assert!(plan.contains(expected), "{expected} missing from:\n{plan}");
        }
        assert!(plan.contains("SEARCH n USING COVERING INDEX"), "{plan}");
        assert!(
            !plan.contains("SCAN h") && !plan.contains("SCAN l"),
            "{plan}"
        );
    }

    #[test]
    fn unchanged_listing_reads_only_the_newest_row() {
        let measure = |versions: i64| {
            let conn = crate::test_support::conn();
            // Effective times run backwards so the newest row by history ID
            // is not the newest by effective time.
            for version in 0..versions {
                let plan = ["anthropic/pro", "anthropic/max"][usize::from(version % 2 == 0)];
                history_row(&conn, "busy", Some("acct"), Some(plan), versions - version);
            }
            history_row(&conn, "busy", Some("acct"), Some("anthropic/team"), 0);
            let listing = VaultPlan {
                credential_id: "busy".into(),
                provider: Some("anthropic".into()),
                account_id: Some("acct".into()),
                plan_tier: Some("tier".into()),
                plan_key: Some("anthropic/team".into()),
                source: "detected".into(),
                effective_from_ms: 0,
                observed_at_ms: 0,
            };
            vm_steps(&conn, || differs_from_latest(&conn, &listing).unwrap())
        };
        let (small, small_steps) = measure(100);
        let (large, large_steps) = measure(1_000);
        assert!(!small && !large, "the newest row by history ID matches");
        assert_eq!(
            large_steps, small_steps,
            "{small_steps} -> {large_steps} VM steps"
        );
        let plan = query_plan(&crate::test_support::conn(), LATEST_PLAN_SQL);
        assert_eq!(
            plan,
            "SEARCH vault_plan_history USING INDEX vault_plan_credential_latest (credential_id=?)"
        );
    }

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
