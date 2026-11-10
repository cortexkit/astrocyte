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
    let previous: Option<Option<String>> = conn.query_row(
        "SELECT plan_key FROM vault_plan_history WHERE credential_id=?1 ORDER BY history_id DESC LIMIT 1",
        [&plan.credential_id], |row| row.get(0),
    ).optional()?;
    Ok(previous.as_ref() != Some(&plan.plan_key))
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
