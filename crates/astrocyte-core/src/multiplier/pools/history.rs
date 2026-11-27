use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound::{Excluded, Included};

use rusqlite::Connection;

use crate::subscription::PlanLedger;
use crate::vault_plan::{self, VaultPlan};

#[derive(Debug)]
pub(in crate::multiplier) struct VaultPlanTimeline {
    states: BTreeMap<i64, LoginPlans>,
}

impl VaultPlanTimeline {
    pub(in crate::multiplier) fn capture(
        tx: &Connection,
        (start, end): (i64, i64),
    ) -> rusqlite::Result<Self> {
        // Vault's snapshot uses an exclusive bound. The plan starting at a
        // reading or call's instant is already effective for that input.
        let boundaries = vault_plan::boundaries_between(tx, start, end.saturating_add(1))?;
        let mut states = BTreeMap::new();
        for at in std::iter::once(start).chain(boundaries) {
            let history = vault_plan::at(tx, at.saturating_add(1))?;
            let plans = LoginPlans::from_history(history);
            if states
                .last_key_value()
                .is_none_or(|(_, previous)| *previous != plans)
            {
                states.insert(at, plans);
            }
        }
        Ok(Self { states })
    }

    pub(in crate::multiplier) fn plan_at(
        &self,
        ledger: PlanLedger,
        login: &str,
        at_ms: i64,
    ) -> Option<&str> {
        self.states
            .range(..=at_ms)
            .next_back()?
            .1
            .get(ledger, login)
    }

    pub(super) fn unchanged(
        &self,
        ledger: PlanLedger,
        login: &str,
        (from, to): (i64, i64),
        plan: &str,
    ) -> bool {
        self.states
            .range((Excluded(from), Included(to)))
            .all(|(_, state)| state.get(ledger, login) == Some(plan))
    }

    /// Look up donor metadata only in periods when the donor held this plan.
    pub(super) fn plan_periods<'a>(
        &'a self,
        ledger: PlanLedger,
        login: &'a str,
        plan: &'a str,
    ) -> impl Iterator<Item = (i64, Option<i64>)> + 'a {
        let mut states = self.states.iter().peekable();
        std::iter::from_fn(move || {
            while let Some((&start, state)) = states.next() {
                if state.get(ledger, login) == Some(plan) {
                    return Some((start, states.peek().map(|(&end, _)| end)));
                }
            }
            None
        })
    }
}

#[derive(Debug, PartialEq, Eq)]
struct LoginPlans {
    accounts: BTreeMap<String, String>,
    credentials: BTreeMap<String, String>,
}

impl LoginPlans {
    fn from_history(mut history: Vec<VaultPlan>) -> Self {
        let credentials = history
            .iter()
            .filter(|row| row.account_id.is_none())
            .filter_map(|row| Some((row.credential_id.clone(), row.plan_key.clone()?)))
            .collect();
        // Real vault facts replace a seeded plan even when the vault puts the
        // account under a different provider. Keep ambiguous accounts out.
        let stated: BTreeSet<_> = history
            .iter()
            .filter(|row| {
                row.source != "config" && (row.provider.is_some() || row.plan_key.is_some())
            })
            .filter_map(|row| row.account_id.clone())
            .collect();
        history.retain(|row| {
            row.account_id.as_ref().is_some_and(|id| {
                (row.source != "config" || !stated.contains(id))
                    && (row.provider.is_some() || row.plan_key.is_some())
            })
        });
        let mut providers = BTreeMap::<&str, BTreeSet<&str>>::new();
        for row in &history {
            if let (Some(id), Some(provider)) = (&row.account_id, &row.provider) {
                providers.entry(id).or_default().insert(provider);
            }
        }
        let (winners, differences) = vault_plan::account_winners(&history);
        let ambiguous: BTreeSet<_> = differences
            .iter()
            .filter(|row| row.ambiguous)
            .map(|row| row.account_id.as_str())
            .collect();
        let accounts = winners
            .into_iter()
            .filter_map(|row| {
                let id = row.account_id?;
                let multiple = providers
                    .get(id.as_str())
                    .is_some_and(|keys| keys.len() > 1);
                (!multiple && !ambiguous.contains(id.as_str())).then_some((id, row.plan_key?))
            })
            .collect();
        Self {
            accounts,
            credentials,
        }
    }

    fn get(&self, ledger: PlanLedger, login: &str) -> Option<&str> {
        match ledger {
            PlanLedger::Account => &self.accounts,
            PlanLedger::Credential => &self.credentials,
        }
        .get(login)
        .map(String::as_str)
    }
}

#[cfg(test)]
#[path = "history_tests.rs"]
mod tests;
