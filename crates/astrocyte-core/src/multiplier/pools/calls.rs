use std::collections::{BTreeMap, BTreeSet};

use rusqlite::{types::Value, Connection};

use crate::catalog_client::CatalogClient;
use crate::subscription::plan_lookup_key;
use crate::valuation::EventTokens;

use super::super::{for_events, resolve_rate_inputs, MultiplierError, ResolvedRates};
use super::{capture_pool_readings, PlannedPool, PoolCoverage, PoolLogin, PoolReadingCapture};

#[derive(Debug)]
pub struct PoolDayCapture {
    pub readings: PoolReadingCapture,
    calls: Vec<PoolCall>,
}

#[derive(Debug, PartialEq)]
pub struct PoolCall {
    pub id: i64,
    pub occurred_at_ms: i64,
    /// The account or paying credential used for both plan lookup and gates.
    pub login: PoolLogin,
    pub plan: String,
    pub tokens: EventTokens,
}

/// A pool's calls before its clean-hour exclusions. Calls
/// for other plans or logins, or outside this pool's coverage, are absent.
#[derive(Debug)]
pub struct PoolCallSelection<'a> {
    pub calls: Vec<&'a PoolCall>,
    pub pool_member_models: Option<Vec<String>>,
}

#[derive(Debug, Default)]
pub struct PoolModelIdentities {
    models: BTreeMap<String, BTreeMap<String, PoolModelIdentity>>,
}

#[derive(Debug)]
struct PoolModelIdentity {
    catalog_id: String,
    via_fallback: bool,
}

impl PoolModelIdentities {
    pub(in crate::multiplier) fn record(
        &mut self,
        provider: &str,
        model: &str,
        catalog_provider: &str,
        catalog_model: &str,
        via_fallback: bool,
    ) {
        self.models.entry(provider.into()).or_default().insert(
            model.into(),
            PoolModelIdentity {
                catalog_id: format!("{catalog_provider}/{catalog_model}"),
                via_fallback,
            },
        );
    }

    fn get(&self, tokens: &EventTokens) -> Option<&PoolModelIdentity> {
        self.models
            .get(&tokens.provider)?
            .get(tokens.model.as_deref()?)
    }
}

/// Resolve a call's own catalog identity separately from the rates that price
/// it. Callers using cached rate eras can keep those rates for valuation.
pub fn resolve_pool_models(
    catalog: &CatalogClient,
    models: &BTreeSet<(String, String)>,
    at_ms: i64,
) -> Result<(PoolModelIdentities, ResolvedRates), MultiplierError> {
    let mut identities = PoolModelIdentities::default();
    let rates = resolve_rate_inputs(catalog, models, at_ms, Some(&mut identities))?;
    Ok((identities, rates))
}

/// Read quota counters, their vault plan timeline, and usage from one store
/// snapshot. No transaction is needed once the caller has this capture.
pub fn capture_pool_day(
    tx: &Connection,
    quota_provider: &str,
    interval_start_ms: i64,
    interval_end_ms: i64,
) -> Result<PoolDayCapture, MultiplierError> {
    if tx.is_autocommit() {
        return Err(MultiplierError::InvalidObservation(
            "capturing a provider day requires a read transaction".into(),
        ));
    }
    let readings = capture_pool_readings(tx, quota_provider, interval_start_ms, interval_end_ms)?;
    let logins: BTreeSet<_> = readings
        .streams
        .iter()
        .map(|stream| (stream.login.ledger, stream.login.id.as_str()))
        .collect();
    let mut calls = Vec::new();
    if logins.is_empty() {
        return Ok(PoolDayCapture { readings, calls });
    }
    for_events(
        tx,
        "e.charge_basis='subscription_included'",
        &[
            Value::Integer(interval_start_ms),
            Value::Integer(interval_end_ms),
        ],
        |event, paying_credential| {
            let Some((ledger, login)) =
                plan_lookup_key(event.account_ref.as_deref(), paying_credential.as_deref())
            else {
                return Ok(());
            };
            if !logins.contains(&(ledger, login)) {
                return Ok(());
            }
            if let Some(plan) = readings.plan_at(ledger, login, event.occurred_at_ms) {
                calls.push(PoolCall {
                    id: event.id,
                    occurred_at_ms: event.occurred_at_ms,
                    login: PoolLogin {
                        ledger,
                        id: login.into(),
                    },
                    plan: plan.into(),
                    tokens: event.tokens,
                });
            }
            Ok(())
        },
    )?;
    Ok(PoolDayCapture { readings, calls })
}

impl PoolDayCapture {
    /// Models from calls whose plan and login have at least one pool where
    /// coverage can be measured. Catalog lookups use each call's own provider.
    pub fn models_for_pools(&self, pools: &[PlannedPool<'_>]) -> BTreeSet<(String, String)> {
        self.models_for(pools.iter().filter(|pool| pool.coverage().is_some()))
    }

    /// Only subset pools need to resolve a model's catalog identity.
    pub fn models_for_subsets(&self, pools: &[PlannedPool<'_>]) -> BTreeSet<(String, String)> {
        self.models_for(
            pools
                .iter()
                .filter(|pool| matches!(pool.coverage(), Some(PoolCoverage::Subset(_)))),
        )
    }

    fn models_for<'a>(
        &self,
        pools: impl Iterator<Item = &'a PlannedPool<'a>>,
    ) -> BTreeSet<(String, String)> {
        let members: BTreeSet<_> = pools
            .flat_map(|pool| pool.reading_runs.iter().map(|run| (pool.plan, run.login)))
            .collect();
        self.calls
            .iter()
            .filter(|call| members.contains(&(call.plan.as_str(), &call.login)))
            .filter(|call| !call.tokens.provider.is_empty())
            .filter_map(|call| Some((call.tokens.provider.clone(), call.tokens.model.clone()?)))
            .collect()
    }

    pub fn select_calls<'a>(
        &'a self,
        pool: &PlannedPool<'_>,
        identities: &PoolModelIdentities,
    ) -> Option<PoolCallSelection<'a>> {
        let members: BTreeSet<_> = pool.reading_runs.iter().map(|run| run.login).collect();
        self.select_calls_for_logins(pool, identities, |login| members.contains(login))
    }

    pub(super) fn select_calls_for_logins<'a>(
        &'a self,
        pool: &PlannedPool<'_>,
        identities: &PoolModelIdentities,
        in_denominator: impl Fn(&PoolLogin) -> bool,
    ) -> Option<PoolCallSelection<'a>> {
        let coverage = pool.coverage()?;
        let mut calls = Vec::new();
        let mut fallback_models = BTreeSet::new();
        for call in &self.calls {
            if call.plan != pool.plan || !in_denominator(&call.login) {
                continue;
            }
            if let PoolCoverage::Subset(models) = coverage {
                let provider = call.tokens.provider.as_str();
                let Some(model) = call.tokens.model.as_deref() else {
                    continue;
                };
                // The served list identifies exact members independently of
                // whether the rate lookup found a card or a price reference.
                if !provider.is_empty()
                    && models
                        .iter()
                        .any(|listed| listed.split_once('/') == Some((provider, model)))
                {
                    calls.push(call);
                    continue;
                }
                let Some(identity) = identities.get(&call.tokens) else {
                    continue;
                };
                if !coverage.covers_model(Some(&identity.catalog_id)) {
                    continue;
                }
                if identity.via_fallback {
                    fallback_models.insert(format!("{provider}/{model}"));
                }
            }
            calls.push(call);
        }
        Some(PoolCallSelection {
            calls,
            pool_member_models: pool
                .pool_models
                .map(|_| fallback_models.into_iter().collect()),
        })
    }
}
