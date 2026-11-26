//! Served rate timelines for valuation, separate from immutable ledger pricing.

use std::collections::{BTreeMap, BTreeSet};

use fusiform_protocol::{HistoryResponse, HistorySegment, HistoryState};
use rusqlite::{params, Connection, OptionalExtension};
use serde_json::Value;

use crate::catalog_client::{CatalogClient, CatalogLookup, CatalogModel, CatalogSnapshot};
use crate::multiplier::{self, ModelResolution, MultiplierError, RateState, ResolvedRates};

type Model = (String, String);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedEra {
    pub from_ms: i64,
    pub until_ms: Option<i64>,
    pub resolution: ModelResolution,
}

/// Owned network result. Fetch this outside a transaction, then publish atomically.
pub struct FetchedModel {
    pub provider: String,
    pub model: String,
    pub reference: Option<CatalogModel>,
    pub segments: BTreeMap<String, Vec<HistorySegment>>,
}

fn catalog_error(error: impl std::fmt::Debug) -> MultiplierError {
    MultiplierError::Catalog(format!("rate cache: {error:?}"))
}

/// Discover only newly ingested calls. The cursor bounds both startup and steady-state work.
pub fn discover(conn: &Connection, limit: i64) -> Result<(), MultiplierError> {
    let after: i64 = conn.query_row(
        "SELECT after_event_key FROM rate_cache_state WHERE id=1",
        [],
        |row| row.get(0),
    )?;
    let mut stmt = conn.prepare(
        "SELECT event_key,provider,model FROM usage_event WHERE event_key>?1 ORDER BY event_key LIMIT ?2",
    )?;
    let rows = stmt.query_map(params![after, limit], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, Option<String>>(1)?,
            row.get::<_, Option<String>>(2)?,
        ))
    })?;
    let mut last = after;
    for row in rows {
        let (key, provider, model) = row?;
        last = key;
        if let (Some(provider), Some(model)) = (provider, model) {
            conn.execute(
                "INSERT OR IGNORE INTO rate_model(provider,model) VALUES(?1,?2)",
                params![provider, model],
            )?;
        }
    }
    conn.execute(
        "UPDATE rate_cache_state SET after_event_key=?1 WHERE id=1",
        [last],
    )?;
    Ok(())
}

/// A catalog version change queues every known model, including targets no longer used directly.
pub fn prepare(conn: &Connection, version: i64) -> Result<(), MultiplierError> {
    let (previous, legacy_references): (i64, bool) = conn.query_row(
        "SELECT catalog_version,twins<>'{}' FROM rate_cache_state WHERE id=1",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    // A restored catalog can reuse earlier versions for different timelines.
    if version < previous {
        conn.execute("UPDATE rate_model SET catalog_version=NULL", [])?;
    }
    if legacy_references {
        // Cached references may come from a removed setting rather than the catalog.
        conn.execute(
            "UPDATE rate_model SET catalog_version=NULL WHERE reference_provider IS NOT NULL",
            [],
        )?;
    }
    conn.execute(
        "UPDATE rate_cache_state SET catalog_version=?1,twins='{}' WHERE id=1",
        [version],
    )?;
    Ok(())
}

pub fn pending(conn: &Connection, limit: i64) -> Result<Vec<Model>, MultiplierError> {
    let mut stmt = conn.prepare_cached(
        "SELECT provider,model FROM (
             SELECT provider,model,last_attempt,catalog_version FROM rate_model
             WHERE catalog_version IS NULL
             UNION ALL
             SELECT provider,model,last_attempt,catalog_version FROM rate_model
             WHERE catalog_version < (SELECT catalog_version FROM rate_cache_state WHERE id=1)
         ) ORDER BY last_attempt,catalog_version,provider,model LIMIT ?1",
    )?;
    let rows = stmt.query_map([limit], |row| Ok((row.get(0)?, row.get(1)?)))?;
    rows.collect::<Result<_, _>>().map_err(Into::into)
}

/// Current identities are deliberately not dated: aliases and API-key quota proxies
/// use today's target, but that target's rates at the call's own instant.
pub fn fetch(
    catalog: &CatalogClient,
    provider: &str,
    model: &str,
) -> Result<FetchedModel, MultiplierError> {
    let current = catalog.get_current(provider, model);
    let mut reference = match &current {
        Ok(CatalogLookup::Resolved(snapshot)) => snapshot.alias_target.clone(),
        _ => None,
    };
    if multiplier::lookup_missing(&current) && reference.is_none() {
        if let Some(base) = model
            .strip_suffix("-low")
            .or_else(|| model.strip_suffix("-high"))
        {
            reference = Some(CatalogModel {
                provider_id: provider.into(),
                model_id: base.into(),
            });
        }
    } else if multiplier::lookup_without_prices(&current) && reference.is_none() {
        match catalog.get_current_apikey(provider, model) {
            Ok(CatalogLookup::Resolved(snapshot)) => {
                reference = multiplier::quota_proxy_target(&snapshot)
            }
            Ok(_) => {}
            Err(crate::catalog_client::CatalogClientError::Call(error))
                if error.code == "no_coverage" => {}
            Err(error) => return Err(catalog_error(error)),
        }
    }
    if let Err(error) = current {
        if !matches!(&error, crate::catalog_client::CatalogClientError::Call(error) if error.code == "no_coverage")
        {
            return Err(catalog_error(error));
        }
    }
    let history = match catalog.history(provider, model) {
        Ok(history) => history,
        Err(crate::catalog_client::CatalogClientError::Call(error))
            if error.code == "no_coverage" =>
        {
            return Ok(FetchedModel {
                provider: provider.into(),
                model: model.into(),
                reference,
                segments: BTreeMap::new(),
            });
        }
        Err(error) => return Err(catalog_error(error)),
    };
    if reference.is_none() {
        if let Some(origin) = &history.inherited_from {
            if matches!(origin.basis.as_str(), "alias" | "quota_proxy") {
                reference = origin.model_id.as_ref().map(|model| CatalogModel {
                    provider_id: origin.provider_id.clone(),
                    model_id: model.clone(),
                });
            }
        }
    }
    fetched(history, provider, model, reference)
}

pub fn fetched(
    history: HistoryResponse,
    provider: &str,
    model: &str,
    reference: Option<CatalogModel>,
) -> Result<FetchedModel, MultiplierError> {
    if history.provider_id != provider
        || history.model_id != model
        || history.fact_prefix.as_deref() != Some("rate.")
    {
        return Err(catalog_error("history reply identity or prefix mismatch"));
    }
    let segments = history
        .served_by_key
        .ok_or_else(|| catalog_error("history reply omitted served_by_key"))?;
    for (key, eras) in &segments {
        crate::catalog_client::parse_rate_key(key)
            .map_err(|error| MultiplierError::RateFactUnrecognized(format!("{key}: {error:?}")))?;
        let mut previous_until = None;
        for (index, era) in eras.iter().enumerate() {
            if era.until_ms.is_some_and(|until| until <= era.from_ms)
                || (index > 0 && previous_until.is_none_or(|until| era.from_ms < until))
                || (era.state == HistoryState::Value && era.value.is_none())
            {
                return Err(catalog_error("invalid or overlapping served intervals"));
            }
            previous_until = era.until_ms;
        }
    }
    Ok(FetchedModel {
        provider: provider.into(),
        model: model.into(),
        reference,
        segments,
    })
}

fn stored_segments(
    conn: &Connection,
    provider: &str,
    model: &str,
) -> Result<BTreeMap<String, Vec<HistorySegment>>, MultiplierError> {
    let mut stmt = conn.prepare("SELECT fact_key,segment FROM rate_era WHERE provider=?1 AND model=?2 ORDER BY fact_key,from_ms")?;
    let rows = stmt.query_map(params![provider, model], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?;
    let mut segments: BTreeMap<String, Vec<HistorySegment>> = BTreeMap::new();
    for row in rows {
        let (key, segment) = row?;
        segments
            .entry(key)
            .or_default()
            .push(serde_json::from_str(&segment).map_err(catalog_error)?);
    }
    Ok(segments)
}

/// Mark only summary-backed hours holding this model or a model that refers to it.
/// Dirty summaries are already rebuilt from their authoritative ledger rows.
pub fn mark_dirty(
    conn: &Connection,
    provider: &str,
    model: &str,
) -> Result<usize, MultiplierError> {
    // A Linux debug measurement over 160,890 cells queued the busiest model's
    // 16,089 hours in 50.6 ms, including queue inserts. Rate changes are rare;
    // an extra summary index would cost every rebuild and enlarge backups.
    conn.execute(
        "WITH RECURSIVE referring(provider,model) AS (
             SELECT ?1,?2 UNION
             SELECT r.provider,r.model FROM rate_model r JOIN referring t
             ON r.reference_provider=t.provider AND r.reference_model=t.model
         ) INSERT OR IGNORE INTO rate_dirty_hour(hour_number)
         SELECT DISTINCT c.hour_number FROM referring r
         JOIN summary_value p ON p.value=r.provider
         JOIN summary_value m ON m.value=r.model
         JOIN summary_cell c ON c.provider=p.value_id AND c.model=m.value_id
         WHERE c.kind=0",
        params![provider, model],
    )
    .map_err(Into::into)
}

/// Publish under the writer's transaction, after every network call has finished.
pub fn publish(
    conn: &Connection,
    fetched: &FetchedModel,
    version: i64,
) -> Result<bool, MultiplierError> {
    let old: Option<(Option<i64>, Option<String>, Option<String>)> = conn.query_row(
        "SELECT catalog_version,reference_provider,reference_model FROM rate_model WHERE provider=?1 AND model=?2",
        params![fetched.provider, fetched.model], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    ).optional()?;
    let reference_provider = fetched
        .reference
        .as_ref()
        .map(|target| target.provider_id.as_str());
    let reference_model = fetched
        .reference
        .as_ref()
        .map(|target| target.model_id.as_str());
    let changed = old.as_ref().is_none_or(|(version, provider, model)| {
        version.is_none()
            || provider.as_deref() != reference_provider
            || model.as_deref() != reference_model
    }) || stored_segments(conn, &fetched.provider, &fetched.model)?
        != fetched.segments;
    conn.execute(
        "INSERT INTO rate_model(provider,model,catalog_version,reference_provider,reference_model) VALUES(?1,?2,?3,?4,?5)
         ON CONFLICT(provider,model) DO UPDATE SET catalog_version=excluded.catalog_version,
         reference_provider=excluded.reference_provider,reference_model=excluded.reference_model,last_error=NULL",
        params![fetched.provider, fetched.model, version, reference_provider, reference_model],
    )?;
    if let Some(reference) = &fetched.reference {
        conn.execute(
            "INSERT OR IGNORE INTO rate_model(provider,model) VALUES(?1,?2)",
            params![reference.provider_id, reference.model_id],
        )?;
    }
    if changed {
        conn.execute(
            "DELETE FROM rate_era WHERE provider=?1 AND model=?2",
            params![fetched.provider, fetched.model],
        )?;
        let mut insert = conn.prepare("INSERT INTO rate_era(provider,model,fact_key,from_ms,until_ms,segment) VALUES(?1,?2,?3,?4,?5,?6)")?;
        for (key, eras) in &fetched.segments {
            for era in eras {
                insert.execute(params![
                    fetched.provider,
                    fetched.model,
                    key,
                    era.from_ms,
                    era.until_ms,
                    serde_json::to_string(era).map_err(catalog_error)?
                ])?;
            }
        }
        mark_dirty(conn, &fetched.provider, &fetched.model)?;
        conn.execute(
            "UPDATE rate_cache_state SET generation=generation+1 WHERE id=1",
            [],
        )?;
    }
    Ok(changed)
}

pub fn record_failure(
    conn: &Connection,
    provider: &str,
    model: &str,
    error: &MultiplierError,
) -> Result<(), MultiplierError> {
    let diagnostic = match error {
        MultiplierError::RateFactUnrecognized(message) => Some(message.as_str()),
        _ => None,
    };
    // Keep the last fetched timeline usable during retries. A model selected by
    // pending() already has either no version or an older version than the cache.
    // Move it later in the queue so a short tick cannot starve new models.
    conn.execute(
        "UPDATE rate_model SET last_error=?3,
         last_attempt=(SELECT COALESCE(MAX(last_attempt),0)+1 FROM rate_model)
         WHERE provider=?1 AND model=?2",
        params![provider, model, diagnostic],
    )?;
    Ok(())
}

fn snapshot_at(segments: &BTreeMap<String, Vec<HistorySegment>>, at_ms: i64) -> CatalogSnapshot {
    let mut snapshot = CatalogSnapshot {
        source: "rate_era".into(),
        resolved_at_ms: at_ms,
        model_key: String::new(),
        facts: BTreeMap::new(),
        withheld_facts: BTreeMap::new(),
        uncertain_facts: BTreeMap::new(),
        fact_overrides: BTreeMap::new(),
        alias_target: None,
    };
    for (key, eras) in segments {
        if let Some(era) = eras
            .iter()
            .find(|era| era.from_ms <= at_ms && era.until_ms.is_none_or(|until| at_ms < until))
        {
            match era.state {
                HistoryState::Value => {
                    snapshot
                        .facts
                        .insert(key.clone(), era.value.clone().unwrap_or(Value::Null));
                }
                HistoryState::Refused => {
                    snapshot.withheld_facts.insert(key.clone(), Value::Null);
                }
                HistoryState::Absent => {}
            }
        }
    }
    snapshot
}

fn resolution(snapshot: &CatalogSnapshot) -> ModelResolution {
    let mut resolution = multiplier::rates_from_snapshot(snapshot);
    match &mut resolution {
        ModelResolution::Unpriceable { reason } if reason == "no_catalog_coverage" => {
            *reason = "no_rate".into()
        }
        ModelResolution::Rates(rates) => {
            for state in rates
                .base
                .values_mut()
                .chain(
                    rates
                        .tiers
                        .values_mut()
                        .flat_map(|rates| rates.values_mut()),
                )
                .chain(
                    rates
                        .modes
                        .values_mut()
                        .flat_map(|rates| rates.values_mut()),
                )
                .chain(rates.conditional.values_mut())
            {
                if let RateState::Unpriced { reason } = state {
                    if reason == "rate_withheld" {
                        *reason = "refused".into();
                    }
                }
            }
        }
        _ => {}
    }
    resolution
}

/// `None` means not yet fetched, not an absence of rates. Consumers must leave
/// that hour pending rather than recording a permanent `no_rate` verdict.
pub fn resolve(
    conn: &Connection,
    provider: &str,
    model: &str,
    at_ms: i64,
) -> Result<Option<ModelResolution>, MultiplierError> {
    Ok(ModelTimeline::load(conn, provider, model)?.map(|timeline| timeline.at(at_ms).clone()))
}

/// A model's resolved timeline frozen for one read snapshot. Calls select their
/// own era in memory after the reader loads each distinct model once.
pub(crate) struct ModelTimeline(Vec<(ResolvedEra, CatalogSnapshot)>);

impl ModelTimeline {
    pub(crate) fn load(
        conn: &Connection,
        provider: &str,
        model: &str,
    ) -> Result<Option<Self>, MultiplierError> {
        Ok(
            resolved_model(conn, provider, model)?.map(|(segments, reference)| {
                let mut boundaries = BTreeSet::from([i64::MIN]);
                for era in segments.values().flatten() {
                    boundaries.insert(era.from_ms);
                    boundaries.extend(era.until_ms);
                }
                let boundaries: Vec<_> = boundaries.into_iter().collect();
                Self(
                    boundaries
                        .iter()
                        .enumerate()
                        .map(|(index, from)| {
                            let mut snapshot = snapshot_at(&segments, *from);
                            let mut resolved = resolution(&snapshot);
                            if let ModelResolution::Rates(rates) = &mut resolved {
                                rates.reference_model = reference.clone();
                            }
                            let is_unit = |key: &String, _: &mut Value| {
                                crate::catalog_client::parse_rate_key(key)
                                    .is_ok_and(|parsed| parsed.unit.is_some())
                            };
                            snapshot.facts.retain(is_unit);
                            snapshot.withheld_facts.retain(is_unit);
                            (
                                ResolvedEra {
                                    from_ms: *from,
                                    until_ms: boundaries.get(index + 1).copied(),
                                    resolution: resolved,
                                },
                                snapshot,
                            )
                        })
                        .collect(),
                )
            }),
        )
    }

    pub(crate) fn at(&self, at_ms: i64) -> &ModelResolution {
        &self.entry_at(at_ms).0.resolution
    }

    pub(crate) fn unit_rates_at(&self, at_ms: i64) -> &CatalogSnapshot {
        &self.entry_at(at_ms).1
    }

    fn entry_at(&self, at_ms: i64) -> &(ResolvedEra, CatalogSnapshot) {
        // Every loaded timeline starts at i64::MIN, even without rate segments.
        &self.0[self.0.partition_point(|(era, _)| era.from_ms <= at_ms) - 1]
    }
}

type ResolvedModel = (BTreeMap<String, Vec<HistorySegment>>, Option<CatalogModel>);

fn resolved_model(
    conn: &Connection,
    provider: &str,
    model: &str,
) -> Result<Option<ResolvedModel>, MultiplierError> {
    let mut current = (provider.to_string(), model.to_string());
    let mut visited = BTreeSet::new();
    loop {
        if !visited.insert(current.clone()) {
            return Err(catalog_error("cyclic model reference"));
        }
        let row: Option<(Option<i64>, Option<String>, Option<String>)> = conn.query_row(
            "SELECT CASE WHEN reference_provider IS NULL OR (SELECT twins='{}' FROM rate_cache_state WHERE id=1)
                         THEN catalog_version END,reference_provider,reference_model
             FROM rate_model WHERE provider=?1 AND model=?2",
            params![current.0, current.1], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        ).optional()?;
        match row {
            Some((Some(_), Some(provider), Some(model))) => current = (provider, model),
            Some((Some(_), None, None)) => {
                let reference =
                    (current != (provider.into(), model.into())).then(|| CatalogModel {
                        provider_id: current.0.clone(),
                        model_id: current.1.clone(),
                    });
                return Ok(Some((
                    stored_segments(conn, &current.0, &current.1)?,
                    reference,
                )));
            }
            _ => return Ok(None),
        }
    }
}

/// Freeze all model timelines for one observation. No catalog I/O or per-call SQL.
pub fn resolved_rates(
    conn: &Connection,
    models: &BTreeSet<Model>,
    at_ms: i64,
) -> Result<ResolvedRates, MultiplierError> {
    let mut rates = ResolvedRates {
        resolved_at_ms: at_ms,
        ..ResolvedRates::default()
    };
    for (provider, model) in models {
        let Some((segments, reference)) = resolved_model(conn, provider, model)? else {
            let diagnostic: Option<String> = conn
                .query_row(
                    "SELECT last_error FROM rate_model WHERE provider=?1 AND model=?2",
                    params![provider, model],
                    |row| row.get(0),
                )
                .optional()?
                .flatten();
            if let Some(message) = diagnostic {
                return Err(MultiplierError::RateFactUnrecognized(message));
            }
            return Err(catalog_error(format!("not_yet_valued: {provider}/{model}")));
        };
        let mut boundaries = BTreeSet::from([i64::MIN]);
        for era in segments.values().flatten() {
            boundaries.insert(era.from_ms);
            boundaries.extend(era.until_ms);
        }
        let boundaries: Vec<_> = boundaries.into_iter().collect();
        let mut eras = Vec::new();
        for (index, from_ms) in boundaries.iter().enumerate() {
            // Routing and the desktop consume the multiplier's stored diagnostic.
            // Preserve its missing-model wording; list_value_event normalizes it to no_rate.
            let mut resolution = if segments.is_empty() {
                ModelResolution::Unpriceable {
                    reason: "no_catalog_coverage".into(),
                }
            } else {
                resolution(&snapshot_at(&segments, *from_ms))
            };
            if let ModelResolution::Rates(model_rates) = &mut resolution {
                model_rates.reference_model = reference.clone();
            }
            if let ModelResolution::Unpriceable { reason } = &resolution {
                if matches!(
                    reason.as_str(),
                    "rate_undecodable" | "rate_fact_unrecognized"
                ) {
                    return Err(MultiplierError::RateFactUnrecognized(reason.clone()));
                }
            }
            eras.push(ResolvedEra {
                from_ms: *from_ms,
                until_ms: boundaries.get(index + 1).copied(),
                resolution,
            });
        }
        let current = eras
            .iter()
            .find(|era| era.from_ms <= at_ms && era.until_ms.is_none_or(|until| at_ms < until))
            .expect("timeline starts at i64::MIN")
            .resolution
            .clone();
        rates
            .models
            .insert((provider.clone(), model.clone()), current);
        rates.eras.insert((provider.clone(), model.clone()), eras);
    }
    Ok(rates)
}

#[cfg(test)]
mod tests;
