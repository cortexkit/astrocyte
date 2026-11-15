//! Independent list-value partitions. Recorded spend uses the summary reducer unchanged.
use super::*;
use crate::multiplier::{MultiplierConfig, ScopeRule};
use crate::subscription::plan_lookup_key;
use crate::valuation::{list_value_event, EventTokens, Unvalued, ValuationRules};

pub mod fees;

/// The immutable primary key of a record in either subscription ledger.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct PlanRecordId {
    pub ledger: crate::subscription::PlanLedger,
    pub provider: String,
    pub identity: String,
    pub effective_from_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct PlanMember {
    pub plan: String,
    pub plan_account: Option<String>,
    pub record: Option<PlanRecordId>,
}

impl PlanMember {
    fn non_pool(plan: &str) -> Self {
        Self {
            plan: plan.into(),
            plan_account: None,
            record: None,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Recorded {
    cell: Cell,
    quantities: Vec<(QuantityKey, Quantity)>,
    money: Vec<((i64, String), Money)>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct ValuedCell {
    recorded: Recorded,
    numerator: i128,
    unvalued: Unvalued,
}

type Cells = BTreeMap<(Key, PlanMember), ValuedCell>;

fn rule_matches(rule: &ScopeRule, event: &EventTokens, basis: &str) -> bool {
    rule.producer == event.producer
        && rule.provider == event.provider
        && rule
            .charge_basis
            .as_deref()
            .is_none_or(|value| value == basis)
        && rule.model_prefix.as_deref().is_none_or(|prefix| {
            event
                .model
                .as_deref()
                .is_some_and(|model| model.starts_with(prefix))
        })
}

/// Membership deliberately matches ledger coverage: a present account selects
/// account plans, and only a NULL account selects the exact paying credential.
/// An empty account selects neither ledger; it is not an absent account.
#[allow(clippy::too_many_arguments)]
pub fn plan_member(
    conn: &Connection,
    config: &MultiplierConfig,
    event: &EventTokens,
    basis: &str,
    account: Option<&str>,
    paying_credential: Option<&str>,
    at_ms: Option<i64>,
) -> Result<PlanMember, ReportError> {
    if basis == "metered_api" {
        return Ok(PlanMember::non_pool("api"));
    }
    let mut records = BTreeSet::new();
    if let (Some((ledger, identity)), Some(at)) =
        (plan_lookup_key(account, paying_credential), at_ms)
    {
        let mut stmt = conn.prepare(&format!("SELECT provider,effective_from_ms FROM {} WHERE {}=?1 AND effective_from_ms<=?2 AND (effective_to_ms IS NULL OR effective_to_ms>?2) AND provider=?3", ledger.table(), ledger.key_column()))?;
        for provider in stmt.query_map(params![identity, at, event.provider], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })? {
            let (provider, effective_from_ms) = provider?;
            for pool in pools_for_provider(&provider, config) {
                records.insert(PlanMember {
                    plan: pool,
                    plan_account: Some(identity.into()),
                    record: Some(PlanRecordId {
                        ledger,
                        provider: provider.clone(),
                        identity: identity.into(),
                        effective_from_ms,
                    }),
                });
            }
        }
    }
    if records.len() > 1 {
        return Ok(PlanMember::non_pool("pool_conflict"));
    }
    if let Some(member) = records.into_iter().next() {
        return Ok(member);
    }
    let pools: Vec<_> = config
        .quota_providers
        .iter()
        .filter(|(_, pool)| {
            pool.scope
                .iter()
                .any(|rule| rule_matches(rule, event, basis))
        })
        .map(|(name, _)| name.clone())
        .collect();
    Ok(match pools.as_slice() {
        [] => PlanMember::non_pool("none"),
        [pool] => PlanMember {
            plan: pool.clone(),
            plan_account: Some("account_unknown".into()),
            record: None,
        },
        _ => PlanMember::non_pool("pool_conflict"),
    })
}

fn pools_for_provider(provider: &str, config: &MultiplierConfig) -> BTreeSet<String> {
    let mut pools: BTreeSet<_> = config
        .quota_providers
        .iter()
        .filter(|(name, pool)| {
            name.as_str() == provider || pool.scope.iter().any(|rule| rule.provider == provider)
        })
        .map(|(name, _)| name.clone())
        .collect();
    // A recorded plan outside configured quota pools still owns its own fee.
    if pools.is_empty() {
        pools.insert(provider.into());
    }
    pools
}

fn merge_unvalued(target: &mut Unvalued, value: &Unvalued) -> Result<(), ReportError> {
    add_count(&mut target.measurements, value.measurements)?;
    target.tokens = target
        .tokens
        .checked_add(value.tokens)
        .ok_or(ReportError::Arithmetic)?;
    for (reason, count) in &value.by_reason {
        add_count(target.by_reason.entry(*reason).or_default(), *count)?;
    }
    Ok(())
}

/// Shared reduction for the chunked builder and bounded dirty-hour report reads.
/// The caller holds a read snapshot covering rates, plans and ledger rows.
pub(crate) fn reduce(
    conn: &Connection,
    filter: &str,
    config: &MultiplierConfig,
    reasoning: &[String],
    cells: &mut Cells,
) -> Result<(usize, bool), ReportError> {
    let ledger = load_scoped_ledger(conn, &DIMENSIONS, Some(filter))?;
    let mut members: BTreeMap<PlanMember, Vec<i64>> = BTreeMap::new();
    let rules = ValuationRules::from_config(config, reasoning);
    let mut fetched = true;
    for event in &ledger.events {
        let key = key(&ledger, event);
        let mut tokens = EventTokens {
            producer: key.0[Grouping::Producer as usize]
                .clone()
                .unwrap_or_default(),
            provider: key.0[Grouping::Provider as usize]
                .clone()
                .unwrap_or_default(),
            model: key.0[Grouping::Model as usize].clone(),
            mode: None,
            roles: Vec::new(),
        };
        let mut stmt = conn.prepare("SELECT role,unit,source,quantity_units,quantity_exponent FROM measurement WHERE event_key=?1 ORDER BY role,unit")?;
        tokens.roles = stmt
            .query_map([event.key], |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            })?
            .collect::<rusqlite::Result<_>>()?;
        let paying_credential: Option<String> = conn.query_row("SELECT COALESCE(resolved_credential,auth_selection) FROM usage_event WHERE event_key=?1",[event.key],|row|row.get(0))?;
        let member = plan_member(
            conn,
            config,
            &tokens,
            key.0[Grouping::ChargeBasis as usize]
                .as_deref()
                .unwrap_or_default(),
            key.0[Grouping::Account as usize].as_deref(),
            paying_credential.as_deref(),
            event.occurred_at_ms,
        )?;
        members.entry(member.clone()).or_default().push(event.key);
        let resolution = match (&tokens.model, event.occurred_at_ms) {
            (Some(model), Some(at)) => {
                match crate::rate_era::resolve(conn, &tokens.provider, model, at) {
                    Ok(resolution) => resolution,
                    Err(_) => Some(crate::multiplier::ModelResolution::Unpriceable {
                        reason: "valuation_error".into(),
                    }),
                }
            }
            _ => None,
        };
        if tokens.model.is_some() && (resolution.is_none() || event.occurred_at_ms.is_none()) {
            fetched = false;
            continue;
        }
        let value = list_value_event(&rules, resolution.as_ref(), &tokens)
            .map_err(|_| ReportError::Arithmetic)?;
        let cell = cells.entry((key, member)).or_default();
        sum(&mut cell.numerator, value.numerator)?;
        merge_unvalued(&mut cell.unvalued, &value.unvalued)?;
    }
    for (member, events) in members {
        let ids = events
            .iter()
            .map(i64::to_string)
            .collect::<Vec<_>>()
            .join(",");
        let mut data = Data::default();
        data.observe(conn, &format!("event_key IN ({ids})"))?;
        for (key, recorded) in data.cells {
            let cell = cells.entry((key, member.clone())).or_default();
            cell.recorded.cell.merge(&recorded)?;
        }
        // Merge children too, so the payload is independent of page boundaries.
        for ((key, quantity_key), quantity) in data.quantities {
            merge_quantity(
                &mut cells
                    .entry((key, member.clone()))
                    .or_default()
                    .recorded
                    .quantities,
                &quantity_key,
                &quantity,
            )?;
        }
        for ((key, kind, currency), money) in data.money {
            merge_money(
                &mut cells
                    .entry((key, member.clone()))
                    .or_default()
                    .recorded
                    .money,
                &(kind, currency),
                &money,
            )?;
        }
    }
    Ok((ledger.events.len(), fetched))
}

fn merge_quantity(
    values: &mut Vec<(QuantityKey, Quantity)>,
    key: &QuantityKey,
    value: &Quantity,
) -> Result<(), ReportError> {
    if let Some((_, target)) = values.iter_mut().find(|(candidate, _)| candidate == key) {
        sum(&mut target.units, value.units)?;
        sum(&mut target.covered_units, value.covered_units)?;
        add_count(&mut target.observations, value.observations)?;
        add_count(&mut target.stated_observations, value.stated_observations)?;
        add_count(&mut target.covered_observations, value.covered_observations)?;
    } else {
        values.push((key.clone(), value.clone()));
        values.sort_by(|a, b| a.0.cmp(&b.0));
    }
    Ok(())
}
fn merge_money(
    values: &mut Vec<((i64, String), Money)>,
    key: &(i64, String),
    value: &Money,
) -> Result<(), ReportError> {
    if let Some((_, target)) = values.iter_mut().find(|(candidate, _)| candidate == key) {
        sum(&mut target.nanos, value.nanos)?;
        add_count(&mut target.contributions, value.contributions)?;
    } else {
        values.push((key.clone(), value.clone()));
        values.sort_by(|a, b| a.0.cmp(&b.0));
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Generations {
    rate: i64,
    config: i64,
    plan: i64,
}
fn generations(conn: &Connection) -> Result<Generations, ReportError> {
    Ok(conn.query_row(
        "SELECT rate_generation,config_generation,plan_generation FROM valuation_state WHERE id=1",
        [],
        |r| {
            Ok(Generations {
                rate: r.get(0)?,
                config: r.get(1)?,
                plan: r.get(2)?,
            })
        },
    )?)
}

struct Pending {
    hour: Hour,
    revision: i64,
    generations: Generations,
    after: Option<i64>,
    cells: Cells,
    fetched: bool,
    ready: bool,
}

/// Independent cursor and queue; callers assign this builder its own time budget.
#[derive(Default)]
pub struct ValuationMaintenance {
    pending: Option<Pending>,
    after: Option<Hour>,
}

impl ValuationMaintenance {
    /// Idle cycles use a reader snapshot and need no writer transaction.
    pub fn needs_prepare(
        conn: &Connection,
        config: &MultiplierConfig,
        reasoning: &[String],
    ) -> Result<bool, ReportError> {
        let encoded = config_key(config, reasoning)?;
        Ok(conn.query_row("SELECT config<>?1 OR rate_generation<>(SELECT generation FROM rate_cache_state WHERE id=1) FROM valuation_state WHERE id=1",[encoded],|r|r.get(0))?)
    }

    /// Synchronize generations and consume indexed rate invalidations atomically.
    pub fn prepare(
        tx: &Transaction<'_>,
        config: &MultiplierConfig,
        reasoning: &[String],
    ) -> Result<(), ReportError> {
        let encoded = config_key(config, reasoning)?;
        let previous: String =
            tx.query_row("SELECT config FROM valuation_state WHERE id=1", [], |r| {
                r.get(0)
            })?;
        if encoded != previous {
            tx.execute("UPDATE valuation_state SET config=?1,config_generation=config_generation+1 WHERE id=1",[encoded])?;
            tx.execute(
                "UPDATE valuation_hour SET state='dirty',revision=revision+1",
                [],
            )?;
        }
        let old = generations(tx)?;
        let rate: i64 = tx.query_row(
            "SELECT generation FROM rate_cache_state WHERE id=1",
            [],
            |r| r.get(0),
        )?;
        if rate != old.rate {
            // An unfetched hour may have been reduced before its first summary
            // publication, so it cannot yet appear in the model-to-summary lookup.
            tx.execute("UPDATE valuation_hour SET state='dirty',revision=revision+1 WHERE state='not_yet_valued'",[])?;
            tx.execute("UPDATE valuation_hour SET state='dirty',revision=revision+1 WHERE kind=0 AND hour_number IN (SELECT hour_number FROM rate_dirty_hour)",[])?;
            tx.execute("UPDATE valuation_hour SET rate_generation=?1 WHERE state='built' AND rate_generation=?2 AND config_generation=?3 AND plan_generation=?4",params![rate,old.rate,old.config,old.plan])?;
            tx.execute(
                "UPDATE valuation_state SET rate_generation=?1 WHERE id=1",
                [rate],
            )?;
            tx.execute("DELETE FROM rate_dirty_hour", [])?;
        }
        Ok(())
    }

    pub fn has_pending(&self) -> bool {
        self.pending.is_some()
    }

    pub fn read_chunk(
        &mut self,
        conn: &Connection,
        config: &MultiplierConfig,
        reasoning: &[String],
    ) -> Result<bool, ReportError> {
        if self.pending.is_none() {
            let read = |after: Option<Hour>| {
                conn.query_row(&format!("SELECT v.kind,v.hour_number FROM valuation_hour v JOIN summary_hour s USING(kind,hour_number) WHERE v.state='dirty' AND s.built_revision=s.revision AND s.format_version={FORMAT} AND (?1 IS NULL OR (v.kind,v.hour_number)>(?1,?2)) ORDER BY v.kind,v.hour_number LIMIT 1"),params![after.map(|h|h.kind),after.map(|h|h.number)],|r|Ok(Hour {kind:r.get(0)?,number:r.get(1)?})).optional()
            };
            let hour = read(self.after)?.or(read(None)?);
            let Some(hour) = hour else {
                return Ok(false);
            };
            self.after = Some(hour);
            self.pending = Some(Pending {
                hour,
                revision: valuation_revision(conn, hour)?,
                generations: generations(conn)?,
                after: None,
                cells: Cells::new(),
                fetched: true,
                ready: false,
            });
        }
        let mut pending = self
            .pending
            .take()
            .ok_or(ReportError::Integrity("valuation build absent"))?;
        if pending.revision != valuation_revision(conn, pending.hour)?
            || pending.generations != generations(conn)?
            || pending.generations.rate != cache_generation(conn)?
            || !summary_current(conn, pending.hour)?
        {
            return Ok(false);
        }
        let last = conn.query_row(&page_end_sql(pending.hour, pending.after), [], |r| {
            r.get::<_, Option<i64>>(0)
        })?;
        let count = if let Some(last) = last {
            let (count, fetched) = reduce(
                conn,
                &chunk_filter(pending.hour, pending.after, last),
                config,
                reasoning,
                &mut pending.cells,
            )?;
            pending.fetched &= fetched;
            count
        } else {
            0
        };
        pending.after = last;
        pending.ready = count < CHUNK;
        // Bound publication just as summaries do. An oversized hour remains
        // pending rather than monopolizing the writer with an unbounded commit.
        if pending
            .cells
            .values()
            .map(|cell| 1 + cell.recorded.quantities.len() + cell.recorded.money.len())
            .sum::<usize>()
            > MAX_PUBLICATION_ROWS
        {
            return Ok(false);
        }
        let ready = pending.ready;
        self.pending = Some(pending);
        Ok(ready)
    }

    pub fn publish(&mut self, tx: &Transaction<'_>) -> Result<bool, ReportError> {
        let Some(pending) = self.pending.take() else {
            return Ok(false);
        };
        if !pending.ready
            || pending.revision != valuation_revision(tx, pending.hour)?
            || pending.generations != generations(tx)?
            || pending.generations.rate != cache_generation(tx)?
            || !summary_current(tx, pending.hour)?
        {
            return Ok(false);
        }
        let h = pending.hour;
        tx.execute(
            "DELETE FROM valuation_cell WHERE kind=?1 AND hour_number=?2",
            params![h.kind, h.number],
        )?;
        if pending.fetched {
            for ((key, member), cell) in pending.cells {
                tx.execute(
                    "INSERT INTO valuation_cell VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
                    params![
                        h.kind,
                        h.number,
                        encode(&(&key, &member))?,
                        encode(&key)?,
                        member.plan,
                        member.plan_account,
                        member.record.as_ref().map(encode).transpose()?,
                        encode(&cell.recorded)?,
                        cell.numerator.to_string(),
                        encode(&cell.unvalued)?
                    ],
                )?;
            }
        }
        tx.execute("UPDATE valuation_hour SET rate_generation=?3,config_generation=?4,plan_generation=?5,state=?6 WHERE kind=?1 AND hour_number=?2",params![h.kind,h.number,pending.generations.rate,pending.generations.config,pending.generations.plan,if pending.fetched {"built"} else {"not_yet_valued"}])?;
        Ok(true)
    }
}

fn config_key(config: &MultiplierConfig, reasoning: &[String]) -> Result<String, ReportError> {
    encode(&(
        &config.api_twins,
        &config
            .quota_providers
            .iter()
            .map(|(key, pool)| (key, &pool.scope))
            .collect::<BTreeMap<_, _>>(),
        reasoning,
    ))
}

fn cache_generation(conn: &Connection) -> Result<i64, ReportError> {
    Ok(conn.query_row(
        "SELECT generation FROM rate_cache_state WHERE id=1",
        [],
        |r| r.get(0),
    )?)
}

// The model-to-hour invalidation lookup uses summary cells. Persisting a
// valuation ahead of its summary could hide a newly added model from that lookup.
fn summary_current(conn: &Connection, hour: Hour) -> Result<bool, ReportError> {
    Ok(conn.query_row("SELECT built_revision IS revision AND format_version IS ?3 FROM summary_hour WHERE kind=?1 AND hour_number=?2",params![hour.kind,hour.number,FORMAT],|r|r.get(0))?)
}

fn valuation_revision(conn: &Connection, hour: Hour) -> Result<i64, ReportError> {
    Ok(conn.query_row(
        "SELECT revision FROM valuation_hour WHERE kind=?1 AND hour_number=?2",
        params![hour.kind, hour.number],
        |r| r.get(0),
    )?)
}

#[cfg(test)]
mod tests;
