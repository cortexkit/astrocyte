use std::collections::BTreeMap;

use rusqlite::{params, Connection, Row};

use crate::capacity::{pool_identity, WindowModelScope};

use super::super::{clamp_nanos, millis_to_nanos};
use super::capture::{read_scope, sample_table};
use super::history::VaultPlanTimeline;
use super::{PoolLogin, Stream};

type Logins<'a> = BTreeMap<&'a PoolLogin, Vec<&'a str>>;
type Earliest<T> = Option<(i64, Agreement<T>)>;
type ProviderCache<'a> =
    BTreeMap<(&'a str, &'a str, &'a PoolLogin), BTreeMap<i64, Earliest<String>>>;

#[derive(Clone, Copy, Default)]
enum Agreement<T> {
    #[default]
    Missing,
    Agreed(T),
    Conflict,
}

impl<T: Eq> Agreement<T> {
    fn merge(&mut self, other: Self) {
        match (&*self, &other) {
            (_, Self::Missing) => (),
            (Self::Missing, _) => *self = other,
            (Self::Agreed(a), Self::Agreed(b)) if a == b => (),
            _ => *self = Self::Conflict,
        }
    }
}

pub(super) fn inherit_metadata(
    tx: &Connection,
    quota_provider: &str,
    cut_ns: i64,
    history: &VaultPlanTimeline,
    keys: &[(PoolLogin, String)],
    streams: &mut [Stream],
) -> rusqlite::Result<()> {
    let mut pools = BTreeMap::<&str, Logins<'_>>::new();
    for (login, key) in keys {
        if let Some(pool) = pool_identity(key) {
            pools
                .entry(pool)
                .or_default()
                .entry(login)
                .or_default()
                .push(key);
        }
    }
    let mut evidence = Evidence {
        tx,
        quota_provider,
        cut_ns,
        history,
        scopes: BTreeMap::new(),
        providers: BTreeMap::new(),
    };
    for stream in streams {
        let (pool, logins) = pools
            .get_key_value(stream.pool_id.as_str())
            .expect("captured stream key");
        let (&login, own_keys) = logins
            .get_key_value(&stream.login)
            .expect("captured login key");
        // Keep an unstated first reading distinct from a login that hasn't
        // reported at all. Only the latter can borrow a peer's historical scope.
        let own_scope =
            if stream.readings.iter().any(|reading| {
                reading.fetched_at_instant_ns < cut_ns && reading.model_scope.is_none()
            }) {
                evidence.first(login, own_keys, (cut_ns, i64::MAX), "", |row| {
                    read_scope(row, 1)
                })?
            } else {
                None
            };
        for reading in &mut stream.readings {
            let plan = history.plan_at(login.ledger, &login.id, reading.fetched_at_ms());
            if reading.fetched_at_instant_ns < cut_ns && reading.model_scope.is_none() {
                let scope = match own_scope {
                    Some((_, Agreement::Agreed(Some(scope)))) => Agreement::Agreed(scope),
                    Some((_, Agreement::Conflict)) => Agreement::Conflict,
                    Some(_) => Agreement::Missing,
                    None => match plan {
                        Some(plan) => evidence.peer_scope(plan, pool, logins)?,
                        None => Agreement::Missing,
                    },
                };
                match scope {
                    Agreement::Agreed(scope) => reading.model_scope = Some(scope),
                    Agreement::Conflict => {
                        stream.scope_conflicts.insert(reading.fetched_at_instant_ns);
                    }
                    Agreement::Missing => (),
                }
            }
            if let Some(plan) = plan.filter(|_| reading.api_provider.is_none()) {
                if let Some(from) = reading.fetched_at_instant_ns.checked_add(1) {
                    reading.api_provider = evidence.provider(plan, pool, logins, login, from)?;
                }
            }
        }
    }
    Ok(())
}

struct Evidence<'a> {
    tx: &'a Connection,
    quota_provider: &'a str,
    cut_ns: i64,
    history: &'a VaultPlanTimeline,
    scopes: BTreeMap<(&'a str, &'a str), Agreement<WindowModelScope>>,
    providers: ProviderCache<'a>,
}

impl<'a> Evidence<'a> {
    fn peer_scope(
        &mut self,
        plan: &'a str,
        pool: &'a str,
        logins: &Logins<'_>,
    ) -> rusqlite::Result<Agreement<WindowModelScope>> {
        if let Some(scope) = self.scopes.get(&(plan, pool)) {
            return Ok(*scope);
        }
        let mut scope = Agreement::Missing;
        for (&peer, keys) in logins {
            if let Some((_, next)) = self.first_in_plan(
                plan,
                peer,
                keys,
                self.cut_ns,
                "AND model_scope IN ('all','subset')",
                |row| {
                    Ok(match row.get_ref(1)?.as_str()? {
                        "all" => WindowModelScope::All,
                        _ => WindowModelScope::Subset,
                    })
                },
            )? {
                scope.merge(next);
            }
        }
        self.scopes.insert((plan, pool), scope);
        Ok(scope)
    }

    fn provider(
        &mut self,
        plan: &'a str,
        pool: &'a str,
        logins: &Logins<'a>,
        own: &'a PoolLogin,
        from: i64,
    ) -> rusqlite::Result<Option<String>> {
        // A reading's own login is more specific than any peer.
        let provider = match self.next_provider((plan, pool, own), &logins[own], from)? {
            Some(own) => own,
            None => {
                let mut agreed = Agreement::Missing;
                for (&peer, keys) in logins.iter().filter(|(&peer, _)| peer != own) {
                    if let Some(next) =
                        self.next_provider((plan, pool, peer), keys, from.max(self.cut_ns))?
                    {
                        agreed.merge(next);
                    }
                }
                agreed
            }
        };
        Ok(match provider {
            Agreement::Agreed(provider) => Some(provider),
            Agreement::Missing | Agreement::Conflict => None,
        })
    }

    fn next_provider(
        &mut self,
        key @ (plan, _, login): (&'a str, &'a str, &'a PoolLogin),
        keys: &[&str],
        from: i64,
    ) -> rusqlite::Result<Option<Agreement<String>>> {
        // A whole run of old nulls often inherits the same later reading. Reuse
        // the indexed seek until the request moves past that reading.
        if let Some((_, next)) = self
            .providers
            .get(&key)
            .and_then(|cache| cache.range(..=from).next_back())
        {
            if next.as_ref().is_none_or(|(at, _)| *at >= from) {
                return Ok(next.as_ref().map(|(_, value)| value.clone()));
            }
        }
        let next =
            self.first_in_plan(plan, login, keys, from, "AND api_provider <> ''", |row| {
                row.get(2)
            })?;
        let result = next.as_ref().map(|(_, value)| value.clone());
        self.providers.entry(key).or_default().insert(from, next);
        Ok(result)
    }

    fn first_in_plan<T: Eq>(
        &self,
        plan: &str,
        login: &PoolLogin,
        keys: &[&str],
        from: i64,
        condition: &str,
        read: impl Fn(&Row<'_>) -> rusqlite::Result<T>,
    ) -> rusqlite::Result<Earliest<T>> {
        for (start, end) in self.history.plan_periods(login.ledger, &login.id, plan) {
            let start = from.max(clamp_nanos(millis_to_nanos(start)));
            let end = end.map_or(i64::MAX, |end| clamp_nanos(millis_to_nanos(end) - 1));
            if start <= end {
                if let Some(first) = self.first(login, keys, (start, end), condition, &read)? {
                    return Ok(Some(first));
                }
            }
        }
        Ok(None)
    }

    fn first<T: Eq>(
        &self,
        login: &PoolLogin,
        keys: &[&str],
        (from, to): (i64, i64),
        condition: &str,
        read: impl Fn(&Row<'_>) -> rusqlite::Result<T>,
    ) -> rusqlite::Result<Earliest<T>> {
        let (table, field) = sample_table(login.ledger);
        let mut stmt = self.tx.prepare_cached(&format!(
            "SELECT fetched_at_instant,model_scope,api_provider FROM {table}
             WHERE provider=?1 AND {field}=?2 AND pool_key=?3
               AND fetched_at_instant BETWEEN ?4 AND ?5 {condition}
             ORDER BY fetched_at_instant LIMIT 1"
        ))?;
        let mut earliest: Earliest<T> = None;
        for key in keys {
            let mut rows = stmt.query(params![self.quota_provider, login.id, key, from, to])?;
            if let Some(row) = rows.next()? {
                let at = row.get(0)?;
                match &mut earliest {
                    Some((previous, value)) if *previous == at => {
                        value.merge(Agreement::Agreed(read(row)?))
                    }
                    Some((previous, _)) if *previous < at => (),
                    _ => earliest = Some((at, Agreement::Agreed(read(row)?))),
                }
            }
        }
        Ok(earliest)
    }
}
