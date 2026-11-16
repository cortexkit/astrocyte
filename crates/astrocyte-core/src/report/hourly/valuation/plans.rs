use super::*;
use crate::subscription::PlanLedger;

pub(crate) struct PlanRecord {
    pub id: PlanRecordId,
    pub until_ms: Option<i64>,
    pub minor: i64,
    pub exponent: u32,
    pub currency: String,
    pub period: String,
}

/// One snapshot of the small subscription ledgers, shared by a build chunk or
/// report. Identity lookup and interval membership never perform per-call SQL.
pub struct PlanBook {
    pub(crate) records: Vec<PlanRecord>,
    by_identity: BTreeMap<(PlanLedger, String, String), Vec<usize>>,
}

impl PlanBook {
    pub fn load(conn: &Connection) -> Result<Self, ReportError> {
        let mut book = Self {
            records: Vec::new(),
            by_identity: BTreeMap::new(),
        };
        for ledger in [PlanLedger::Account, PlanLedger::Credential] {
            let mut stmt = conn.prepare(&format!("SELECT {},provider,effective_from_ms,effective_to_ms,recurring_cost_minor,amount_exponent,currency,period FROM {}", ledger.key_column(), ledger.table()))?;
            let mut rows = stmt.query([])?;
            while let Some(row) = rows.next()? {
                let id = PlanRecordId {
                    ledger,
                    identity: row.get(0)?,
                    provider: row.get(1)?,
                    effective_from_ms: row.get(2)?,
                };
                book.by_identity
                    .entry((ledger, id.provider.clone(), id.identity.clone()))
                    .or_default()
                    .push(book.records.len());
                book.records.push(PlanRecord {
                    id,
                    until_ms: row.get(3)?,
                    minor: row.get(4)?,
                    exponent: row.get(5)?,
                    currency: row.get(6)?,
                    period: row.get(7)?,
                });
            }
        }
        Ok(book)
    }

    pub(super) fn active(
        &self,
        ledger: PlanLedger,
        provider: &str,
        identity: &str,
        at: i64,
    ) -> impl Iterator<Item = &PlanRecord> {
        self.by_identity
            .get(&(ledger, provider.into(), identity.into()))
            .into_iter()
            .flatten()
            .map(|index| &self.records[*index])
            .filter(move |record| {
                record.id.effective_from_ms <= at && record.until_ms.is_none_or(|until| at < until)
            })
    }

    pub(super) fn possible_credential(&self, provider: &str, from: i64, last: i64) -> bool {
        self.records.iter().any(|record| {
            record.id.ledger == PlanLedger::Credential
                && record.id.provider == provider
                && record.id.effective_from_ms <= last
                && record.until_ms.is_none_or(|until| until > from)
        })
    }
}
