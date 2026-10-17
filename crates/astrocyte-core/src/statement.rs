//! Vendor statement identities, immutable line content, and transactional matching.

use std::collections::BTreeMap;

use rusqlite::{params, Transaction};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::canon::{canonical_hash, CanonError};
use crate::ingest::{
    record_adjustment_in_tx, AdjustmentReason, AdjustmentRecordRequest, AdjustmentWriteContext,
    IngestError,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StatementIdentity {
    pub provider: String,
    pub account_ref: String,
    pub period_start_ms: i64,
    pub period_end_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Statement {
    pub statement_id: String,
    pub provider: String,
    pub account_ref: String,
    pub period_start_ms: i64,
    pub period_end_ms: i64,
    pub currency: String,
    pub amount_exponent: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StatementLineInput {
    pub line_id: String,
    pub occurred_at_ms: i64,
    pub description: String,
    pub quantity: i64,
    pub unit: String,
    pub amount_minor: i64,
    pub currency: String,
    pub correlation_ref: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MatchOutcome {
    #[serde(rename = "unmatched")]
    Unmatched,
    #[serde(rename = "settled")]
    Settled,
    #[serde(rename = "ambiguous")]
    Ambiguous,
    #[serde(rename = "already-settled")]
    AlreadySettled,
    #[serde(rename = "unpriced_target")]
    UnpricedTarget,
    #[serde(rename = "currency_mismatch")]
    CurrencyMismatch,
}

impl MatchOutcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unmatched => "unmatched",
            Self::Settled => "settled",
            Self::Ambiguous => "ambiguous",
            Self::AlreadySettled => "already-settled",
            Self::UnpricedTarget => "unpriced_target",
            Self::CurrencyMismatch => "currency_mismatch",
        }
    }

    pub(crate) fn parse(value: &str) -> Option<Self> {
        match value {
            "unmatched" => Some(Self::Unmatched),
            "settled" => Some(Self::Settled),
            "ambiguous" => Some(Self::Ambiguous),
            "already-settled" => Some(Self::AlreadySettled),
            "unpriced_target" => Some(Self::UnpricedTarget),
            "currency_mismatch" => Some(Self::CurrencyMismatch),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct MatchSummary {
    pub unmatched_lines: usize,
    pub settled_lines: usize,
    pub ambiguous_lines: usize,
    pub already_settled_lines: usize,
    pub unpriced_target_lines: usize,
    pub currency_mismatch_lines: usize,
    pub changed_outcomes: usize,
}

impl MatchSummary {
    pub fn line_count(&self) -> usize {
        self.unmatched_lines
            + self.settled_lines
            + self.ambiguous_lines
            + self.already_settled_lines
            + self.unpriced_target_lines
            + self.currency_mismatch_lines
    }

    fn record(&mut self, outcome: MatchOutcome, changed: bool) {
        match outcome {
            MatchOutcome::Unmatched => self.unmatched_lines += 1,
            MatchOutcome::Settled => self.settled_lines += 1,
            MatchOutcome::Ambiguous => self.ambiguous_lines += 1,
            MatchOutcome::AlreadySettled => self.already_settled_lines += 1,
            MatchOutcome::UnpricedTarget => self.unpriced_target_lines += 1,
            MatchOutcome::CurrencyMismatch => self.currency_mismatch_lines += 1,
        }
        if changed {
            self.changed_outcomes += 1;
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StatementRefusal {
    Malformed {
        line: Option<usize>,
        field: String,
        detail: String,
    },
    InvalidPeriod,
    LineOutsidePeriod {
        line_ids: Vec<String>,
    },
    StatementCurrencyMismatch {
        line_ids: Vec<String>,
    },
    ConflictingReimport {
        line_ids: Vec<String>,
        detail: String,
    },
    AmountOutOfRange {
        line_id: String,
    },
    SettlementActRefused(String),
}

impl StatementRefusal {
    pub fn code(&self) -> &'static str {
        match self {
            Self::Malformed { .. } => "malformed",
            Self::InvalidPeriod => "invalid_period",
            Self::LineOutsidePeriod { .. } => "line_outside_period",
            Self::StatementCurrencyMismatch { .. } => "statement_currency_mismatch",
            Self::ConflictingReimport { .. } => "conflicting_reimport",
            Self::AmountOutOfRange { .. } => "amount_out_of_range",
            Self::SettlementActRefused(_) => "settlement_act_refused",
        }
    }

    pub fn message(&self) -> String {
        match self {
            Self::Malformed {
                line,
                field,
                detail,
            } => match line {
                Some(line) => format!("line {line} field '{field}': {detail}"),
                None => format!("field '{field}': {detail}"),
            },
            Self::InvalidPeriod => {
                "period_start_ms and period_end_ms are required and start must precede end".into()
            }
            Self::LineOutsidePeriod { line_ids } => {
                format!(
                    "statement lines outside declared period: {}",
                    line_ids.join(", ")
                )
            }
            Self::StatementCurrencyMismatch { line_ids } => format!(
                "statement lines differ from the declared currency: {}",
                line_ids.join(", ")
            ),
            Self::ConflictingReimport { line_ids, detail } => format!(
                "statement content conflicts for lines [{}]: {detail}",
                line_ids.join(", ")
            ),
            Self::AmountOutOfRange { line_id } => {
                format!("line '{line_id}' amount cannot be represented in nanounits")
            }
            Self::SettlementActRefused(detail) => {
                format!("statement settlement act refused: {detail}")
            }
        }
    }
}

#[derive(Debug)]
pub enum StatementError {
    Store(rusqlite::Error),
    Canon(CanonError),
    Refused(StatementRefusal),
}

impl StatementError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::Store(_) | Self::Canon(_) => "internal",
            Self::Refused(refusal) => refusal.code(),
        }
    }

    pub fn message(&self) -> String {
        match self {
            Self::Store(error) => format!("statement store operation: {error}"),
            Self::Canon(error) => format!("statement identity: {error:?}"),
            Self::Refused(refusal) => refusal.message(),
        }
    }
}

impl From<rusqlite::Error> for StatementError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Store(error)
    }
}

impl From<CanonError> for StatementError {
    fn from(error: CanonError) -> Self {
        Self::Canon(error)
    }
}

pub fn statement_id(identity: &StatementIdentity) -> Result<String, CanonError> {
    canonical_hash(&json!({
        "provider": identity.provider,
        "account_ref": identity.account_ref,
        "period_start_ms": identity.period_start_ms,
        "period_end_ms": identity.period_end_ms,
    }))
}

pub fn statement_line_id(statement_id: &str, line_id: &str) -> Result<String, CanonError> {
    canonical_hash(&json!({
        "statement_id": statement_id,
        "line_id": line_id,
    }))
}

pub(crate) fn vendor_amount_nanos(
    amount_minor: i64,
    amount_exponent: u32,
    line_id: &str,
) -> Result<i64, StatementError> {
    let power = 9_u32.checked_sub(amount_exponent).ok_or_else(|| {
        StatementError::Refused(StatementRefusal::AmountOutOfRange {
            line_id: line_id.to_string(),
        })
    })?;
    let scale = 10_i128.checked_pow(power).ok_or_else(|| {
        StatementError::Refused(StatementRefusal::AmountOutOfRange {
            line_id: line_id.to_string(),
        })
    })?;
    let amount = i128::from(amount_minor).checked_mul(scale).ok_or_else(|| {
        StatementError::Refused(StatementRefusal::AmountOutOfRange {
            line_id: line_id.to_string(),
        })
    })?;
    i64::try_from(amount).map_err(|_| {
        StatementError::Refused(StatementRefusal::AmountOutOfRange {
            line_id: line_id.to_string(),
        })
    })
}

#[derive(Debug)]
struct CorrelatedEvent {
    event_id: String,
    currency: Option<String>,
    unresolved: bool,
}

#[derive(Debug)]
struct LineForMatching {
    statement_line_id: String,
    line_id: String,
    occurred_at_ms: i64,
    amount_minor: i64,
    correlation_ref: Option<String>,
    prior_outcome: MatchOutcome,
    prior_event_id: Option<String>,
    prior_attempts: i64,
}

#[derive(Debug)]
struct MatchDecision {
    outcome: MatchOutcome,
    event_id: Option<String>,
    ledger_amount_nanos: Option<i64>,
}

pub(crate) fn reconcile_statement_in_tx(
    tx: &Transaction<'_>,
    statement_id: &str,
    attempted_at_ms: i64,
) -> Result<MatchSummary, StatementError> {
    let (provider, account_ref, statement_currency, amount_exponent) = tx.query_row(
        "SELECT provider,account_ref,currency,amount_exponent
         FROM statement WHERE statement_id=?1",
        [statement_id],
        |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, u32>(3)?,
            ))
        },
    )?;
    let mut stmt = tx.prepare_cached(
        "SELECT statement_line_id,line_id,occurred_at_ms,amount_minor,correlation_ref,
                 match_outcome,(SELECT event_id FROM usage_event WHERE event_key=matched_event_key),
                 (SELECT count(*) FROM match_attempt
                  WHERE match_attempt.statement_line_id=statement_line.statement_line_id)
         FROM statement_line
         WHERE statement_id=?1
         ORDER BY occurred_at_ms,line_id",
    )?;
    let lines = stmt
        .query_map([statement_id], |row| {
            let outcome = row.get::<_, String>(5)?;
            Ok(LineForMatching {
                statement_line_id: row.get(0)?,
                line_id: row.get(1)?,
                occurred_at_ms: row.get(2)?,
                amount_minor: row.get(3)?,
                correlation_ref: row.get(4)?,
                prior_outcome: MatchOutcome::parse(&outcome).ok_or_else(|| {
                    crate::ingest::stored_value_error(5, "statement_line.match_outcome", &outcome)
                })?,
                prior_event_id: row.get(6)?,
                prior_attempts: row.get(7)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    drop(stmt);

    let matching = StatementMatching {
        statement_id,
        currency: &statement_currency,
        attempted_at_ms,
    };
    let mut attribution = LedgerAttribution::load(tx, statement_id)?;
    let mut summary = MatchSummary::default();
    for line in lines {
        let vendor_amount =
            vendor_amount_nanos(line.amount_minor, amount_exponent, line.line_id.as_str())?;
        let correlated =
            correlated_events(tx, &provider, &account_ref, line.correlation_ref.as_deref())?;
        let decision = matching.decide(tx, &line, &correlated, vendor_amount, &attribution)?;

        persist_attempt(
            tx,
            &line,
            &decision,
            vendor_amount,
            attempted_at_ms,
            &mut attribution,
        )?;
        let changed =
            line.prior_outcome != decision.outcome || line.prior_event_id != decision.event_id;
        summary.record(decision.outcome, changed);
    }
    Ok(summary)
}

struct StatementMatching<'a> {
    statement_id: &'a str,
    currency: &'a str,
    attempted_at_ms: i64,
}

impl StatementMatching<'_> {
    fn decide(
        &self,
        tx: &Transaction<'_>,
        line: &LineForMatching,
        correlated: &[CorrelatedEvent],
        vendor_amount: i64,
        attribution: &LedgerAttribution,
    ) -> Result<MatchDecision, StatementError> {
        let candidates: Vec<&CorrelatedEvent> =
            correlated.iter().filter(|event| event.unresolved).collect();
        Ok(if correlated.is_empty() {
            MatchDecision {
                outcome: MatchOutcome::Unmatched,
                event_id: None,
                ledger_amount_nanos: None,
            }
        } else if candidates.is_empty() {
            if correlated.len() == 1 {
                let event = &correlated[0];
                let outcome = if event.currency.is_none() {
                    MatchOutcome::UnpricedTarget
                } else if event.currency.as_deref() != Some(self.currency) {
                    MatchOutcome::CurrencyMismatch
                } else {
                    MatchOutcome::AlreadySettled
                };
                let ledger_amount = if outcome == MatchOutcome::AlreadySettled {
                    attribution.amount(tx, &line.statement_line_id, &event.event_id)?
                } else {
                    current_ledger_amount(tx, &event.event_id)?
                };
                MatchDecision {
                    outcome,
                    event_id: Some(event.event_id.clone()),
                    ledger_amount_nanos: Some(ledger_amount),
                }
            } else {
                MatchDecision {
                    outcome: MatchOutcome::AlreadySettled,
                    event_id: None,
                    ledger_amount_nanos: None,
                }
            }
        } else if candidates.len() > 1 {
            MatchDecision {
                outcome: MatchOutcome::Ambiguous,
                event_id: None,
                ledger_amount_nanos: None,
            }
        } else {
            let candidate = candidates[0];
            if candidate.currency.is_none() {
                MatchDecision {
                    outcome: MatchOutcome::UnpricedTarget,
                    event_id: Some(candidate.event_id.clone()),
                    ledger_amount_nanos: Some(current_ledger_amount(tx, &candidate.event_id)?),
                }
            } else if candidate.currency.as_deref() != Some(self.currency) {
                MatchDecision {
                    outcome: MatchOutcome::CurrencyMismatch,
                    event_id: Some(candidate.event_id.clone()),
                    ledger_amount_nanos: Some(current_ledger_amount(tx, &candidate.event_id)?),
                }
            } else {
                let ledger_amount = current_ledger_amount(tx, &candidate.event_id)?;
                self.settle(tx, line, candidate, vendor_amount, ledger_amount)?;
                MatchDecision {
                    outcome: MatchOutcome::Settled,
                    event_id: Some(candidate.event_id.clone()),
                    ledger_amount_nanos: Some(ledger_amount),
                }
            }
        })
    }

    fn settle(
        &self,
        tx: &Transaction<'_>,
        line: &LineForMatching,
        candidate: &CorrelatedEvent,
        vendor_amount: i64,
        ledger_amount: i64,
    ) -> Result<(), StatementError> {
        let adjustment_amount = vendor_amount.checked_sub(ledger_amount).ok_or_else(|| {
            StatementError::Refused(StatementRefusal::AmountOutOfRange {
                line_id: line.line_id.clone(),
            })
        })?;
        tx.prepare_cached(
            "INSERT INTO chargeability_transition (
                 event_key,from_state,to_state,reason,resolved_by,occurred_at_ms
             ) VALUES ((SELECT event_key FROM usage_event WHERE event_id=?1),'unresolved','billed',?2,'statement',?3)",
        )?
        .execute(params![
            candidate.event_id,
            format!("statement {} line {}", self.statement_id, line.line_id),
            self.attempted_at_ms,
        ])?;
        if adjustment_amount != 0 {
            let request = AdjustmentRecordRequest {
                event_id: candidate.event_id.clone(),
                currency: self.currency.to_owned(),
                amount_nanos: adjustment_amount,
                reason: AdjustmentReason::VendorRestatement,
                occurred_at_ms: line.occurred_at_ms,
                note: Some(format!(
                    "vendor statement {} line {}",
                    self.statement_id, line.line_id
                )),
            };
            record_adjustment_in_tx(
                tx,
                &request,
                &AdjustmentWriteContext::statement(),
                self.attempted_at_ms,
            )
            .map_err(map_settlement_error)?;
        }
        Ok(())
    }
}

fn correlated_events(
    tx: &Transaction<'_>,
    provider: &str,
    account_ref: &str,
    correlation_ref: Option<&str>,
) -> Result<Vec<CorrelatedEvent>, StatementError> {
    let Some(correlation_ref) = correlation_ref else {
        return Ok(Vec::new());
    };
    let mut stmt = tx.prepare_cached(
        "SELECT event.event_id,event.currency,
                NOT EXISTS (
                    SELECT 1 FROM chargeability_transition transition
                    WHERE transition.event_key=event.event_key
                ) AS unresolved
         FROM usage_event event
         WHERE event.provider=?1 AND event.account_ref=?2 AND event.producer_ref=?3
         ORDER BY event.event_id",
    )?;
    let events = stmt
        .query_map(params![provider, account_ref, correlation_ref], |row| {
            Ok(CorrelatedEvent {
                event_id: row.get(0)?,
                currency: row.get(1)?,
                unresolved: row.get(2)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(events)
}

fn current_ledger_amount(tx: &Transaction<'_>, event_id: &str) -> Result<i64, StatementError> {
    let mut total = 0_i128;
    let mut charges = tx.prepare_cached(
        "SELECT charge.charge_nanos
         FROM priced_charge charge
         WHERE charge.event_key=(SELECT event_key FROM usage_event WHERE event_id=?1)
           AND charge.charge_nanos IS NOT NULL
           AND NOT EXISTS (
               SELECT 1 FROM priced_charge successor
               WHERE successor.supersedes=charge.charge_key
           )",
    )?;
    for amount in charges.query_map([event_id], |row| row.get::<_, i64>(0))? {
        total = total.checked_add(i128::from(amount?)).ok_or_else(|| {
            StatementError::Refused(StatementRefusal::SettlementActRefused(
                "ledger charge total overflowed".into(),
            ))
        })?;
    }
    drop(charges);
    let mut adjustments =
        tx.prepare_cached("SELECT amount_nanos FROM adjustment WHERE event_key=(SELECT event_key FROM usage_event WHERE event_id=?1)")?;
    for amount in adjustments.query_map([event_id], |row| row.get::<_, i64>(0))? {
        total = total.checked_add(i128::from(amount?)).ok_or_else(|| {
            StatementError::Refused(StatementRefusal::SettlementActRefused(
                "ledger adjustment total overflowed".into(),
            ))
        })?;
    }
    i64::try_from(total).map_err(|_| {
        StatementError::Refused(StatementRefusal::SettlementActRefused(
            "ledger amount is outside the stored integer range".into(),
        ))
    })
}

#[derive(Default)]
struct LedgerAttribution {
    events: BTreeMap<String, BTreeMap<String, AttributionSnapshot>>,
}

struct AttributionSnapshot {
    attempted_at_ms: i64,
    attempt_id: String,
    amount_nanos: i64,
}

impl LedgerAttribution {
    // Read history once per reconciliation instead of scanning it for every
    // line. New comparable attempts join this map immediately after insertion
    // so later lines still observe earlier lines' writes in the transaction.
    fn load(tx: &Transaction<'_>, statement_id: &str) -> Result<Self, StatementError> {
        let mut stmt = tx.prepare_cached(
            "SELECT event.event_id,attempt.statement_line_id,attempt.attempted_at_ms,
                    attempt.attempt_id,attempt.ledger_amount_nanos
             FROM match_attempt AS attempt
             JOIN usage_event AS event USING(event_key)
             JOIN statement_line AS line USING(statement_line_id)
             WHERE line.statement_id=?1 AND attempt.event_key IS NOT NULL
               AND attempt.outcome IN ('settled','already-settled')",
        )?;
        let mut attribution = Self::default();
        let mut rows = stmt.query([statement_id])?;
        while let Some(row) = rows.next()? {
            attribution.record(
                &row.get::<_, String>(0)?,
                &row.get::<_, String>(1)?,
                row.get(2)?,
                &row.get::<_, String>(3)?,
                row.get(4)?,
            );
        }
        Ok(attribution)
    }

    fn record(
        &mut self,
        event_id: &str,
        line_id: &str,
        attempted_at_ms: i64,
        attempt_id: &str,
        amount_nanos: i64,
    ) {
        let lines = self.events.entry(event_id.to_owned()).or_default();
        let replace = lines.get(line_id).is_none_or(|prior| {
            (attempted_at_ms, attempt_id) < (prior.attempted_at_ms, prior.attempt_id.as_str())
        });
        if replace {
            lines.insert(
                line_id.to_owned(),
                AttributionSnapshot {
                    attempted_at_ms,
                    attempt_id: attempt_id.to_owned(),
                    amount_nanos,
                },
            );
        }
    }

    fn amount(
        &self,
        tx: &Transaction<'_>,
        line_id: &str,
        event_id: &str,
    ) -> Result<i64, StatementError> {
        // Each line retains its earliest comparable snapshot. If only another
        // line claimed the event, this line must not count its ledger amount again.
        match self.events.get(event_id) {
            Some(lines) => Ok(lines
                .get(line_id)
                .map_or(0, |snapshot| snapshot.amount_nanos)),
            None => current_ledger_amount(tx, event_id),
        }
    }
}

fn persist_attempt(
    tx: &Transaction<'_>,
    line: &LineForMatching,
    decision: &MatchDecision,
    vendor_amount: i64,
    attempted_at_ms: i64,
    attribution: &mut LedgerAttribution,
) -> Result<(), StatementError> {
    let attempt_id = canonical_hash(&json!({
        "statement_line_id": line.statement_line_id,
        "attempted_at_ms": attempted_at_ms,
        "attempt_ordinal": line.prior_attempts,
    }))?;
    tx.prepare_cached(
        "INSERT INTO match_attempt (
             attempt_id,statement_line_id,outcome,event_key,ledger_amount_nanos,
             vendor_amount_nanos,attempted_at_ms
         ) VALUES (?1,?2,?3,(SELECT event_key FROM usage_event WHERE event_id=?4),?5,?6,?7)",
    )?
    .execute(params![
        attempt_id,
        line.statement_line_id,
        decision.outcome.as_str(),
        decision.event_id,
        decision.ledger_amount_nanos,
        vendor_amount,
        attempted_at_ms,
    ])?;
    tx.prepare_cached(
        "UPDATE statement_line SET match_outcome=?2,matched_event_key=(SELECT event_key FROM usage_event WHERE event_id=?3)
         WHERE statement_line_id=?1",
    )?
    .execute(params![
        line.statement_line_id,
        decision.outcome.as_str(),
        decision.event_id
    ])?;
    if matches!(
        decision.outcome,
        MatchOutcome::Settled | MatchOutcome::AlreadySettled
    ) {
        if let (Some(event_id), Some(amount)) = (&decision.event_id, decision.ledger_amount_nanos) {
            attribution.record(
                event_id,
                &line.statement_line_id,
                attempted_at_ms,
                &attempt_id,
                amount,
            );
        }
    }
    Ok(())
}

fn map_settlement_error(error: IngestError) -> StatementError {
    match error {
        IngestError::Store(error) => StatementError::Store(error),
        IngestError::Canon(error) => StatementError::Canon(error),
        IngestError::Refused(refusal) => StatementError::Refused(
            StatementRefusal::SettlementActRefused(refusal.as_str().to_string()),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statement_identity_is_operator_arguments_only_and_ignores_content() {
        let identity = StatementIdentity {
            provider: "vendor".into(),
            account_ref: "acct".into(),
            period_start_ms: 1,
            period_end_ms: 2,
        };
        let first = statement_id(&identity).unwrap();
        let second = statement_id(&identity).unwrap();
        assert_eq!(first, second);
        assert_ne!(
            first,
            statement_id(&StatementIdentity {
                period_end_ms: 3,
                ..identity
            })
            .unwrap()
        );
    }

    #[test]
    fn statement_line_identity_uses_vendor_line_id_verbatim() {
        let first = statement_line_id("statement", "Line-A").unwrap();
        assert_eq!(first, statement_line_id("statement", "Line-A").unwrap());
        assert_ne!(first, statement_line_id("statement", "line-a").unwrap());
    }

    #[test]
    fn jpy_exponent_zero_scales_whole_currency_units_to_nanos() {
        assert_eq!(vendor_amount_nanos(2, 0, "line").unwrap(), 2_000_000_000);
        assert_eq!(vendor_amount_nanos(200, 2, "line").unwrap(), 2_000_000_000);
    }
}
