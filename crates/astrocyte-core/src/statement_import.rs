//! Transactional CSV statement import with content idempotency and invocation provenance.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use rusqlite::{params, OptionalExtension, Transaction};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};

use crate::canon::canonical_hash;
use crate::statement::{
    reconcile_statement_in_tx, statement_id, statement_line_id, vendor_amount_nanos, MatchSummary,
    StatementError, StatementIdentity, StatementLineInput, StatementRefusal,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StatementImportRequest {
    pub path: String,
    pub provider: String,
    pub account_ref: String,
    pub period_start: i64,
    pub period_end: i64,
    pub amount_exponent: u32,
    pub currency: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StatementContentOutcome {
    Created,
    NoOp,
}

impl StatementContentOutcome {
    fn as_str(self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::NoOp => "no_op",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StatementImportOutcome {
    pub statement_id: String,
    pub import_id: String,
    pub content_outcome: StatementContentOutcome,
    pub lines_created: usize,
    pub lines_skipped: usize,
    pub matching: MatchSummary,
}

impl StatementImportOutcome {
    pub fn changed(&self) -> bool {
        true
    }
}

pub fn import_statement_in_tx(
    tx: &Transaction<'_>,
    request: &StatementImportRequest,
    file_bytes: &[u8],
    operator: Option<&str>,
    imported_at_ms: i64,
) -> Result<StatementImportOutcome, StatementError> {
    let parsed = parse_and_validate(request, file_bytes, imported_at_ms)?;
    import_statement_in_tx_parsed(tx, request, &parsed, operator, imported_at_ms)
}

struct ParsedImport {
    identity: StatementIdentity,
    statement_id: String,
    lines: Vec<StatementLineInput>,
    file_sha256: String,
}

fn parse_and_validate(
    request: &StatementImportRequest,
    file_bytes: &[u8],
    imported_at_ms: i64,
) -> Result<ParsedImport, StatementError> {
    if request.path.is_empty() {
        return malformed(None, "path", "source path is required");
    }
    if request.provider.is_empty() {
        return malformed(None, "provider", "provider is required");
    }
    if request.account_ref.is_empty() {
        return malformed(None, "account_ref", "account reference is required");
    }
    if request.currency.is_empty() {
        return malformed(None, "currency", "statement currency is required");
    }
    if imported_at_ms < 0 {
        return malformed(None, "imported_at_ms", "import instant cannot be negative");
    }
    if request.period_start < 0 || request.period_start >= request.period_end {
        return Err(StatementError::Refused(StatementRefusal::InvalidPeriod));
    }
    if request.amount_exponent > 9 {
        return malformed(
            None,
            "amount_exponent",
            "amount exponent must be between 0 and 9",
        );
    }

    let identity = StatementIdentity {
        provider: request.provider.clone(),
        account_ref: request.account_ref.clone(),
        period_start_ms: request.period_start,
        period_end_ms: request.period_end,
    };
    let statement_id = statement_id(&identity)?;
    let records = parse_csv(file_bytes)?;
    let lines = parse_lines(request, records)?;
    let mut hasher = Sha256::new();
    hasher.update(file_bytes);
    Ok(ParsedImport {
        identity,
        statement_id,
        lines,
        file_sha256: hex::encode(hasher.finalize()),
    })
}

fn import_statement_in_tx_parsed(
    tx: &Transaction<'_>,
    request: &StatementImportRequest,
    parsed: &ParsedImport,
    operator: Option<&str>,
    imported_at_ms: i64,
) -> Result<StatementImportOutcome, StatementError> {
    let existing = tx
        .query_row(
            "SELECT currency,amount_exponent FROM statement WHERE statement_id=?1",
            [&parsed.statement_id],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, u32>(1)?)),
        )
        .optional()?;

    let content_outcome = if let Some((currency, amount_exponent)) = existing {
        if currency != request.currency || amount_exponent != request.amount_exponent {
            return Err(StatementError::Refused(
                StatementRefusal::ConflictingReimport {
                    line_ids: parsed
                        .lines
                        .iter()
                        .map(|line| line.line_id.clone())
                        .collect(),
                    detail: format!(
                        "stored currency/exponent {currency}/{amount_exponent}, declared {}/{}",
                        request.currency, request.amount_exponent
                    ),
                },
            ));
        }
        ensure_equal_content(tx, &parsed.statement_id, &parsed.lines)?;
        StatementContentOutcome::NoOp
    } else {
        insert_content(tx, request, parsed)?;
        StatementContentOutcome::Created
    };

    let matching = reconcile_statement_in_tx(tx, &parsed.statement_id, imported_at_ms)?;
    let line_count = parsed.lines.len();
    debug_assert_eq!(matching.line_count(), line_count);
    let (lines_created, lines_skipped) = match content_outcome {
        StatementContentOutcome::Created => (line_count, 0),
        StatementContentOutcome::NoOp => (0, line_count),
    };
    let import_ordinal: i64 = tx.query_row(
        "SELECT count(*) FROM statement_import WHERE statement_id=?1",
        [&parsed.statement_id],
        |row| row.get(0),
    )?;
    let import_id = canonical_hash(&json!({
        "statement_id": parsed.statement_id,
        "file_sha256": parsed.file_sha256,
        "source_path": request.path,
        "imported_at_ms": imported_at_ms,
        "operator": operator,
        "import_ordinal": import_ordinal,
    }))?;
    tx.execute(
        "INSERT INTO statement_import (
             import_id,statement_id,file_sha256,source_path,imported_at_ms,operator,outcome,
             lines_created,lines_skipped,unmatched_lines,settled_lines,ambiguous_lines,
             already_settled_lines,unpriced_target_lines,currency_mismatch_lines,changed_outcomes
         ) VALUES (
             ?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16
         )",
        params![
            import_id,
            parsed.statement_id,
            parsed.file_sha256,
            request.path,
            imported_at_ms,
            operator,
            content_outcome.as_str(),
            to_i64(lines_created)?,
            to_i64(lines_skipped)?,
            to_i64(matching.unmatched_lines)?,
            to_i64(matching.settled_lines)?,
            to_i64(matching.ambiguous_lines)?,
            to_i64(matching.already_settled_lines)?,
            to_i64(matching.unpriced_target_lines)?,
            to_i64(matching.currency_mismatch_lines)?,
            to_i64(matching.changed_outcomes)?,
        ],
    )?;
    Ok(StatementImportOutcome {
        statement_id: parsed.statement_id.clone(),
        import_id,
        content_outcome,
        lines_created,
        lines_skipped,
        matching,
    })
}

fn insert_content(
    tx: &Transaction<'_>,
    request: &StatementImportRequest,
    parsed: &ParsedImport,
) -> Result<(), StatementError> {
    tx.execute(
        "INSERT INTO statement (
             statement_id,provider,account_ref,period_start_ms,period_end_ms,currency,amount_exponent
         ) VALUES (?1,?2,?3,?4,?5,?6,?7)",
        params![
            parsed.statement_id,
            parsed.identity.provider,
            parsed.identity.account_ref,
            parsed.identity.period_start_ms,
            parsed.identity.period_end_ms,
            request.currency,
            request.amount_exponent,
        ],
    )?;
    for line in &parsed.lines {
        let id = statement_line_id(&parsed.statement_id, &line.line_id)?;
        tx.execute(
            "INSERT INTO statement_line (
                 statement_line_id,statement_id,line_id,occurred_at_ms,description,quantity,
                 unit,amount_minor,currency,correlation_ref,match_outcome,matched_event_key
             ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,'unmatched',NULL)",
            params![
                id,
                parsed.statement_id,
                line.line_id,
                line.occurred_at_ms,
                line.description,
                line.quantity,
                line.unit,
                line.amount_minor,
                line.currency,
                line.correlation_ref,
            ],
        )?;
    }
    Ok(())
}

fn ensure_equal_content(
    tx: &Transaction<'_>,
    statement_id: &str,
    incoming: &[StatementLineInput],
) -> Result<(), StatementError> {
    let mut stmt = tx.prepare(
        "SELECT line_id,occurred_at_ms,description,quantity,unit,amount_minor,currency,correlation_ref
         FROM statement_line WHERE statement_id=?1",
    )?;
    let stored = stmt
        .query_map([statement_id], |row| {
            let line = StatementLineInput {
                line_id: row.get(0)?,
                occurred_at_ms: row.get(1)?,
                description: row.get(2)?,
                quantity: row.get(3)?,
                unit: row.get(4)?,
                amount_minor: row.get(5)?,
                currency: row.get(6)?,
                correlation_ref: row.get(7)?,
            };
            Ok((line.line_id.clone(), line))
        })?
        .collect::<Result<BTreeMap<_, _>, _>>()?;
    let incoming = incoming
        .iter()
        .cloned()
        .map(|line| (line.line_id.clone(), line))
        .collect::<BTreeMap<_, _>>();
    let mut conflicts = BTreeSet::new();
    for line_id in stored.keys().chain(incoming.keys()) {
        if stored.get(line_id) != incoming.get(line_id) {
            conflicts.insert(line_id.clone());
        }
    }
    if conflicts.is_empty() {
        return Ok(());
    }
    Err(StatementError::Refused(
        StatementRefusal::ConflictingReimport {
            line_ids: conflicts.into_iter().collect(),
            detail: "normalised line fields differ".into(),
        },
    ))
}

#[derive(Debug)]
struct CsvRecord {
    row_number: usize,
    fields: Vec<String>,
}

fn parse_csv(file_bytes: &[u8]) -> Result<Vec<CsvRecord>, StatementError> {
    let text = std::str::from_utf8(file_bytes)
        .map_err(|_| malformed_error(None, "file", "CSV is not valid UTF-8"))?;
    let mut records = Vec::new();
    let mut fields = Vec::new();
    let mut field = String::new();
    let mut chars = text.chars().peekable();
    let mut in_quotes = false;
    let mut closed_quote = false;
    let mut row_number = 1_usize;

    while let Some(ch) = chars.next() {
        if in_quotes {
            if ch == '"' {
                if chars.peek() == Some(&'"') {
                    chars.next();
                    field.push('"');
                } else {
                    in_quotes = false;
                    closed_quote = true;
                }
            } else {
                field.push(ch);
            }
            continue;
        }
        if closed_quote {
            match ch {
                ',' => {
                    fields.push(std::mem::take(&mut field));
                    closed_quote = false;
                }
                '\n' => {
                    fields.push(std::mem::take(&mut field));
                    records.push(CsvRecord {
                        row_number,
                        fields: std::mem::take(&mut fields),
                    });
                    row_number += 1;
                    closed_quote = false;
                }
                '\r' if chars.peek() == Some(&'\n') => {
                    chars.next();
                    fields.push(std::mem::take(&mut field));
                    records.push(CsvRecord {
                        row_number,
                        fields: std::mem::take(&mut fields),
                    });
                    row_number += 1;
                    closed_quote = false;
                }
                _ => {
                    return malformed(
                        Some(row_number),
                        "file",
                        "unexpected character after a quoted field",
                    )
                }
            }
            continue;
        }
        match ch {
            '"' if field.is_empty() => in_quotes = true,
            '"' => return malformed(Some(row_number), "file", "quote must begin a field"),
            ',' => fields.push(std::mem::take(&mut field)),
            '\n' => {
                fields.push(std::mem::take(&mut field));
                records.push(CsvRecord {
                    row_number,
                    fields: std::mem::take(&mut fields),
                });
                row_number += 1;
            }
            '\r' if chars.peek() == Some(&'\n') => {
                chars.next();
                fields.push(std::mem::take(&mut field));
                records.push(CsvRecord {
                    row_number,
                    fields: std::mem::take(&mut fields),
                });
                row_number += 1;
            }
            _ => field.push(ch),
        }
    }
    if in_quotes {
        return malformed(Some(row_number), "file", "quoted field is not terminated");
    }
    if closed_quote || !field.is_empty() || !fields.is_empty() {
        fields.push(field);
        records.push(CsvRecord { row_number, fields });
    }
    if records.is_empty() {
        return malformed(None, "file", "CSV must contain a header row");
    }
    Ok(records)
}

fn parse_lines(
    request: &StatementImportRequest,
    mut records: Vec<CsvRecord>,
) -> Result<Vec<StatementLineInput>, StatementError> {
    let header = records.remove(0);
    let mut columns = HashMap::new();
    for (index, name) in header.fields.iter().enumerate() {
        if columns.insert(name.as_str(), index).is_some() {
            return malformed(
                Some(header.row_number),
                name,
                "header column appears more than once",
            );
        }
    }
    const REQUIRED: [&str; 7] = [
        "line_id",
        "occurred_at",
        "description",
        "quantity",
        "unit",
        "amount_minor",
        "currency",
    ];
    for name in REQUIRED {
        if !columns.contains_key(name) {
            return malformed(Some(header.row_number), name, "required column is absent");
        }
    }
    if records.is_empty() {
        return malformed(None, "file", "CSV must contain at least one statement line");
    }

    let mut seen = BTreeSet::new();
    let mut lines = Vec::with_capacity(records.len());
    let mut outside_period = Vec::new();
    let mut currency_mismatch = Vec::new();
    for record in records {
        if record.fields.len() != header.fields.len() {
            return malformed(
                Some(record.row_number),
                "file",
                "row has a different field count from the header",
            );
        }
        let value = |name: &str| record.fields[columns[name]].as_str();
        let line_id = value("line_id").to_string();
        if line_id.is_empty() {
            return malformed(
                Some(record.row_number),
                "line_id",
                "line identifier is required",
            );
        }
        if !seen.insert(line_id.clone()) {
            return malformed(
                Some(record.row_number),
                "line_id",
                "line identifier appears more than once",
            );
        }
        let occurred_at_ms =
            parse_timestamp(record.row_number, "occurred_at", value("occurred_at"))?;
        let quantity = parse_integer(record.row_number, "quantity", value("quantity"))?;
        if quantity < 0 {
            return malformed(
                Some(record.row_number),
                "quantity",
                "quantity cannot be negative",
            );
        }
        let amount_minor = parse_integer(record.row_number, "amount_minor", value("amount_minor"))?;
        let unit = value("unit").to_string();
        if unit.is_empty() {
            return malformed(Some(record.row_number), "unit", "unit is required");
        }
        let currency = value("currency").to_string();
        if occurred_at_ms < request.period_start || occurred_at_ms >= request.period_end {
            outside_period.push(line_id.clone());
        }
        if currency != request.currency {
            currency_mismatch.push(line_id.clone());
        }
        vendor_amount_nanos(amount_minor, request.amount_exponent, &line_id)?;
        let correlation_ref = columns
            .get("correlation_ref")
            .map(|index| record.fields[*index].clone())
            .filter(|value| !value.is_empty());
        lines.push(StatementLineInput {
            line_id,
            occurred_at_ms,
            description: value("description").to_string(),
            quantity,
            unit,
            amount_minor,
            currency,
            correlation_ref,
        });
    }
    if !outside_period.is_empty() {
        return Err(StatementError::Refused(
            StatementRefusal::LineOutsidePeriod {
                line_ids: outside_period,
            },
        ));
    }
    if !currency_mismatch.is_empty() {
        return Err(StatementError::Refused(
            StatementRefusal::StatementCurrencyMismatch {
                line_ids: currency_mismatch,
            },
        ));
    }
    lines.sort_by(|left, right| left.line_id.cmp(&right.line_id));
    Ok(lines)
}

fn parse_timestamp(row: usize, field: &str, value: &str) -> Result<i64, StatementError> {
    if let Ok(epoch_ms) = value.parse::<i64>() {
        return Ok(epoch_ms);
    }
    chrono::DateTime::parse_from_rfc3339(value)
        .map(|timestamp| timestamp.timestamp_millis())
        .map_err(|_| {
            malformed_error(
                Some(row),
                field,
                "timestamp must be epoch milliseconds or RFC 3339",
            )
        })
}

fn parse_integer(row: usize, field: &str, value: &str) -> Result<i64, StatementError> {
    value.parse::<i64>().map_err(|_| {
        malformed_error(
            Some(row),
            field,
            "value must be a base-10 integer without decoration",
        )
    })
}

fn malformed<T>(line: Option<usize>, field: &str, detail: &str) -> Result<T, StatementError> {
    Err(malformed_error(line, field, detail))
}

fn malformed_error(line: Option<usize>, field: &str, detail: &str) -> StatementError {
    StatementError::Refused(StatementRefusal::Malformed {
        line,
        field: field.to_string(),
        detail: detail.to_string(),
    })
}

fn to_i64(value: usize) -> Result<i64, StatementError> {
    i64::try_from(value).map_err(|_| {
        StatementError::Refused(StatementRefusal::SettlementActRefused(
            "statement line count exceeds the stored integer range".into(),
        ))
    })
}

#[cfg(test)]
mod tests {
    use rusqlite::Connection;
    use serde_json::json;

    use super::*;
    use crate::statement::MatchOutcome;
    use crate::test_support::conn;

    /// Imports a statement the way the served import operation does: through
    /// `import_statement_in_tx` inside one transaction that commits on success.
    fn import_statement(
        conn: &mut Connection,
        request: &StatementImportRequest,
        file_bytes: &[u8],
        operator: Option<&str>,
        imported_at_ms: i64,
    ) -> Result<StatementImportOutcome, StatementError> {
        let tx = conn.transaction()?;
        let outcome = import_statement_in_tx(&tx, request, file_bytes, operator, imported_at_ms)?;
        tx.commit()?;
        Ok(outcome)
    }

    fn request() -> StatementImportRequest {
        StatementImportRequest {
            path: "/vendor/jpy.csv".into(),
            provider: "vendor".into(),
            account_ref: "acct".into(),
            period_start: 100,
            period_end: 200,
            amount_exponent: 0,
            currency: "JPY".into(),
        }
    }

    fn csv(line: &str) -> Vec<u8> {
        format!(
            "line_id,occurred_at,description,quantity,unit,amount_minor,currency,correlation_ref\n{line}\n"
        )
        .into_bytes()
    }

    fn standard_line(correlation_ref: &str) -> Vec<u8> {
        csv(&format!(
            "line-1,150,vendor charge,1,request,2,JPY,{correlation_ref}"
        ))
    }

    fn statement_state(conn: &Connection) -> (i64, i64, i64, i64, i64, i64) {
        let count = |table: &str| {
            conn.query_row(&format!("SELECT count(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .unwrap()
        };
        (
            count("statement"),
            count("statement_line"),
            count("statement_import"),
            count("match_attempt"),
            count("chargeability_transition"),
            count("adjustment"),
        )
    }

    fn seed_event(
        conn: &Connection,
        event_id: &str,
        producer: &str,
        producer_ref: &str,
        currency: Option<&str>,
        charge_nanos: i64,
    ) {
        conn.execute(
            "INSERT INTO usage_event (
                 event_id,producer,producer_lineage,producer_ref,occurred_at_ms,recorded_at_ms,
                 provider,account_ref,charge_basis,pricing_generation,currency
             ) VALUES (?1,?2,'lineage',?3,150,151,'vendor','acct','metered_api',0,NULL)",
            params![event_id, producer, producer_ref],
        )
        .unwrap();
        if let Some(currency) = currency {
            price_existing_event(conn, event_id, currency, charge_nanos);
        }
    }

    fn price_existing_event(conn: &Connection, event_id: &str, currency: &str, charge_nanos: i64) {
        conn.execute(
            "UPDATE usage_event SET currency=?2,pricing_generation=pricing_generation+1
             WHERE event_id=?1",
            params![event_id, currency],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO measurement (
                 event_key,role,unit,kind,source,quantity_units,quantity_exponent
             ) VALUES ((SELECT event_key FROM usage_event WHERE event_id=?1),'usage','request','count','vendor',1,0)",
            [event_id],
        )
        .unwrap();
        // Statement matching consumes a ledger charge rather than a catalog rate,
        // so the fixture cannot establish whether a minimum applied.
        conn.execute(
            "INSERT INTO priced_charge (
                 charge_key,event_key,role,unit,pricing_generation,outcome,charge_basis,
                 basis_covered,currency,charge_nanos,unpriced_reason,rate_units,rate_exponent,
                  floor_state,floor_units,catalog_source,resolved_at_ms,catalog_entry,
                  supersedes,origin,recorded_at_ms
              ) VALUES (
                  NULL,(SELECT event_key FROM usage_event WHERE event_id=?1),'usage','request',1,'priced','metered_api',0,?2,?3,
                  NULL,1,9,'unknown',NULL,'test-catalog',152,'{}',NULL,'pricing_pass',152
             )",
            params![event_id, currency, charge_nanos],
        )
        .unwrap();
    }

    fn settle_event(conn: &Connection, event_id: &str) {
        conn.execute(
            "INSERT INTO chargeability_transition (
                 event_key,from_state,to_state,reason,resolved_by,occurred_at_ms
             ) VALUES ((SELECT event_key FROM usage_event WHERE event_id=?1),'unresolved','written_off','test','operator',160)",
            [event_id],
        )
        .unwrap();
    }

    fn line_outcome(conn: &Connection) -> (String, Option<String>) {
        conn.query_row(
            "SELECT match_outcome,(SELECT event_id FROM usage_event WHERE event_key=matched_event_key) FROM statement_line",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap()
    }

    #[test]
    #[ignore = "manual reconciliation timing fixture"]
    fn reconciliation_query_fixture() {
        let mut conn = conn();
        let mut bytes = String::from(
            "line_id,occurred_at,description,quantity,unit,amount_minor,currency,correlation_ref\n",
        );
        for index in 0..1_000 {
            let event = format!("event-{index}");
            let reference = format!("ref-{index}");
            seed_event(
                &conn,
                &event,
                "producer",
                &reference,
                Some("JPY"),
                1_000_000_000,
            );
            bytes.push_str(&format!(
                "line-{index},150,charge,1,request,2,JPY,{reference}\n"
            ));
        }
        let first = import_statement(&mut conn, &request(), bytes.as_bytes(), None, 170).unwrap();
        assert_eq!(first.matching.settled_lines, 1_000);
        let tx = conn.transaction().unwrap();
        let start = std::time::Instant::now();
        for instant in 171..176 {
            let summary = reconcile_statement_in_tx(&tx, &first.statement_id, instant).unwrap();
            assert_eq!(summary.already_settled_lines, 1_000);
            assert_eq!(
                summary.changed_outcomes,
                usize::from(instant == 171) * 1_000
            );
        }
        eprintln!(
            "reconciliation fixture: 5 passes x 1000 priced/adjusted lines: {:?}",
            start.elapsed()
        );
        assert_eq!(statement_state(&tx), (1, 1_000, 1, 6_000, 1_000, 1_000));
        let amounts: (i64, i64) = tx
            .query_row(
                "SELECT sum(vendor_amount_nanos),sum(ledger_amount_nanos) FROM match_attempt",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(amounts, (12_000_000_000_000, 6_000_000_000_000));
        let mut heads = tx
            .prepare(
                "SELECT charge.charge_nanos FROM priced_charge charge
             WHERE charge.event_id=?1 AND charge.charge_nanos IS NOT NULL
               AND NOT EXISTS (SELECT 1 FROM priced_charge successor
                               WHERE successor.supersedes=charge.charge_key)",
            )
            .unwrap();
        assert_eq!(
            heads
                .query_row(["event-0"], |row| row.get::<_, i64>(0))
                .unwrap(),
            1_000_000_000
        );
        let mut attribution = tx
            .prepare(
                "SELECT attempt.ledger_amount_nanos FROM match_attempt attempt
             JOIN statement_line line USING(statement_line_id)
             WHERE line.statement_id=?1 AND attempt.event_id='event-0'
               AND attempt.outcome IN ('settled','already-settled')
             ORDER BY (attempt.statement_line_id=?2) DESC,attempt.attempted_at_ms,attempt.attempt_id
             LIMIT 1",
            )
            .unwrap();
        let line_id = statement_line_id(&first.statement_id, "line-0").unwrap();
        assert_eq!(
            attribution
                .query_row(params![first.statement_id, line_id], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            1_000_000_000
        );
        eprintln!("representative indexed head lookup VM steps: {}; per-line historical attribution VM steps: {} (6000 attempts)",
            heads.get_status(rusqlite::StatementStatus::VmStep), attribution.get_status(rusqlite::StatementStatus::VmStep));
    }

    #[test]
    fn batched_attribution_retains_earliest_snapshot_per_line_with_tied_instants() {
        let mut conn = conn();
        seed_event(
            &conn,
            "event",
            "producer",
            "ref",
            Some("JPY"),
            1_000_000_000,
        );
        let bytes =
            csv("line-1,150,charge,1,request,2,JPY,ref\nline-2,150,charge,1,request,0,JPY,ref");
        let first = import_statement(&mut conn, &request(), &bytes, None, 170).unwrap();
        let line = statement_line_id(&first.statement_id, "line-1").unwrap();
        for (id, instant, amount) in [("z", 169, 800), ("a", 169, 700)] {
            conn.execute(
                "INSERT INTO match_attempt (
                     attempt_id,statement_line_id,outcome,event_key,ledger_amount_nanos,
                     vendor_amount_nanos,attempted_at_ms
                 ) VALUES (?1,?2,'already-settled',(SELECT event_key FROM usage_event WHERE event_id='event'),?3,2000000000,?4)",
                params![id, line, amount, instant],
            )
            .unwrap();
        }
        let tx = conn.transaction().unwrap();
        reconcile_statement_in_tx(&tx, &first.statement_id, 180).unwrap();
        let snapshots = tx
            .prepare(
                "SELECT line.line_id,attempt.ledger_amount_nanos
             FROM match_attempt attempt JOIN statement_line line USING(statement_line_id)
             WHERE attempted_at_ms=180 ORDER BY line.line_id",
            )
            .unwrap()
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            snapshots,
            vec![("line-1".into(), 700), ("line-2".into(), 0)]
        );
        tx.execute(
            "INSERT INTO match_attempt (
                 attempt_id,statement_line_id,outcome,event_key,ledger_amount_nanos,
                 vendor_amount_nanos,attempted_at_ms
             ) VALUES ('older',?1,'already-settled',(SELECT event_key FROM usage_event WHERE event_id='event'),600,2000000000,168)",
            [&line],
        )
        .unwrap();
        reconcile_statement_in_tx(&tx, &first.statement_id, 181).unwrap();
        let snapshot: i64 = tx
            .query_row(
                "SELECT ledger_amount_nanos FROM match_attempt
             WHERE statement_line_id=?1 AND attempted_at_ms=181",
                [&line],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(snapshot, 600);
        assert_eq!(statement_state(&tx), (1, 2, 1, 9, 1, 1));
    }

    #[test]
    fn absent_amount_exponent_is_malformed_without_stored_state() {
        let conn = conn();
        let parsed = serde_json::from_value::<StatementImportRequest>(json!({
            "path":"/vendor/file.csv","provider":"vendor","account_ref":"acct",
            "period_start":100,"period_end":200,"currency":"JPY"
        }));
        assert!(parsed.is_err());
        assert_eq!(statement_state(&conn), (0, 0, 0, 0, 0, 0));
    }

    #[test]
    fn absent_statement_currency_is_malformed_without_stored_state() {
        let conn = conn();
        let parsed = serde_json::from_value::<StatementImportRequest>(json!({
            "path":"/vendor/file.csv","provider":"vendor","account_ref":"acct",
            "period_start":100,"period_end":200,"amount_exponent":0
        }));
        assert!(parsed.is_err());
        assert_eq!(statement_state(&conn), (0, 0, 0, 0, 0, 0));
    }

    #[test]
    fn payload_supplied_operator_is_malformed_without_stored_state() {
        let conn = conn();
        let parsed = serde_json::from_value::<StatementImportRequest>(json!({
            "path":"/vendor/file.csv","provider":"vendor","account_ref":"acct",
            "period_start":100,"period_end":200,"amount_exponent":0,
            "currency":"JPY","operator":"forged-operator"
        }));
        assert!(parsed.is_err());
        assert_eq!(statement_state(&conn), (0, 0, 0, 0, 0, 0));
    }

    #[test]
    fn missing_vendor_line_id_refuses_whole_file_without_stored_state() {
        let mut conn = conn();
        let result = import_statement(
            &mut conn,
            &request(),
            &csv(",150,missing id,1,request,2,JPY,ref"),
            None,
            300,
        );
        assert!(matches!(result, Err(StatementError::Refused(_))));
        assert_eq!(statement_state(&conn), (0, 0, 0, 0, 0, 0));
    }

    #[test]
    fn malformed_numeric_line_refuses_whole_file_without_stored_state() {
        let mut conn = conn();
        let result = import_statement(
            &mut conn,
            &request(),
            &csv("line-1,150,bad quantity,1.5,request,2,JPY,ref"),
            None,
            300,
        );
        assert!(matches!(result, Err(StatementError::Refused(_))));
        assert_eq!(statement_state(&conn), (0, 0, 0, 0, 0, 0));
    }

    #[test]
    fn negative_statement_quantity_refuses_without_stored_state() {
        let mut conn = conn();
        let result = import_statement(
            &mut conn,
            &request(),
            &csv("line-1,150,negative,-1,request,2,JPY,ref"),
            None,
            300,
        );
        assert!(matches!(result, Err(StatementError::Refused(_))));
        assert_eq!(statement_state(&conn), (0, 0, 0, 0, 0, 0));
    }

    #[test]
    fn invalid_period_bounds_refuse_whole_file_without_stored_state() {
        let mut conn = conn();
        let mut invalid = request();
        invalid.period_start = invalid.period_end;
        let result = import_statement(&mut conn, &invalid, &standard_line("ref"), None, 300);
        assert!(matches!(
            result,
            Err(StatementError::Refused(StatementRefusal::InvalidPeriod))
        ));
        assert_eq!(statement_state(&conn), (0, 0, 0, 0, 0, 0));
    }

    #[test]
    fn line_outside_declared_period_refuses_whole_file_without_stored_state() {
        let mut conn = conn();
        let result = import_statement(
            &mut conn,
            &request(),
            &csv("line-1,200,outside,1,request,2,JPY,ref"),
            None,
            300,
        );
        assert!(matches!(
            result,
            Err(StatementError::Refused(
                StatementRefusal::LineOutsidePeriod { .. }
            ))
        ));
        assert_eq!(statement_state(&conn), (0, 0, 0, 0, 0, 0));
    }

    #[test]
    fn mixed_line_currency_refuses_whole_file_without_stored_state() {
        let mut conn = conn();
        let result = import_statement(
            &mut conn,
            &request(),
            &csv("line-1,150,wrong currency,1,request,2,USD,ref"),
            None,
            300,
        );
        assert!(matches!(
            result,
            Err(StatementError::Refused(
                StatementRefusal::StatementCurrencyMismatch { .. }
            ))
        ));
        assert_eq!(statement_state(&conn), (0, 0, 0, 0, 0, 0));
    }

    #[test]
    fn duplicate_vendor_line_id_refuses_whole_file_without_stored_state() {
        let mut conn = conn();
        let bytes = b"line_id,occurred_at,description,quantity,unit,amount_minor,currency\nline-1,150,a,1,request,2,JPY\nline-1,151,b,1,request,3,JPY\n";
        let result = import_statement(&mut conn, &request(), bytes, None, 300);
        assert!(matches!(result, Err(StatementError::Refused(_))));
        assert_eq!(statement_state(&conn), (0, 0, 0, 0, 0, 0));
    }

    #[test]
    fn amount_conversion_overflow_refuses_whole_file_without_stored_state() {
        let mut conn = conn();
        let bytes = csv(&format!(
            "line-1,150,overflow,1,request,{},JPY,ref",
            i64::MAX
        ));
        let result = import_statement(&mut conn, &request(), &bytes, None, 300);
        assert!(matches!(
            result,
            Err(StatementError::Refused(
                StatementRefusal::AmountOutOfRange { .. }
            ))
        ));
        assert_eq!(statement_state(&conn), (0, 0, 0, 0, 0, 0));
    }

    #[test]
    fn conflicting_normalised_content_refuses_without_changing_stored_state() {
        let mut conn = conn();
        import_statement(&mut conn, &request(), &standard_line("missing"), None, 300).unwrap();
        let before = statement_state(&conn);
        let result = import_statement(
            &mut conn,
            &request(),
            &csv("line-1,150,changed description,1,request,2,JPY,missing"),
            None,
            301,
        );
        assert!(matches!(
            result,
            Err(StatementError::Refused(
                StatementRefusal::ConflictingReimport { .. }
            ))
        ));
        assert_eq!(statement_state(&conn), before);
        let description: String = conn
            .query_row("SELECT description FROM statement_line", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(description, "vendor charge");
    }

    #[test]
    fn changed_amount_exponent_is_a_conflict_without_new_provenance() {
        let mut conn = conn();
        import_statement(&mut conn, &request(), &standard_line("missing"), None, 300).unwrap();
        let before = statement_state(&conn);
        let mut changed = request();
        changed.amount_exponent = 2;
        let result = import_statement(&mut conn, &changed, &standard_line("missing"), None, 301);
        assert!(matches!(
            result,
            Err(StatementError::Refused(
                StatementRefusal::ConflictingReimport { .. }
            ))
        ));
        assert_eq!(statement_state(&conn), before);
    }

    #[test]
    fn changed_declared_currency_is_a_conflict_without_new_provenance() {
        let mut conn = conn();
        import_statement(&mut conn, &request(), &standard_line("missing"), None, 300).unwrap();
        let before = statement_state(&conn);
        let mut changed = request();
        changed.currency = "USD".into();
        let result = import_statement(
            &mut conn,
            &changed,
            &csv("line-1,150,vendor charge,1,request,2,USD,missing"),
            None,
            301,
        );
        assert!(matches!(
            result,
            Err(StatementError::Refused(
                StatementRefusal::ConflictingReimport { .. }
            ))
        ));
        assert_eq!(statement_state(&conn), before);
    }

    #[test]
    fn byte_different_equal_content_is_no_op_with_per_invocation_digest() {
        let mut conn = conn();
        let first = standard_line("missing");
        let second = b"currency,amount_minor,unit,quantity,description,occurred_at,line_id,correlation_ref\r\nJPY,2,request,1,\"vendor charge\",1970-01-01T00:00:00.150Z,line-1,missing\r\n";
        let created = import_statement(&mut conn, &request(), &first, Some("alice"), 300).unwrap();
        let no_op = import_statement(&mut conn, &request(), second, Some("alice"), 301).unwrap();
        assert_eq!(created.content_outcome, StatementContentOutcome::Created);
        assert_eq!(created.lines_created, 1);
        assert_eq!(no_op.content_outcome, StatementContentOutcome::NoOp);
        assert_eq!(no_op.lines_skipped, 1);
        assert_eq!(statement_state(&conn), (1, 1, 2, 2, 0, 0));
        let distinct_digests: i64 = conn
            .query_row(
                "SELECT count(DISTINCT file_sha256) FROM statement_import",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(distinct_digests, 2);
    }

    #[test]
    fn nullable_transport_operator_is_recorded_as_null() {
        let mut conn = conn();
        import_statement(&mut conn, &request(), &standard_line("missing"), None, 300).unwrap();
        let operator: Option<String> = conn
            .query_row("SELECT operator FROM statement_import", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(operator, None);
    }

    #[test]
    fn jpy_exponent_zero_settlement_snapshots_both_amounts_and_writes_signed_adjustment() {
        let mut conn = conn();
        seed_event(
            &conn,
            "event-1",
            "producer-a",
            "corr",
            Some("JPY"),
            1_000_000_000,
        );
        let outcome =
            import_statement(&mut conn, &request(), &standard_line("corr"), None, 300).unwrap();
        assert_eq!(outcome.matching.settled_lines, 1);
        assert_eq!(
            line_outcome(&conn),
            ("settled".into(), Some("event-1".into()))
        );
        let attempt: (i64, i64) = conn
            .query_row(
                "SELECT vendor_amount_nanos,ledger_amount_nanos FROM match_attempt",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(attempt, (2_000_000_000, 1_000_000_000));
        let adjustment: (i64, String, String) = conn
            .query_row(
                "SELECT amount_nanos,origin,reason FROM adjustment",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(
            adjustment,
            (
                1_000_000_000,
                "statement".into(),
                "vendor_restatement".into()
            )
        );
        let authority: String = conn
            .query_row(
                "SELECT resolved_by FROM chargeability_transition WHERE event_key=(SELECT event_key FROM usage_event WHERE event_id='event-1')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(authority, "statement");
    }

    #[test]
    fn equal_vendor_and_ledger_amount_settles_with_zero_adjustments() {
        let mut conn = conn();
        seed_event(
            &conn,
            "event-1",
            "producer-a",
            "corr",
            Some("JPY"),
            2_000_000_000,
        );
        import_statement(&mut conn, &request(), &standard_line("corr"), None, 300).unwrap();
        assert_eq!(statement_state(&conn), (1, 1, 1, 1, 1, 0));
        assert_eq!(line_outcome(&conn).0, "settled");
    }

    #[test]
    fn cross_currency_candidate_records_mismatch_without_transition_or_adjustment() {
        let mut conn = conn();
        seed_event(
            &conn,
            "event-1",
            "producer-a",
            "corr",
            Some("EUR"),
            2_000_000_000,
        );
        import_statement(&mut conn, &request(), &standard_line("corr"), None, 300).unwrap();
        assert_eq!(
            line_outcome(&conn),
            ("currency_mismatch".into(), Some("event-1".into()))
        );
        assert_eq!(statement_state(&conn), (1, 1, 1, 1, 0, 0));
    }

    #[test]
    fn null_currency_candidate_records_unpriced_target_without_transition() {
        let mut conn = conn();
        seed_event(&conn, "event-1", "producer-a", "corr", None, 0);
        import_statement(&mut conn, &request(), &standard_line("corr"), None, 300).unwrap();
        assert_eq!(
            line_outcome(&conn),
            ("unpriced_target".into(), Some("event-1".into()))
        );
        assert_eq!(statement_state(&conn), (1, 1, 1, 1, 0, 0));
    }

    #[test]
    fn multiple_unresolved_candidates_record_ambiguity_with_zero_transitions() {
        let mut conn = conn();
        seed_event(&conn, "event-1", "producer-a", "corr", Some("JPY"), 1);
        seed_event(&conn, "event-2", "producer-b", "corr", Some("JPY"), 1);
        import_statement(&mut conn, &request(), &standard_line("corr"), None, 300).unwrap();
        assert_eq!(line_outcome(&conn), ("ambiguous".into(), None));
        assert_eq!(statement_state(&conn), (1, 1, 1, 1, 0, 0));
    }

    #[test]
    fn only_already_settled_correlations_record_already_settled_without_resettling() {
        let mut conn = conn();
        seed_event(&conn, "event-1", "producer-a", "corr", Some("JPY"), 1);
        settle_event(&conn, "event-1");
        let before_transitions = statement_state(&conn).4;
        import_statement(&mut conn, &request(), &standard_line("corr"), None, 300).unwrap();
        assert_eq!(
            line_outcome(&conn),
            ("already-settled".into(), Some("event-1".into()))
        );
        assert_eq!(statement_state(&conn).4, before_transitions);
    }

    #[test]
    fn absent_correlation_imports_unmatched_without_inferred_settlement() {
        let mut conn = conn();
        seed_event(
            &conn,
            "event-1",
            "producer-a",
            "nearby",
            Some("JPY"),
            2_000_000_000,
        );
        import_statement(
            &mut conn,
            &request(),
            &csv("line-1,150,no correlation,1,request,2,JPY,"),
            None,
            300,
        )
        .unwrap();
        assert_eq!(line_outcome(&conn), ("unmatched".into(), None));
        assert_eq!(statement_state(&conn).4, 0);
    }

    #[test]
    fn same_content_reimport_reconciles_unpriced_target_after_pricing() {
        let mut conn = conn();
        seed_event(&conn, "event-1", "producer-a", "corr", None, 0);
        let first =
            import_statement(&mut conn, &request(), &standard_line("corr"), None, 300).unwrap();
        assert_eq!(first.matching.unpriced_target_lines, 1);
        price_existing_event(&conn, "event-1", "JPY", 2_000_000_000);
        let second =
            import_statement(&mut conn, &request(), &standard_line("corr"), None, 301).unwrap();
        assert_eq!(second.content_outcome, StatementContentOutcome::NoOp);
        assert_eq!(second.matching.settled_lines, 1);
        assert_eq!(line_outcome(&conn).0, "settled");
        assert_eq!(statement_state(&conn), (1, 1, 2, 2, 1, 0));
        let outcomes: Vec<String> = conn
            .prepare("SELECT outcome FROM match_attempt ORDER BY attempted_at_ms")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(outcomes, ["unpriced_target", "settled"]);
    }

    #[test]
    fn same_content_reimport_preserves_once_per_event_ledger_attribution() {
        let mut conn = conn();
        seed_event(
            &conn,
            "event-1",
            "producer-a",
            "corr",
            Some("JPY"),
            2_000_000_000,
        );
        let bytes =
            csv("line-a,150,first,1,request,2,JPY,corr\nline-b,151,second,1,request,3,JPY,corr");
        let first = import_statement(&mut conn, &request(), &bytes, None, 300).unwrap();
        let read = |conn: &Connection| {
            crate::reconcile::run_reconciliation(
                conn,
                &crate::reconcile::ReconcileRequest {
                    statement_id: first.statement_id.clone(),
                },
            )
            .unwrap()
        };
        let initial = read(&conn);
        assert_eq!(
            initial
                .lines
                .iter()
                .filter_map(|line| line.ledger_amount_nanos)
                .sum::<i64>(),
            2_000_000_000
        );
        for time in [301, 302] {
            import_statement(&mut conn, &request(), &bytes, None, time).unwrap();
            let repeated = read(&conn);
            assert_eq!(
                repeated
                    .lines
                    .iter()
                    .filter_map(|line| line.ledger_amount_nanos)
                    .sum::<i64>(),
                2_000_000_000
            );
            assert_eq!(repeated.summary.difference, initial.summary.difference);
        }
        assert_eq!(statement_state(&conn), (1, 2, 3, 6, 1, 0));
    }

    #[test]
    fn reimport_preserves_pre_adjustment_snapshot_and_split_line_attribution() {
        let mut conn = conn();
        seed_event(
            &conn,
            "event-1",
            "producer-a",
            "corr",
            Some("JPY"),
            1_000_000_000,
        );
        let bytes =
            csv("line-a,150,first,1,request,2,JPY,corr\nline-b,151,second,1,request,3,JPY,corr");
        let first = import_statement(&mut conn, &request(), &bytes, None, 300).unwrap();
        for time in [300, 301, 302] {
            import_statement(&mut conn, &request(), &bytes, None, time).unwrap();
            let report = crate::reconcile::run_reconciliation(
                &conn,
                &crate::reconcile::ReconcileRequest {
                    statement_id: first.statement_id.clone(),
                },
            )
            .unwrap();
            assert_eq!(
                report
                    .lines
                    .iter()
                    .map(|line| line.ledger_amount_nanos)
                    .collect::<Vec<_>>(),
                [Some(1_000_000_000), Some(0)]
            );
            assert_eq!(
                report.summary.difference.difference_nanos,
                Some(4_000_000_000)
            );
        }
        let money: (i64, i64, Option<String>) = conn
            .query_row(
                "SELECT count(*),sum(amount_nanos),correction_id FROM adjustment",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(money, (1, 1_000_000_000, None));
        assert_eq!(statement_state(&conn).4, 1);
    }

    #[test]
    fn multiple_lines_for_previously_settled_event_count_ledger_once() {
        let mut conn = conn();
        seed_event(
            &conn,
            "event-1",
            "producer-a",
            "corr",
            Some("JPY"),
            2_000_000_000,
        );
        settle_event(&conn, "event-1");
        let bytes =
            csv("line-a,150,first,1,request,2,JPY,corr\nline-b,151,second,1,request,3,JPY,corr");
        let first = import_statement(&mut conn, &request(), &bytes, None, 300).unwrap();
        let report = crate::reconcile::run_reconciliation(
            &conn,
            &crate::reconcile::ReconcileRequest {
                statement_id: first.statement_id,
            },
        )
        .unwrap();
        assert_eq!(
            report
                .lines
                .iter()
                .map(|line| line.ledger_amount_nanos)
                .collect::<Vec<_>>(),
            [Some(2_000_000_000), Some(0)]
        );
        assert_eq!(statement_state(&conn), (1, 2, 1, 2, 1, 0));
    }

    #[test]
    fn normally_ingested_undated_event_does_not_block_statement_import() {
        let mut conn = conn();
        let event = serde_json::from_value::<crate::ingest::EventRecordRequest>(json!({
            "producer":"producer-a", "producer_ref":"corr", "provider":"vendor", "account_ref":"acct",
            "charge_basis":"metered_api", "measurements":[]
        })).unwrap();
        crate::ingest::record_event(&mut conn, &event, 250).unwrap();
        let result =
            import_statement(&mut conn, &request(), &standard_line("corr"), None, 300).unwrap();
        assert_eq!(result.matching.unpriced_target_lines, 1);
        assert_eq!(statement_state(&conn), (1, 1, 1, 1, 0, 0));
    }

    #[test]
    fn settled_currency_mismatch_never_reports_a_cross_currency_difference() {
        let mut conn = conn();
        seed_event(
            &conn,
            "event-1",
            "producer-a",
            "corr",
            Some("USD"),
            1_000_000_000,
        );
        settle_event(&conn, "event-1");
        let imported =
            import_statement(&mut conn, &request(), &standard_line("corr"), None, 300).unwrap();
        let report = crate::reconcile::run_reconciliation(
            &conn,
            &crate::reconcile::ReconcileRequest {
                statement_id: imported.statement_id,
            },
        )
        .unwrap();
        assert_eq!(report.lines.len(), 1);
        assert_eq!(
            report.lines[0].difference_nanos, None,
            "USD and JPY cannot be subtracted"
        );
        assert_eq!(
            report.lines[0].match_outcome,
            Some(MatchOutcome::CurrencyMismatch)
        );
        assert_eq!(report.lines[0].ledger_currency.as_deref(), Some("USD"));
        assert_eq!(report.lines[0].ledger_amount_nanos, Some(1_000_000_000));
        assert_eq!(report.summary.difference.compared_lines.lines, 0);
        assert_eq!(statement_state(&conn), (1, 1, 1, 1, 1, 0));
    }

    #[test]
    fn multiple_lines_for_one_candidate_attribute_ledger_amount_to_first_only() {
        let mut conn = conn();
        seed_event(
            &conn,
            "event-1",
            "producer-a",
            "corr",
            Some("JPY"),
            1_000_000_000,
        );
        let bytes = b"line_id,occurred_at,description,quantity,unit,amount_minor,currency,correlation_ref\nline-b,151,second,1,request,3,JPY,corr\nline-a,150,first,1,request,1,JPY,corr\n";
        import_statement(&mut conn, &request(), bytes, None, 300).unwrap();
        let attempts: Vec<(String, String, i64)> = conn
            .prepare(
                "SELECT line.line_id,attempt.outcome,attempt.ledger_amount_nanos
                 FROM match_attempt attempt
                 JOIN statement_line line USING(statement_line_id)
                 ORDER BY line.occurred_at_ms,line.line_id",
            )
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(
            attempts,
            [
                ("line-a".into(), "settled".into(), 1_000_000_000),
                ("line-b".into(), "already-settled".into(), 0),
            ]
        );
        assert_eq!(statement_state(&conn).4, 1);
    }

    #[test]
    fn import_failure_after_transition_write_rolls_back_transition_and_all_import_rows() {
        let mut conn = conn();
        seed_event(
            &conn,
            "event-1",
            "producer-a",
            "corr",
            Some("JPY"),
            2_000_000_000,
        );
        conn.execute_batch(
            "CREATE TRIGGER force_statement_import_failure
             BEFORE INSERT ON statement_import
             WHEN EXISTS (
                 SELECT 1 FROM chargeability_transition
                 WHERE event_key=(SELECT event_key FROM usage_event WHERE event_id='event-1') AND resolved_by='statement'
             )
             BEGIN
                 SELECT RAISE(ABORT, 'forced after transition');
             END;",
        )
        .unwrap();
        let result = import_statement(&mut conn, &request(), &standard_line("corr"), None, 300);
        assert!(matches!(result, Err(StatementError::Store(_))));
        assert_eq!(statement_state(&conn), (0, 0, 0, 0, 0, 0));
    }

    #[test]
    fn statement_content_and_history_are_immutable_without_effect() {
        let mut conn = conn();
        import_statement(&mut conn, &request(), &standard_line("missing"), None, 300).unwrap();
        let before = statement_state(&conn);
        assert!(conn
            .execute("UPDATE statement SET currency='USD'", [])
            .is_err());
        assert!(conn
            .execute("UPDATE statement_line SET description='changed'", [])
            .is_err());
        assert!(conn
            .execute("UPDATE match_attempt SET vendor_amount_nanos=0", [])
            .is_err());
        assert!(conn
            .execute("UPDATE statement_import SET operator='forged'", [])
            .is_err());
        assert!(conn.execute("DELETE FROM match_attempt", []).is_err());
        assert!(conn.execute("DELETE FROM statement_import", []).is_err());
        assert_eq!(statement_state(&conn), before);
        let stored_outcome: String = conn
            .query_row(
                "SELECT match_outcome FROM statement_line WHERE statement_id=?1 AND line_id=?2",
                params![
                    statement_id(&StatementIdentity {
                        provider: "vendor".into(),
                        account_ref: "acct".into(),
                        period_start_ms: 100,
                        period_end_ms: 200,
                    })
                    .unwrap(),
                    "line-1"
                ],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(stored_outcome, MatchOutcome::Unmatched.as_str());
    }

    #[test]
    fn absent_period_bound_is_malformed_without_stored_state() {
        let conn = conn();
        let parsed = serde_json::from_value::<StatementImportRequest>(json!({
            "path":"/vendor/file.csv","provider":"vendor","account_ref":"acct",
            "period_start":100,"amount_exponent":0,"currency":"JPY"
        }));
        assert!(parsed.is_err());
        assert_eq!(statement_state(&conn), (0, 0, 0, 0, 0, 0));
    }

    #[test]
    fn malformed_second_line_rolls_back_the_valid_first_line() {
        let mut conn = conn();
        let bytes = b"line_id,occurred_at,description,quantity,unit,amount_minor,currency\nline-1,150,valid,1,request,2,JPY\nline-2,151,invalid,not-an-integer,request,3,JPY\n";
        let result = import_statement(&mut conn, &request(), bytes, None, 300);
        assert!(matches!(result, Err(StatementError::Refused(_))));
        assert_eq!(statement_state(&conn), (0, 0, 0, 0, 0, 0));
    }

    #[test]
    fn unresolved_candidate_set_is_counted_before_already_settled_correlations() {
        let mut conn = conn();
        seed_event(&conn, "event-settled", "producer-a", "corr", Some("JPY"), 1);
        seed_event(
            &conn,
            "event-unresolved",
            "producer-b",
            "corr",
            Some("JPY"),
            2_000_000_000,
        );
        settle_event(&conn, "event-settled");
        import_statement(&mut conn, &request(), &standard_line("corr"), None, 300).unwrap();
        assert_eq!(
            line_outcome(&conn),
            ("settled".into(), Some("event-unresolved".into()))
        );
        let transitioned_settled_target: i64 = conn
            .query_row(
                "SELECT count(*) FROM chargeability_transition
                 WHERE event_key=(SELECT event_key FROM usage_event WHERE event_id='event-settled') AND resolved_by='statement'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(transitioned_settled_target, 0);
        assert_eq!(statement_state(&conn).4, 2);
    }

    #[test]
    fn correlation_is_scoped_to_import_provider_and_account_without_effect() {
        let mut conn = conn();
        conn.execute(
            "INSERT INTO usage_event (
                 event_id,producer,producer_lineage,producer_ref,occurred_at_ms,recorded_at_ms,
                 provider,account_ref,charge_basis,pricing_generation,currency
             ) VALUES ('wrong-scope','producer','lineage','corr',150,151,
                       'other-vendor','acct','metered_api',0,NULL)",
            [],
        )
        .unwrap();
        import_statement(&mut conn, &request(), &standard_line("corr"), None, 300).unwrap();
        assert_eq!(line_outcome(&conn), ("unmatched".into(), None));
        assert_eq!(statement_state(&conn).4, 0);
    }

    #[test]
    fn statement_line_period_and_currency_guards_reject_rows_without_effect() {
        let conn = conn();
        let identity = StatementIdentity {
            provider: "vendor".into(),
            account_ref: "acct".into(),
            period_start_ms: 100,
            period_end_ms: 200,
        };
        let id = statement_id(&identity).unwrap();
        conn.execute(
            "INSERT INTO statement (
                 statement_id,provider,account_ref,period_start_ms,period_end_ms,currency,amount_exponent
             ) VALUES (?1,'vendor','acct',100,200,'JPY',0)",
            [&id],
        )
        .unwrap();
        for (line_id, occurred_at, currency) in
            [("outside", 200, "JPY"), ("wrong-currency", 150, "USD")]
        {
            let line_hash = statement_line_id(&id, line_id).unwrap();
            assert!(conn
                .execute(
                    "INSERT INTO statement_line (
                         statement_line_id,statement_id,line_id,occurred_at_ms,description,
                         quantity,unit,amount_minor,currency,match_outcome
                     ) VALUES (?1,?2,?3,?4,'invalid',1,'request',1,?5,'unmatched')",
                    params![line_hash, id, line_id, occurred_at, currency],
                )
                .is_err());
        }
        let line_count: i64 = conn
            .query_row("SELECT count(*) FROM statement_line", [], |row| row.get(0))
            .unwrap();
        assert_eq!(line_count, 0);
    }

    #[test]
    fn match_attempt_snapshot_shape_rejects_missing_amounts_without_effect() {
        let mut conn = conn();
        let outcome =
            import_statement(&mut conn, &request(), &standard_line("missing"), None, 300).unwrap();
        let line_id: String = conn
            .query_row("SELECT statement_line_id FROM statement_line", [], |row| {
                row.get(0)
            })
            .unwrap();
        let before = statement_state(&conn);
        assert!(conn
            .execute(
                "INSERT INTO match_attempt (
                     attempt_id,statement_line_id,outcome,event_key,ledger_amount_nanos,
                     vendor_amount_nanos,attempted_at_ms
                 ) VALUES ('invalid',?1,'settled',NULL,NULL,1,301)",
                [line_id],
            )
            .is_err());
        assert_eq!(statement_state(&conn), before);
        assert_eq!(outcome.matching.unmatched_lines, 1);
    }

    #[test]
    fn statement_import_outcome_partition_rejects_false_created_count_without_effect() {
        let mut conn = conn();
        let outcome =
            import_statement(&mut conn, &request(), &standard_line("missing"), None, 300).unwrap();
        let before = statement_state(&conn);
        assert!(conn
            .execute(
                "INSERT INTO statement_import (
                     import_id,statement_id,file_sha256,source_path,imported_at_ms,outcome,
                     lines_created,lines_skipped,unmatched_lines,settled_lines,ambiguous_lines,
                     already_settled_lines,unpriced_target_lines,currency_mismatch_lines,
                     changed_outcomes
                 ) VALUES (
                     'invalid',?1,?2,'/invalid',301,'created',0,1,1,0,0,0,0,0,0
                 )",
                params![outcome.statement_id, "0".repeat(64)],
            )
            .is_err());
        assert_eq!(statement_state(&conn), before);
    }
}
