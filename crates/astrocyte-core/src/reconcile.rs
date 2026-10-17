//! Reconciles vendor statement lines against ledger amounts captured when each match was attempted;
//! later adjustments cannot rewrite the reported difference.

use rusqlite::{Connection, OptionalExtension};
use serde::{Deserialize, Serialize};

use crate::statement::{MatchOutcome, Statement};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReconcileRequest {
    pub statement_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct ReconciliationCount {
    pub lines: usize,
    pub of_statement_lines: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ReconciliationDifference {
    pub currency: String,
    pub difference_nanos: Option<i128>,
    pub compared_lines: ReconciliationCount,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ReconciliationSummary {
    pub unmatched: ReconciliationCount,
    pub settled: ReconciliationCount,
    pub ambiguous: ReconciliationCount,
    pub already_settled: ReconciliationCount,
    pub unpriced_target: ReconciliationCount,
    pub currency_mismatch: ReconciliationCount,
    pub unattempted: ReconciliationCount,
    pub difference: ReconciliationDifference,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ReconciliationLine {
    pub statement_line_id: String,
    pub line_id: String,
    pub attempt_id: Option<String>,
    pub attempted_at_ms: Option<i64>,
    pub match_outcome: Option<MatchOutcome>,
    pub event_id: Option<String>,
    pub vendor_amount_nanos: Option<i64>,
    pub vendor_currency: String,
    pub ledger_amount_nanos: Option<i64>,
    pub ledger_currency: Option<String>,
    pub difference_nanos: Option<i128>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ReconciliationRun {
    pub statement: Statement,
    pub lines: Vec<ReconciliationLine>,
    pub summary: ReconciliationSummary,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReconcileRefusal {
    Malformed { field: &'static str, detail: String },
    StatementNotFound { statement_id: String },
}

impl ReconcileRefusal {
    pub fn code(&self) -> &'static str {
        match self {
            Self::Malformed { .. } => "malformed",
            Self::StatementNotFound { .. } => "statement_not_found",
        }
    }

    pub fn message(&self) -> String {
        match self {
            Self::Malformed { field, detail } => format!("malformed {field}: {detail}"),
            Self::StatementNotFound { statement_id } => {
                format!("statement '{statement_id}' was not found")
            }
        }
    }
}

#[derive(Debug)]
pub enum ReconcileError {
    Refused(ReconcileRefusal),
    Store(rusqlite::Error),
}

impl ReconcileError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::Refused(refusal) => refusal.code(),
            Self::Store(_) => "store_error",
        }
    }

    pub fn message(&self) -> String {
        match self {
            Self::Refused(refusal) => refusal.message(),
            Self::Store(error) => format!("reconciliation store read: {error}"),
        }
    }
}

impl From<rusqlite::Error> for ReconcileError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Store(error)
    }
}

pub fn run_reconciliation(
    conn: &Connection,
    request: &ReconcileRequest,
) -> Result<ReconciliationRun, ReconcileError> {
    if request.statement_id.is_empty() {
        return Err(ReconcileError::Refused(ReconcileRefusal::Malformed {
            field: "statement_id",
            detail: "statement id is required".into(),
        }));
    }

    let statement = conn
        .query_row(
            "SELECT statement_id,provider,account_ref,period_start_ms,period_end_ms,
                    currency,amount_exponent
             FROM statement WHERE statement_id=?1",
            [&request.statement_id],
            |row| {
                Ok(Statement {
                    statement_id: row.get(0)?,
                    provider: row.get(1)?,
                    account_ref: row.get(2)?,
                    period_start_ms: row.get(3)?,
                    period_end_ms: row.get(4)?,
                    currency: row.get(5)?,
                    amount_exponent: row.get(6)?,
                })
            },
        )
        .optional()?
        .ok_or_else(|| {
            ReconcileError::Refused(ReconcileRefusal::StatementNotFound {
                statement_id: request.statement_id.clone(),
            })
        })?;

    let mut stmt = conn.prepare(
        "SELECT line.statement_line_id,line.line_id,
                attempt.attempt_id,attempt.attempted_at_ms,attempt.outcome,
                 (SELECT event_id FROM usage_event WHERE event_key=attempt.event_key),
                attempt.vendor_amount_nanos,attempt.ledger_amount_nanos,event.currency
         FROM statement_line AS line
         LEFT JOIN match_attempt AS attempt ON attempt.attempt_id = (
             SELECT latest.attempt_id
             FROM match_attempt AS latest
             WHERE latest.statement_line_id=line.statement_line_id
             -- Two imports can share one millisecond. The immutable unique id
             -- selects the same representative on every read when they tie.
             ORDER BY latest.attempted_at_ms DESC, latest.attempt_id DESC
             LIMIT 1
         )
         LEFT JOIN usage_event AS event ON event.event_key=attempt.event_key
         WHERE line.statement_id=?1
         ORDER BY line.occurred_at_ms,line.line_id",
    )?;
    let rows = stmt
        .query_map([&request.statement_id], |row| {
            let outcome = row
                .get::<_, Option<String>>(4)?
                .map(|value| parse_outcome(&value))
                .transpose()?;
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, Option<i64>>(3)?,
                outcome,
                row.get::<_, Option<String>>(5)?,
                row.get::<_, Option<i64>>(6)?,
                row.get::<_, Option<i64>>(7)?,
                row.get::<_, Option<String>>(8)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;

    let mut lines = Vec::with_capacity(rows.len());
    for (
        statement_line_id,
        line_id,
        attempt_id,
        attempted_at_ms,
        match_outcome,
        event_id,
        vendor_amount_nanos,
        ledger_amount_nanos,
        event_currency,
    ) in rows
    {
        let ledger_currency = match match_outcome {
            Some(MatchOutcome::UnpricedTarget) | None => None,
            Some(MatchOutcome::CurrencyMismatch) => event_currency,
            Some(_) if ledger_amount_nanos.is_some() => event_currency,
            Some(_) => None,
        };
        let difference_nanos = match (match_outcome, vendor_amount_nanos, ledger_amount_nanos) {
            (
                Some(MatchOutcome::Settled | MatchOutcome::AlreadySettled),
                Some(vendor),
                Some(ledger),
            ) => Some(i128::from(vendor) - i128::from(ledger)),
            _ => None,
        };
        lines.push(ReconciliationLine {
            statement_line_id,
            line_id,
            attempt_id,
            attempted_at_ms,
            match_outcome,
            event_id,
            vendor_amount_nanos,
            vendor_currency: statement.currency.clone(),
            ledger_amount_nanos,
            ledger_currency,
            difference_nanos,
        });
    }
    drop(stmt);

    let summary = summarize(&statement.currency, &lines);
    Ok(ReconciliationRun {
        statement,
        lines,
        summary,
    })
}

fn parse_outcome(value: &str) -> rusqlite::Result<MatchOutcome> {
    MatchOutcome::parse(value)
        .ok_or_else(|| crate::ingest::stored_value_error(4, "match_attempt.outcome", value))
}

fn summarize(currency: &str, lines: &[ReconciliationLine]) -> ReconciliationSummary {
    let population = lines.len();
    let count = |outcome| {
        counted(
            lines
                .iter()
                .filter(|line| line.match_outcome == Some(outcome))
                .count(),
            population,
        )
    };
    let difference_nanos = lines
        .iter()
        .filter_map(|line| line.difference_nanos)
        .reduce(|total, difference| total + difference);
    let compared_lines = counted(
        lines
            .iter()
            .filter(|line| line.difference_nanos.is_some())
            .count(),
        population,
    );
    ReconciliationSummary {
        unmatched: count(MatchOutcome::Unmatched),
        settled: count(MatchOutcome::Settled),
        ambiguous: count(MatchOutcome::Ambiguous),
        already_settled: count(MatchOutcome::AlreadySettled),
        unpriced_target: count(MatchOutcome::UnpricedTarget),
        currency_mismatch: count(MatchOutcome::CurrencyMismatch),
        unattempted: counted(
            lines
                .iter()
                .filter(|line| line.match_outcome.is_none())
                .count(),
            population,
        ),
        difference: ReconciliationDifference {
            currency: currency.into(),
            difference_nanos,
            compared_lines,
        },
    }
}

fn counted(lines: usize, population: usize) -> ReconciliationCount {
    ReconciliationCount {
        lines,
        of_statement_lines: population,
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn outcome_parser_uses_the_enum_vocabulary_and_names_corrupt_stored_data() {
        for outcome in [
            super::MatchOutcome::Unmatched,
            super::MatchOutcome::Settled,
            super::MatchOutcome::Ambiguous,
            super::MatchOutcome::AlreadySettled,
            super::MatchOutcome::UnpricedTarget,
            super::MatchOutcome::CurrencyMismatch,
        ] {
            assert_eq!(super::parse_outcome(outcome.as_str()).unwrap(), outcome);
        }
        let error = super::parse_outcome("unknown").unwrap_err();
        assert!(error
            .to_string()
            .contains("unrecognized stored match_attempt.outcome"));
    }

    use rusqlite::{params, Connection};
    use serde_json::json;

    use super::*;
    use crate::statement_import::{import_statement_in_tx, StatementImportRequest};
    use crate::store::{initialize_connection, MIGRATIONS};

    fn conn() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        initialize_connection(&conn).unwrap();
        for migration in MIGRATIONS {
            conn.execute_batch(migration.statements).unwrap();
        }
        conn
    }

    fn stored_state(conn: &Connection) -> (i64, i64, i64, i64, i64) {
        let count = |table: &str| {
            conn.query_row(&format!("SELECT count(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .unwrap()
        };
        (
            count("statement"),
            count("statement_line"),
            count("match_attempt"),
            count("adjustment"),
            count("chargeability_transition"),
        )
    }

    fn insert_event(conn: &Connection, event_id: &str, currency: Option<&str>) {
        conn.execute(
            "INSERT INTO usage_event (
                 event_id,producer,producer_lineage,producer_ref,occurred_at_ms,recorded_at_ms,
                 provider,account_ref,charge_basis,currency
             ) VALUES (?1,'producer',?1,?1,150,151,'vendor','acct','metered_api',NULL)",
            [event_id],
        )
        .unwrap();
        if let Some(currency) = currency {
            conn.execute(
                "UPDATE usage_event SET currency=?2 WHERE event_id=?1",
                params![event_id, currency],
            )
            .unwrap();
        }
    }

    fn insert_priced_event(conn: &Connection, event_id: &str, charge_nanos: i64) {
        insert_event(conn, event_id, Some("JPY"));
        conn.execute(
            "UPDATE usage_event SET pricing_generation=1 WHERE event_id=?1",
            [event_id],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO measurement (
                 event_key,role,unit,kind,source,quantity_units,quantity_exponent
             ) VALUES ((SELECT event_key FROM usage_event WHERE event_id=?1),'usage','request','count','vendor',1,0)",
            [event_id],
        )
        .unwrap();
        // Reconciliation consumes a ledger charge rather than a catalog rate, so
        // the fixture cannot establish whether a minimum applied.
        conn.execute(
            "INSERT INTO priced_charge (
                 charge_key,event_key,role,unit,pricing_generation,outcome,charge_basis,
                 basis_covered,currency,charge_nanos,unpriced_reason,rate_units,rate_exponent,
                  floor_state,floor_units,catalog_source,resolved_at_ms,catalog_entry,
                  supersedes,origin,recorded_at_ms
              ) VALUES (
                  NULL,(SELECT event_key FROM usage_event WHERE event_id=?1),'usage','request',1,'priced','metered_api',0,'JPY',?2,
                  NULL,1,9,'unknown',NULL,'test-catalog',152,'{}',NULL,'pricing_pass',152
             )",
            params![event_id, charge_nanos],
        )
        .unwrap();
    }

    fn insert_statement(conn: &Connection, statement_id: &str) {
        conn.execute(
            "INSERT INTO statement (
                 statement_id,provider,account_ref,period_start_ms,period_end_ms,currency,
                 amount_exponent
             ) VALUES (?1,'vendor','acct',100,200,'JPY',0)",
            [statement_id],
        )
        .unwrap();
    }

    fn insert_line(
        conn: &Connection,
        statement_id: &str,
        line_id: &str,
        outcome: &str,
        event_id: Option<&str>,
    ) {
        conn.execute(
            "INSERT INTO statement_line (
                 statement_line_id,statement_id,line_id,occurred_at_ms,description,quantity,unit,
                 amount_minor,currency,correlation_ref,match_outcome,matched_event_key
             ) VALUES (?1,?2,?1,150,?1,1,'request',10,'JPY',?1,?3,(SELECT event_key FROM usage_event WHERE event_id=?4))",
            params![line_id, statement_id, outcome, event_id],
        )
        .unwrap();
    }

    fn insert_attempt(
        conn: &Connection,
        attempt_id: &str,
        line_id: &str,
        outcome: &str,
        event_id: Option<&str>,
        ledger_amount_nanos: Option<i64>,
        attempted_at_ms: i64,
    ) {
        conn.execute(
            "INSERT INTO match_attempt (
                 attempt_id,statement_line_id,outcome,event_key,ledger_amount_nanos,
                 vendor_amount_nanos,attempted_at_ms
             ) VALUES (?1,?2,?3,(SELECT event_key FROM usage_event WHERE event_id=?4),?5,10,?6)",
            params![
                attempt_id,
                line_id,
                outcome,
                event_id,
                ledger_amount_nanos,
                attempted_at_ms
            ],
        )
        .unwrap();
    }

    #[test]
    fn settling_adjustment_preserves_original_snapshot_difference_in_reconciliation() {
        let mut conn = conn();
        insert_priced_event(&conn, "event-1", 1_000_000_000);
        let request = StatementImportRequest {
            path: "/vendor/jpy.csv".into(),
            provider: "vendor".into(),
            account_ref: "acct".into(),
            period_start: 100,
            period_end: 200,
            amount_exponent: 0,
            currency: "JPY".into(),
        };
        let bytes = b"line_id,occurred_at,description,quantity,unit,amount_minor,currency,correlation_ref\nline-1,150,vendor charge,1,request,2,JPY,event-1\n";
        let tx = conn.transaction().unwrap();
        let imported = import_statement_in_tx(&tx, &request, bytes, None, 300).unwrap();
        tx.commit().unwrap();
        let current_ledger: i64 = conn
            .query_row(
                "SELECT
                     (SELECT sum(charge_nanos) FROM priced_charge WHERE event_key=(SELECT event_key FROM usage_event WHERE event_id='event-1'))
                     + (SELECT sum(amount_nanos) FROM adjustment WHERE event_key=(SELECT event_key FROM usage_event WHERE event_id='event-1'))",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(current_ledger, 2_000_000_000);

        let run = run_reconciliation(
            &conn,
            &ReconcileRequest {
                statement_id: imported.statement_id,
            },
        )
        .unwrap();

        assert_eq!(run.lines[0].vendor_amount_nanos, Some(2_000_000_000));
        assert_eq!(run.lines[0].ledger_amount_nanos, Some(1_000_000_000));
        assert_eq!(run.lines[0].difference_nanos, Some(1_000_000_000));
        assert_eq!(run.summary.difference.difference_nanos, Some(1_000_000_000));
        assert_eq!(
            run.summary.difference.compared_lines,
            ReconciliationCount {
                lines: 1,
                of_statement_lines: 1
            }
        );
    }

    #[test]
    fn latest_attempt_tie_break_and_outcome_counts_partition_statement_population() {
        let conn = conn();
        insert_statement(&conn, "statement-1");
        insert_event(&conn, "event-settled", Some("JPY"));
        insert_event(&conn, "event-already", Some("JPY"));
        insert_event(&conn, "event-unpriced", None);
        insert_event(&conn, "event-mismatch", Some("USD"));

        insert_line(
            &conn,
            "statement-1",
            "line-settled",
            "settled",
            Some("event-settled"),
        );
        insert_line(&conn, "statement-1", "line-ambiguous", "ambiguous", None);
        insert_line(
            &conn,
            "statement-1",
            "line-already",
            "already-settled",
            Some("event-already"),
        );
        insert_line(
            &conn,
            "statement-1",
            "line-unpriced",
            "unpriced_target",
            Some("event-unpriced"),
        );
        insert_line(
            &conn,
            "statement-1",
            "line-mismatch",
            "currency_mismatch",
            Some("event-mismatch"),
        );
        insert_line(&conn, "statement-1", "line-unmatched", "unmatched", None);
        insert_line(&conn, "statement-1", "line-unattempted", "unmatched", None);

        insert_attempt(&conn, "a-old", "line-settled", "unmatched", None, None, 300);
        insert_attempt(
            &conn,
            "z-new",
            "line-settled",
            "settled",
            Some("event-settled"),
            Some(2),
            300,
        );
        insert_attempt(
            &conn,
            "ambiguous",
            "line-ambiguous",
            "ambiguous",
            None,
            None,
            300,
        );
        insert_attempt(
            &conn,
            "already",
            "line-already",
            "already-settled",
            Some("event-already"),
            Some(0),
            300,
        );
        insert_attempt(
            &conn,
            "unpriced",
            "line-unpriced",
            "unpriced_target",
            Some("event-unpriced"),
            Some(0),
            300,
        );
        insert_attempt(
            &conn,
            "mismatch",
            "line-mismatch",
            "currency_mismatch",
            Some("event-mismatch"),
            Some(4),
            300,
        );
        insert_attempt(
            &conn,
            "unmatched",
            "line-unmatched",
            "unmatched",
            None,
            None,
            300,
        );

        let run = run_reconciliation(
            &conn,
            &ReconcileRequest {
                statement_id: "statement-1".into(),
            },
        )
        .unwrap();
        let summary = &run.summary;
        for count in [
            summary.unmatched,
            summary.settled,
            summary.ambiguous,
            summary.already_settled,
            summary.unpriced_target,
            summary.currency_mismatch,
            summary.unattempted,
        ] {
            assert_eq!(count.lines, 1);
            assert_eq!(count.of_statement_lines, 7);
        }
        let partition = summary.unmatched.lines
            + summary.settled.lines
            + summary.ambiguous.lines
            + summary.already_settled.lines
            + summary.unpriced_target.lines
            + summary.currency_mismatch.lines
            + summary.unattempted.lines;
        assert_eq!(partition, 7);
        let settled = run
            .lines
            .iter()
            .find(|line| line.line_id == "line-settled")
            .unwrap();
        assert_eq!(settled.attempt_id.as_deref(), Some("z-new"));
        assert_eq!(summary.difference.difference_nanos, Some(18));
        assert_eq!(summary.difference.compared_lines.lines, 2);
        assert_eq!(summary.difference.compared_lines.of_statement_lines, 7);
    }

    #[test]
    fn currency_mismatch_and_unpriced_target_report_absent_differences() {
        let conn = conn();
        insert_statement(&conn, "statement-1");
        insert_event(&conn, "event-unpriced", None);
        insert_event(&conn, "event-mismatch", Some("USD"));
        insert_line(
            &conn,
            "statement-1",
            "line-unpriced",
            "unpriced_target",
            Some("event-unpriced"),
        );
        insert_line(
            &conn,
            "statement-1",
            "line-mismatch",
            "currency_mismatch",
            Some("event-mismatch"),
        );
        insert_attempt(
            &conn,
            "unpriced",
            "line-unpriced",
            "unpriced_target",
            Some("event-unpriced"),
            Some(0),
            300,
        );
        insert_attempt(
            &conn,
            "mismatch",
            "line-mismatch",
            "currency_mismatch",
            Some("event-mismatch"),
            Some(10),
            300,
        );

        let run = run_reconciliation(
            &conn,
            &ReconcileRequest {
                statement_id: "statement-1".into(),
            },
        )
        .unwrap();
        let unpriced = run
            .lines
            .iter()
            .find(|line| line.line_id == "line-unpriced")
            .unwrap();
        assert_eq!(unpriced.ledger_currency, None);
        assert_eq!(unpriced.difference_nanos, None);
        let mismatch = run
            .lines
            .iter()
            .find(|line| line.line_id == "line-mismatch")
            .unwrap();
        assert_eq!(mismatch.ledger_currency.as_deref(), Some("USD"));
        assert_eq!(mismatch.difference_nanos, None);
        assert_eq!(run.summary.difference.difference_nanos, None);
        assert_eq!(run.summary.difference.compared_lines.lines, 0);
        assert_eq!(run.summary.difference.compared_lines.of_statement_lines, 2);
    }

    #[test]
    fn missing_statement_id_is_malformed_without_stored_state() {
        let conn = conn();
        let before = stored_state(&conn);
        let parsed = serde_json::from_value::<ReconcileRequest>(json!({}));
        assert!(parsed.is_err());
        assert_eq!(stored_state(&conn), before);
    }

    #[test]
    fn payload_supplied_operator_is_malformed_without_stored_state() {
        let conn = conn();
        let before = stored_state(&conn);
        let parsed = serde_json::from_value::<ReconcileRequest>(json!({
            "statement_id": "statement-1",
            "operator": "forged"
        }));
        assert!(parsed.is_err());
        assert_eq!(stored_state(&conn), before);
    }

    #[test]
    fn empty_statement_id_refuses_without_stored_state() {
        let conn = conn();
        let before = stored_state(&conn);
        let result = run_reconciliation(
            &conn,
            &ReconcileRequest {
                statement_id: String::new(),
            },
        );
        assert!(matches!(
            result,
            Err(ReconcileError::Refused(ReconcileRefusal::Malformed {
                field: "statement_id",
                ..
            }))
        ));
        assert_eq!(stored_state(&conn), before);
    }

    #[test]
    fn unknown_statement_refuses_without_stored_state() {
        let conn = conn();
        insert_statement(&conn, "known");
        let before = stored_state(&conn);
        let result = run_reconciliation(
            &conn,
            &ReconcileRequest {
                statement_id: "missing".into(),
            },
        );
        assert!(matches!(
            result,
            Err(ReconcileError::Refused(
                ReconcileRefusal::StatementNotFound { statement_id }
            )) if statement_id == "missing"
        ));
        assert_eq!(stored_state(&conn), before);
    }
}
