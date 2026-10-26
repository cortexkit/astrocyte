//! Machine-scoped budget verdict read model.

use std::collections::BTreeMap;

use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VerdictRequest {
    #[serde(default)]
    pub project_root: Option<String>,
    #[serde(default)]
    pub provider: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub est_cost_nanos: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum VerdictDecision {
    Allow,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct VerdictUncertainty {
    pub unpriced_events: u64,
    pub events_total: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct VerdictResponse {
    pub decision: VerdictDecision,
    /// Recorded current-head amounts and signed adjustments, keyed by currency.
    pub consumed_nanos: BTreeMap<String, i64>,
    /// The caller's estimate is echoed without treating it as recorded spend.
    pub est_cost_nanos: Option<i64>,
    pub limit_nanos: Option<i64>,
    pub remaining_nanos: Option<i64>,
    pub uncertainty: VerdictUncertainty,
    pub uncertainty_scope: &'static str,
}

#[derive(Debug)]
pub enum VerdictError {
    Store(rusqlite::Error),
    Arithmetic,
}
impl From<rusqlite::Error> for VerdictError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Store(error)
    }
}

/// Reports the machine-wide priced population and explicitly labels its uncertainty.
/// Cap enforcement belongs to the future spender-local integration, so no local cap
/// configuration is read or synthesized here.
/// The caller must hold one read transaction so the event and charge reads share
/// a snapshot; served calls use the report connection to avoid blocking ingest.
pub fn evaluate(
    conn: &Connection,
    request: &VerdictRequest,
) -> Result<VerdictResponse, VerdictError> {
    if let Some(verdict) = crate::report::summary_verdict(conn, request) {
        return Ok(verdict);
    }
    evaluate_base(conn, request)
}

pub(crate) fn evaluate_base(
    conn: &Connection,
    request: &VerdictRequest,
) -> Result<VerdictResponse, VerdictError> {
    // Monetary rows are unioned, not joined to each other: multiple heads and
    // adjustments on one event must not multiply either contribution.
    // Split signed amounts into base-2^32 limbs before summing so an intermediate
    // total above i64 can still be cancelled by refunds, then narrow only once.
    const LIMB_BASE: i128 = 4_294_967_296;
    let mut statement = conn.prepare(
        "WITH events AS MATERIALIZED (
            SELECT event_key,currency FROM usage_event
            WHERE (?1 IS NULL OR project_root=?1)
              AND (?2 IS NULL OR provider=?2)
              AND (?3 IS NULL OR model=?3)
        ), heads AS MATERIALIZED (
            SELECT head.event_key,head.currency,head.charge_nanos,head.outcome
            FROM priced_charge head JOIN events ON events.event_key=head.event_key
            WHERE NOT EXISTS (
                SELECT 1 FROM priced_charge child WHERE child.supersedes=head.charge_key
            )
        ), head_state AS (
            SELECT event_key,MAX(outcome='unpriced') AS unpriced
            FROM heads GROUP BY event_key
        ), contributions AS (
            SELECT events.currency,NULL AS amount_nanos,1 AS events_total,
                   CASE WHEN head_state.event_key IS NULL OR head_state.unpriced=1
                        THEN 1 ELSE 0 END AS unpriced_events
            FROM events LEFT JOIN head_state ON head_state.event_key=events.event_key
            UNION ALL
            SELECT currency,charge_nanos,0,0 FROM heads WHERE charge_nanos IS NOT NULL
            UNION ALL
            SELECT adjustment.currency,adjustment.amount_nanos,0,0
            FROM adjustment JOIN events ON events.event_key=adjustment.event_key
        )
        SELECT currency,SUM(amount_nanos / 4294967296),SUM(amount_nanos % 4294967296),
               SUM(events_total),SUM(unpriced_events)
        FROM contributions GROUP BY currency",
    )?;
    let rows = statement.query_map(
        params![request.project_root, request.provider, request.model],
        |row| {
            Ok((
                row.get::<_, Option<String>>(0)?,
                row.get::<_, Option<i64>>(1)?,
                row.get::<_, Option<i64>>(2)?,
                row.get::<_, u64>(3)?,
                row.get::<_, u64>(4)?,
            ))
        },
    )?;
    let mut consumed = BTreeMap::new();
    let mut total = 0_u64;
    let mut unpriced = 0_u64;
    for row in rows {
        let (currency, high, low, events_total, unpriced_events) = row?;
        total = total
            .checked_add(events_total)
            .ok_or(VerdictError::Arithmetic)?;
        unpriced = unpriced
            .checked_add(unpriced_events)
            .ok_or(VerdictError::Arithmetic)?;
        if let (Some(high), Some(low)) = (high, low) {
            let amount = i128::from(high)
                .checked_mul(LIMB_BASE)
                .and_then(|value| value.checked_add(i128::from(low)))
                .and_then(|value| i64::try_from(value).ok())
                .ok_or(VerdictError::Arithmetic)?;
            consumed.insert(currency.ok_or(VerdictError::Arithmetic)?, amount);
        }
    }
    Ok(VerdictResponse {
        decision: VerdictDecision::Allow,
        consumed_nanos: consumed,
        est_cost_nanos: request.est_cost_nanos,
        limit_nanos: None,
        remaining_nanos: None,
        uncertainty: VerdictUncertainty {
            unpriced_events: unpriced,
            events_total: total,
        },
        uncertainty_scope: "machine",
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::{initialize_connection, MIGRATIONS};
    use crate::test_support::conn;

    fn request() -> VerdictRequest {
        VerdictRequest {
            project_root: None,
            provider: None,
            model: None,
            est_cost_nanos: None,
        }
    }

    fn event(conn: &Connection, id: &str) {
        conn.execute(
            "INSERT INTO usage_event (event_id,producer,producer_lineage,producer_ref,
                recorded_at_ms,provider,model,project_root,charge_basis)
             VALUES (?1,'producer','lineage',?1,1,'provider','model','/project','metered_api')",
            [id],
        )
        .unwrap();
    }

    fn head(conn: &Connection, id: &str, role: &str, currency: &str, amount: i64) {
        conn.execute(
            "INSERT INTO measurement (event_key,role,unit,kind,source,quantity_units,quantity_exponent)
             VALUES ((SELECT event_key FROM usage_event WHERE event_id=?1),?2,'token','billed','producer',1,0)",
            params![id, role],
        )
        .unwrap();
        conn.execute(
            "UPDATE usage_event SET pricing_generation=1,currency=?2 WHERE event_id=?1",
            params![id, currency],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO priced_charge (charge_key,event_key,role,unit,pricing_generation,
                outcome,charge_basis,basis_covered,currency,charge_nanos,rate_units,
                rate_exponent,floor_state,catalog_source,resolved_at_ms,catalog_entry,
                origin,recorded_at_ms)
             VALUES (?1,(SELECT event_key FROM usage_event WHERE event_id=?2),?3,'token',1,'priced','metered_api',0,?4,?5,1,9,
                'unknown','fusiform',100,'{}','pricing_pass',100)",
            params![rusqlite::types::Null, id, role, currency, amount],
        )
        .unwrap();
    }

    fn adjustment(conn: &Connection, id: &str, suffix: &str, currency: &str, amount: i64) {
        conn.execute(
            "INSERT INTO adjustment (adjustment_id,event_key,currency,amount_nanos,reason,
                occurred_at_ms,recorded_at_ms,origin)
             VALUES (?1,(SELECT event_key FROM usage_event WHERE event_id=?2),?3,?4,'refund',100,100,'operator')",
            params![format!("{id}-{suffix}"), id, currency, amount],
        )
        .unwrap();
    }

    #[test]
    fn estimate_is_not_recorded_consumption() {
        let conn = conn();
        let empty = evaluate(
            &conn,
            &VerdictRequest {
                est_cost_nanos: Some(123),
                ..request()
            },
        )
        .unwrap();
        assert_eq!(empty.decision, VerdictDecision::Allow);
        assert!(empty.consumed_nanos.is_empty());
        assert_eq!(empty.est_cost_nanos, Some(123));
        event(&conn, "spent");
        head(&conn, "spent", "input", "USD", 100);
        for estimate in [
            None,
            Some(0),
            Some(123),
            Some(-123),
            Some(i64::MIN),
            Some(i64::MAX),
        ] {
            let verdict = serde_json::to_value(
                evaluate(
                    &conn,
                    &VerdictRequest {
                        est_cost_nanos: estimate,
                        ..request()
                    },
                )
                .unwrap(),
            )
            .unwrap();
            assert_eq!(verdict["decision"], "allow");
            assert_eq!(verdict["consumed_nanos"], serde_json::json!({"USD": 100}));
            assert_eq!(verdict["est_cost_nanos"], serde_json::json!(estimate));
        }
    }

    #[test]
    fn adjustments_change_recorded_consumption_without_join_fanout() {
        let conn = conn();
        event(&conn, "adjusted");
        head(&conn, "adjusted", "input", "USD", 100);
        head(&conn, "adjusted", "output", "USD", 200);
        adjustment(&conn, "adjusted", "refund", "USD", -40);
        adjustment(&conn, "adjusted", "restatement", "USD", 10);
        let verdict = evaluate(&conn, &request()).unwrap();
        assert_eq!(verdict.decision, VerdictDecision::Allow);
        assert_eq!(verdict.uncertainty.events_total, 1);
        assert_eq!(
            verdict.consumed_nanos,
            BTreeMap::from([("USD".into(), 270)])
        );
    }

    #[test]
    fn two_currencies_are_reported_separately() {
        let conn = conn();
        event(&conn, "usd");
        head(&conn, "usd", "input", "USD", 100);
        adjustment(&conn, "usd", "refund", "USD", -40);
        event(&conn, "jpy");
        head(&conn, "jpy", "input", "JPY", 200);
        let verdict = serde_json::to_value(evaluate(&conn, &request()).unwrap()).unwrap();
        assert_eq!(verdict["decision"], "allow");
        assert_eq!(
            verdict["consumed_nanos"],
            serde_json::json!({"USD": 60, "JPY": 200})
        );
    }

    #[test]
    fn consumption_uses_only_current_heads_and_counts_unpriced_events_once() {
        let conn = conn();
        event(&conn, "repriced");
        head(&conn, "repriced", "input", "USD", 100);
        conn.execute(
            "UPDATE usage_event SET pricing_generation=2 WHERE event_id='repriced'",
            [],
        )
        .unwrap();
        conn.execute_batch(
            "INSERT INTO priced_charge (charge_key,event_key,role,unit,pricing_generation,
                outcome,charge_basis,basis_covered,currency,charge_nanos,rate_units,
                rate_exponent,floor_state,catalog_source,resolved_at_ms,catalog_entry,
                supersedes,origin,recorded_at_ms)
             VALUES (2,(SELECT event_key FROM usage_event WHERE event_id='repriced'),'input','token',2,'priced','metered_api',0,
                'USD',150,1,9,'unknown','fusiform',100,'{}',1,'operator_reprice',100);
             INSERT INTO measurement (event_key,role,unit,kind,source,quantity_units,quantity_exponent)
             VALUES ((SELECT event_key FROM usage_event WHERE event_id='repriced'),'output','token','billed','producer',1,0);
             INSERT INTO priced_charge (charge_key,event_key,role,unit,pricing_generation,
                outcome,charge_basis,basis_covered,unpriced_reason,origin,recorded_at_ms)
             VALUES (3,(SELECT event_key FROM usage_event WHERE event_id='repriced'),'output','token',2,'unpriced','metered_api',0,
                'no_rate','pricing_pass',100);"
        ).unwrap();
        adjustment(&conn, "repriced", "refund", "USD", -40);
        event(&conn, "pending");
        let verdict = evaluate(&conn, &request()).unwrap();
        assert_eq!(verdict.decision, VerdictDecision::Allow);
        assert_eq!(
            verdict.consumed_nanos,
            BTreeMap::from([("USD".into(), 110)])
        );
        assert_eq!(verdict.uncertainty.events_total, 2);
        assert_eq!(verdict.uncertainty.unpriced_events, 2);
    }

    #[test]
    fn filters_select_the_same_events_for_heads_and_adjustments() {
        let conn = conn();
        event(&conn, "matched");
        head(&conn, "matched", "input", "USD", 100);
        adjustment(&conn, "matched", "refund", "USD", -40);
        conn.execute_batch(
            "INSERT INTO usage_event (event_id,producer,producer_lineage,producer_ref,
                recorded_at_ms,provider,model,project_root,charge_basis)
             VALUES ('other-project','producer','lineage','other-project',1,'provider','model','/other','metered_api'),
                    ('other-provider','producer','lineage','other-provider',1,'other','model','/project','metered_api'),
                    ('other-model','producer','lineage','other-model',1,'provider','other','/project','metered_api'),
                    ('missing','producer','lineage','missing',1,NULL,NULL,NULL,'metered_api');"
        ).unwrap();
        for id in ["other-project", "other-provider", "other-model", "missing"] {
            head(&conn, id, "input", "JPY", 200);
            adjustment(&conn, id, "refund", "JPY", -20);
        }
        let verdict = evaluate(
            &conn,
            &VerdictRequest {
                project_root: Some("/project".into()),
                provider: Some("provider".into()),
                model: Some("model".into()),
                est_cost_nanos: None,
            },
        )
        .unwrap();
        assert_eq!(verdict.decision, VerdictDecision::Allow);
        assert_eq!(verdict.consumed_nanos, BTreeMap::from([("USD".into(), 60)]));
        assert_eq!(verdict.uncertainty.events_total, 1);
        assert_eq!(verdict.uncertainty.unpriced_events, 0);
        let empty = evaluate(
            &conn,
            &VerdictRequest {
                provider: Some("absent".into()),
                ..request()
            },
        )
        .unwrap();
        assert_eq!(empty.decision, VerdictDecision::Allow);
        assert!(empty.consumed_nanos.is_empty());
        assert_eq!(empty.uncertainty.events_total, 0);
    }

    #[test]
    fn signed_consumption_is_checked_after_refunds_cancel_large_heads() {
        let conn = conn();
        event(&conn, "large");
        head(&conn, "large", "input", "USD", i64::MAX);
        head(&conn, "large", "output", "USD", i64::MAX);
        adjustment(&conn, "large", "refund", "USD", -i64::MAX);
        let verdict = evaluate(&conn, &request()).unwrap();
        assert_eq!(verdict.decision, VerdictDecision::Allow);
        assert_eq!(
            verdict.consumed_nanos,
            BTreeMap::from([("USD".into(), i64::MAX)])
        );
        adjustment(&conn, "large", "overflow", "USD", 1);
        assert!(matches!(
            evaluate(&conn, &request()),
            Err(VerdictError::Arithmetic)
        ));

        let conn = self::conn();
        event(&conn, "negative");
        head(&conn, "negative", "input", "USD", 0);
        adjustment(&conn, "negative", "refund", "USD", i64::MIN);
        let verdict = evaluate(&conn, &request()).unwrap();
        assert_eq!(verdict.decision, VerdictDecision::Allow);
        assert_eq!(
            verdict.consumed_nanos,
            BTreeMap::from([("USD".into(), i64::MIN)])
        );
    }

    #[test]
    fn covered_zero_and_adjustment_only_spend_keep_their_currencies() {
        let conn = conn();
        event(&conn, "refunded");
        head(&conn, "refunded", "input", "USD", 100);
        conn.execute_batch(
            "UPDATE usage_event SET pricing_generation=2 WHERE event_id='refunded';
             INSERT INTO priced_charge (charge_key,event_key,role,unit,pricing_generation,
                outcome,charge_basis,basis_covered,unpriced_reason,supersedes,origin,recorded_at_ms)
             VALUES (2,(SELECT event_key FROM usage_event WHERE event_id='refunded'),'input','token',2,'unpriced','metered_api',0,
                'no_rate',1,'operator_reprice',100);
             INSERT INTO usage_event (event_id,producer,producer_lineage,producer_ref,
                recorded_at_ms,charge_basis)
             VALUES ('covered','producer','lineage','covered',1,'subscription_included');
             INSERT INTO measurement (event_key,role,unit,kind,source,quantity_units,quantity_exponent)
             VALUES ((SELECT event_key FROM usage_event WHERE event_id='covered'),'input','token','billed','producer',1,0);
             UPDATE usage_event SET pricing_generation=1,currency='JPY' WHERE event_id='covered';
             INSERT INTO priced_charge (charge_key,event_key,role,unit,pricing_generation,
                outcome,charge_basis,basis_covered,currency,charge_nanos,origin,recorded_at_ms)
             VALUES (3,(SELECT event_key FROM usage_event WHERE event_id='covered'),'input','token',1,'covered','subscription_included',1,
                'JPY',0,'pricing_pass',100);"
        ).unwrap();
        adjustment(&conn, "refunded", "refund", "USD", -40);
        let verdict = evaluate(&conn, &request()).unwrap();
        assert_eq!(verdict.decision, VerdictDecision::Allow);
        assert_eq!(
            verdict.consumed_nanos,
            BTreeMap::from([("USD".into(), -40), ("JPY".into(), 0)])
        );
        assert_eq!(verdict.uncertainty.events_total, 2);
        assert_eq!(verdict.uncertainty.unpriced_events, 1);
    }

    #[test]
    #[ignore = "100,000-event wall-time fixture; run explicitly with --ignored --nocapture"]
    fn verdict_cost_on_100_000_recorded_events() {
        let conn = conn();
        let write = conn.unchecked_transaction().unwrap();
        for ordinal in 0..100_000 {
            let id = format!("event-{ordinal}");
            event(&write, &id);
            head(&write, &id, "input", "USD", 100);
            adjustment(&write, &id, "refund", "USD", -40);
        }
        write.commit().unwrap();
        let read = conn.unchecked_transaction().unwrap();
        for iteration in 1..=3 {
            let start = std::time::Instant::now();
            let verdict = evaluate(&read, &request()).unwrap();
            let elapsed = start.elapsed();
            assert_eq!(verdict.decision, VerdictDecision::Allow);
            assert_eq!(verdict.uncertainty.events_total, 100_000);
            assert_eq!(verdict.uncertainty.unpriced_events, 0);
            assert_eq!(
                verdict.consumed_nanos,
                BTreeMap::from([("USD".into(), 6_000_000)])
            );
            println!("100000 events, 100000 heads, 100000 adjustments; iteration {iteration}: {elapsed:?}; consumed={:?}", verdict.consumed_nanos);
        }
    }

    #[test]
    fn verdict_labels_machine_scoped_uncertainty_without_cap_state() {
        let conn = Connection::open_in_memory().unwrap();
        initialize_connection(&conn).unwrap();
        for migration in MIGRATIONS {
            conn.execute_batch(migration.statements).unwrap();
        }
        let verdict = evaluate(
            &conn,
            &VerdictRequest {
                project_root: None,
                provider: None,
                model: None,
                est_cost_nanos: None,
            },
        )
        .unwrap();
        assert_eq!(verdict.decision, VerdictDecision::Allow);
        assert_eq!(verdict.uncertainty_scope, "machine");
        assert_eq!(verdict.limit_nanos, None);
    }
}
