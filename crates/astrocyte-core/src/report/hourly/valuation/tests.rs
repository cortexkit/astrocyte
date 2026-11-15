use super::*;
use crate::ingest;
use crate::multiplier::{QuotaProviderConfig, WindowSelector};
use crate::rate_era::{fetched, publish};
use crate::subscription::PlanLedger;
use crate::test_support::conn;
use serde_json::json;

fn config() -> MultiplierConfig {
    MultiplierConfig {
        api_twins: BTreeMap::new(),
        plan_price_overrides: BTreeMap::new(),
        quota_providers: BTreeMap::from([(
            "pool".into(),
            QuotaProviderConfig {
                window: WindowSelector {
                    pool_key: "primary".into(),
                    window_minutes: 10080,
                },
                scope: vec![ScopeRule {
                    producer: "opencode".into(),
                    provider: "provider".into(),
                    charge_basis: None,
                    model_prefix: None,
                }],
            },
        )]),
    }
}

fn record_id(ledger: PlanLedger, identity: &str, effective_from_ms: i64) -> PlanRecordId {
    PlanRecordId {
        ledger,
        provider: "provider".into(),
        identity: identity.into(),
        effective_from_ms,
    }
}

fn rate(conn: &Connection, model: &str, units: i64) {
    let history = serde_json::from_value(json!({
        "source":"synthetic", "provider_id":"provider", "model_id":model,
        "fact_prefix":"rate.", "eras":[], "served_by_key":{
            "rate.input":[{"from_ms":0,"until_ms":null,"state":"value","value":{"state":"priced","units":units,"exponent":9,"currency":"USD","unit_provenance":{"kind":"stated"}}}],
            "rate.output":[{"from_ms":0,"until_ms":null,"state":"value","value":{"state":"priced","units":units*2,"exponent":9,"currency":"USD","unit_provenance":{"kind":"stated"}}}]
        }
    })).unwrap();
    publish(conn, &fetched(history, "provider", model, None).unwrap(), 1).unwrap();
}

#[allow(clippy::too_many_arguments)]
fn event(
    conn: &mut Connection,
    id: &str,
    at: i64,
    model: &str,
    account: Option<&str>,
    credential: Option<&str>,
    basis: &str,
    quantity: i64,
) -> i64 {
    let mut request: ingest::EventRecordRequest = serde_json::from_value(json!({
        "producer":"opencode","producer_ref":id,"provider":"provider","model":model,
        "account_ref":account,"auth_selection":"selected",
        "occurred_at_ms":at,"charge_basis":basis,
        "measurements":[{"role":"input","unit":"token","kind":"billed","source":"fixture","quantity_units":quantity,"quantity_exponent":0},
            {"role":"output","unit":"token","kind":"billed","source":"fixture","quantity_units":quantity/2,"quantity_exponent":0}]
    })).unwrap();
    request.resolved_credential = credential.map(str::to_string);
    let result = ingest::record_event(conn, &request, 1).unwrap();
    conn.query_row(
        "SELECT event_key FROM usage_event WHERE event_id=?1",
        [result.event_id],
        |r| r.get(0),
    )
    .unwrap()
}

fn plan(
    conn: &Connection,
    ledger: PlanLedger,
    provider: &str,
    identity: &str,
    from: i64,
    to: Option<i64>,
) {
    conn.execute(&format!("INSERT INTO {}({},provider,plan_name,recurring_cost_minor,currency,amount_exponent,period,effective_from_ms,effective_to_ms) VALUES(?1,?2,'plan',2000,'USD',2,'month',?3,?4)",ledger.table(),ledger.key_column()),params![identity,provider,from,to]).unwrap();
}

fn prepare(conn: &mut Connection, config: &MultiplierConfig) {
    let tx = conn.transaction().unwrap();
    ValuationMaintenance::prepare(&tx, config, &[]).unwrap();
    tx.commit().unwrap();
}

fn drain(conn: &mut Connection, config: &MultiplierConfig) -> Vec<i64> {
    prepare(conn, config);
    let mut maintenance = ValuationMaintenance::default();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    let mut hours = Vec::new();
    loop {
        assert!(
            std::time::Instant::now() < deadline,
            "valuation did not drain"
        );
        let tx = conn.unchecked_transaction().unwrap();
        let ready = maintenance.read_chunk(&tx, config, &[]).unwrap();
        tx.commit().unwrap();
        if ready {
            let hour = maintenance.pending.as_ref().unwrap().hour.number;
            let tx = conn.transaction().unwrap();
            if maintenance.publish(&tx).unwrap() {
                hours.push(hour);
            }
            tx.commit().unwrap();
        } else if !maintenance.has_pending() {
            break;
        }
    }
    hours
}

fn summaries(conn: &mut Connection) {
    let mut maintenance = SummaryMaintenance::default();
    for _ in 0..1000 {
        let dirty: bool = conn
            .query_row("SELECT EXISTS(SELECT 1 FROM summary_dirty_hour)", [], |r| {
                r.get(0)
            })
            .unwrap();
        if !dirty {
            return;
        }
        if maintenance.read_chunk(conn, 10_000).unwrap() {
            let tx = conn.transaction().unwrap();
            maintenance.publish(&tx, 10_000).unwrap();
            tx.commit().unwrap();
        }
    }
    panic!("summary did not drain");
}

fn stored_cells(conn: &Connection, hour: i64) -> Cells {
    let mut stmt = conn.prepare("SELECT summary_key,plan,plan_account,recorded,list_numerator,unvalued,plan_record FROM valuation_cell WHERE kind=0 AND hour_number=?1").unwrap();
    stmt.query_map([hour], |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, String>(1)?,
            r.get::<_, Option<String>>(2)?,
            r.get::<_, String>(3)?,
            r.get::<_, String>(4)?,
            r.get::<_, String>(5)?,
            r.get::<_, Option<String>>(6)?,
        ))
    })
    .unwrap()
    .map(|row| {
        let (key, plan, plan_account, recorded, numerator, unvalued, record) = row.unwrap();
        (
            (
                serde_json::from_str(&key).unwrap(),
                PlanMember {
                    plan,
                    plan_account,
                    record: record.map(|record| serde_json::from_str(&record).unwrap()),
                },
            ),
            ValuedCell {
                recorded: serde_json::from_str(&recorded).unwrap(),
                numerator: numerator.parse().unwrap(),
                unvalued: serde_json::from_str(&unvalued).unwrap(),
            },
        )
    })
    .collect()
}

fn state(conn: &Connection, hour: i64) -> String {
    conn.query_row(
        "SELECT state FROM valuation_hour WHERE kind=0 AND hour_number=?1",
        [hour],
        |r| r.get(0),
    )
    .unwrap()
}

#[test]
fn valuation_summary_dirty_propagates_and_fences_publication() {
    let mut conn = conn();
    let config = config();
    event(&mut conn, "one", 1, "A", None, None, "metered_api", 4);
    rate(&conn, "A", 3);
    summaries(&mut conn);
    drain(&mut conn, &config);
    assert_eq!(state(&conn, 0), "built");
    event(&mut conn, "two", 2, "A", None, None, "metered_api", 6);
    assert_eq!(state(&conn, 0), "dirty");
    summaries(&mut conn);
    let mut maintenance = ValuationMaintenance::default();
    assert!(maintenance.read_chunk(&conn, &config, &[]).unwrap());
    event(
        &mut conn,
        "during-build",
        3,
        "A",
        None,
        None,
        "metered_api",
        2,
    );
    let tx = conn.transaction().unwrap();
    assert!(!maintenance.publish(&tx).unwrap());
    tx.commit().unwrap();
    assert_eq!(state(&conn, 0), "dirty");
    summaries(&mut conn);
    drain(&mut conn, &config);
    assert_eq!(
        stored_cells(&conn, 0)
            .values()
            .map(|c| c.numerator)
            .sum::<i128>(),
        72
    );
}

#[test]
fn valuation_unfetched_models_wait_and_plan_intervals_dirty() {
    let mut conn = conn();
    let config = config();
    for hour in 0..4 {
        event(
            &mut conn,
            &format!("event-{hour}"),
            hour * HOUR_MS + 1,
            "A",
            Some("account"),
            None,
            "subscription_included",
            5,
        );
    }
    summaries(&mut conn);
    drain(&mut conn, &config);
    assert_eq!(state(&conn, 0), "not_yet_valued");
    assert!(stored_cells(&conn, 0).is_empty());
    rate(&conn, "A", 3);
    drain(&mut conn, &config);
    assert_eq!(state(&conn, 0), "built");
    plan(
        &conn,
        PlanLedger::Account,
        "provider",
        "account",
        HOUR_MS + 10,
        Some(2 * HOUR_MS),
    );
    assert_eq!(
        (state(&conn, 0), state(&conn, 1), state(&conn, 2)),
        ("built".into(), "dirty".into(), "built".into())
    );
    assert_eq!(drain(&mut conn, &config), vec![1]);
    plan(
        &conn,
        PlanLedger::Credential,
        "provider",
        "credential",
        2 * HOUR_MS + 1,
        None,
    );
    assert_eq!(drain(&mut conn, &config), vec![2, 3]);
    conn.execute(
        "UPDATE credential_subscription SET effective_to_ms=?1 WHERE credential_ref='credential'",
        [3 * HOUR_MS],
    )
    .unwrap();
    assert_eq!(drain(&mut conn, &config), vec![3]);
}

#[test]
fn valuation_selective_generations_fast_forward_only_unaffected_current_hours() {
    let mut conn = conn();
    let config = config();
    for (hour, model) in [(0, "A"), (1, "B"), (2, "B"), (3, "B"), (4, "B")] {
        event(
            &mut conn,
            &format!("event-{hour}"),
            hour * HOUR_MS + 1,
            model,
            Some("account"),
            None,
            "subscription_included",
            10,
        );
    }
    rate(&conn, "A", 3);
    rate(&conn, "B", 5);
    summaries(&mut conn);
    drain(&mut conn, &config);
    // Leave one hour already dirty: a generation change cannot bless it current.
    event(
        &mut conn,
        "later",
        4 * HOUR_MS + 2,
        "B",
        Some("account"),
        None,
        "subscription_included",
        2,
    );
    let before: Vec<_> = (0..5).map(|h| stored_cells(&conn, h)).collect();
    rate(&conn, "A", 11);
    plan(
        &conn,
        PlanLedger::Account,
        "provider",
        "account",
        2 * HOUR_MS,
        Some(3 * HOUR_MS),
    );
    prepare(&mut conn, &config);
    assert_eq!(
        (
            state(&conn, 0),
            state(&conn, 1),
            state(&conn, 2),
            state(&conn, 3),
            state(&conn, 4)
        ),
        (
            "dirty".into(),
            "built".into(),
            "dirty".into(),
            "built".into(),
            "dirty".into()
        )
    );
    summaries(&mut conn);
    let rebuilt = drain(&mut conn, &config);
    assert_eq!(
        rebuilt,
        vec![0, 2, 4],
        "only changed-model, changed-plan and already-dirty hours rebuild"
    );
    for hour in 0..5 {
        let mut full = Cells::new();
        reduce(
            &conn,
            &Hour {
                kind: 0,
                number: hour,
            }
            .filter(),
            &config,
            &[],
            &mut full,
        )
        .unwrap();
        assert_eq!(stored_cells(&conn, hour), full, "hour {hour}");
        let current: bool = conn.query_row("SELECT h.state='built' AND h.rate_generation=s.rate_generation AND h.config_generation=s.config_generation AND h.plan_generation=s.plan_generation FROM valuation_hour h CROSS JOIN valuation_state s WHERE h.kind=0 AND h.hour_number=?1",[hour],|r|r.get(0)).unwrap();
        assert!(current, "all three generations must match");
    }
    assert_eq!(stored_cells(&conn, 1), before[1]);
    assert_eq!(stored_cells(&conn, 3), before[3]);
    let mut changed = config.clone();
    changed
        .api_twins
        .insert("another".into(), "provider".into());
    assert_eq!(drain(&mut conn, &changed), vec![0, 1, 2, 3, 4]);
}

#[test]
fn valuation_pool_membership_matches_coverage_and_handles_unique_ambiguous_unmapped() {
    let mut conn = conn();
    let config = config();
    plan(&conn, PlanLedger::Account, "provider", "account", 0, None);
    plan(
        &conn,
        PlanLedger::Credential,
        "provider",
        "credential",
        0,
        None,
    );
    let tokens = EventTokens {
        producer: "opencode".into(),
        provider: "provider".into(),
        model: Some("A".into()),
        mode: None,
        roles: Vec::new(),
    };
    let member = |config: &MultiplierConfig, account, credential, basis| {
        plan_member(&conn, config, &tokens, basis, account, credential, Some(1)).unwrap()
    };
    assert_eq!(
        member(
            &config,
            Some("account"),
            Some("credential"),
            "subscription_included"
        ),
        PlanMember {
            plan: "pool".into(),
            plan_account: Some("account".into()),
            record: Some(record_id(PlanLedger::Account, "account", 0)),
        }
    );
    assert_eq!(
        member(&config, None, Some("credential"), "unknown"),
        PlanMember {
            plan: "pool".into(),
            plan_account: Some("credential".into()),
            record: Some(record_id(PlanLedger::Credential, "credential", 0)),
        }
    );
    assert_eq!(
        member(&config, None, Some("Credential"), "unknown")
            .plan_account
            .as_deref(),
        Some("account_unknown")
    );
    assert_eq!(
        member(&config, None, None, "unknown")
            .plan_account
            .as_deref(),
        Some("account_unknown")
    );
    assert_eq!(
        member(&config, Some("account"), Some("credential"), "metered_api"),
        PlanMember::non_pool("api")
    );
    let empty = MultiplierConfig {
        quota_providers: BTreeMap::new(),
        ..config.clone()
    };
    assert_eq!(
        member(&empty, Some("account"), None, "unknown"),
        PlanMember {
            plan: "provider".into(),
            plan_account: Some("account".into()),
            record: Some(record_id(PlanLedger::Account, "account", 0)),
        }
    );
    assert_eq!(
        member(&empty, None, None, "unknown"),
        PlanMember::non_pool("none")
    );
    let mut ambiguous = config.clone();
    let mut other = ambiguous.quota_providers["pool"].clone();
    other.scope[0].producer = "other-producer".into();
    ambiguous.quota_providers.insert("other".into(), other);
    assert!(
        ambiguous.validate().is_ok(),
        "disjoint scopes can still map a plan provider ambiguously"
    );
    assert_eq!(
        member(&ambiguous, Some("account"), None, "unknown"),
        PlanMember::non_pool("pool_conflict")
    );
    // Drive credential precedence and conflict through persisted builder rows too.
    event(
        &mut conn,
        "credential",
        1,
        "A",
        None,
        Some("credential"),
        "unknown",
        10,
    );
    event(&mut conn, "fallback", 2, "A", None, None, "unknown", 10);
    rate(&conn, "A", 3);
    summaries(&mut conn);
    drain(&mut conn, &ambiguous);
    let cells = stored_cells(&conn, 0);
    assert!(cells
        .iter()
        .any(|((_, member), cell)| member.plan == "pool_conflict" && cell.numerator == 60));
    assert!(cells
        .keys()
        .any(|(_, member)| member.plan_account.as_deref() == Some("account_unknown")));
}

#[test]
fn valuation_config_refuses_cross_pool_scope_intersections() {
    let config = config();
    for (prefix, basis, overlaps) in [(None, None, true), (Some("a"), Some("unknown"), true)] {
        let mut config = config.clone();
        let mut other = config.quota_providers["pool"].clone();
        other.scope[0].model_prefix = prefix.map(str::to_string);
        other.scope[0].charge_basis = basis.map(str::to_string);
        config.quota_providers.insert("other".into(), other);
        assert_eq!(config.validate().is_err(), overlaps);
    }
    let mut config = config;
    config.quota_providers.get_mut("pool").unwrap().scope[0].model_prefix = Some("abc".into());
    let mut other = config.quota_providers["pool"].clone();
    other.scope[0].model_prefix = Some("abcd".into());
    config.quota_providers.insert("other".into(), other.clone());
    assert!(config.validate().is_err());
    other.scope[0].model_prefix = Some("xyz".into());
    config.quota_providers.insert("other".into(), other);
    assert!(config.validate().is_ok());
}

#[test]
fn valuation_paying_credential_uses_resolved_else_auth_selection_exactly() {
    let mut conn = conn();
    let config = config();
    plan(
        &conn,
        PlanLedger::Credential,
        "provider",
        "credential",
        0,
        None,
    );
    plan(
        &conn,
        PlanLedger::Credential,
        "provider",
        "selected",
        0,
        None,
    );
    plan(&conn, PlanLedger::Account, "provider", "account", 0, None);
    for (id, account, credential, quantity) in [
        ("resolved", None, Some("credential"), 2),
        ("selected", None, None, 4),
        ("case-mismatch", None, Some("Credential"), 6),
        ("account-wins", Some("account"), Some("credential"), 8),
    ] {
        event(
            &mut conn, id, 1, "A", account, credential, "unknown", quantity,
        );
    }
    rate(&conn, "A", 3);
    summaries(&mut conn);
    drain(&mut conn, &config);
    let actual: BTreeMap<_, _> = stored_cells(&conn, 0)
        .into_iter()
        .map(|((_, member), cell)| {
            (
                member.plan_account.unwrap(),
                (cell.recorded.cell.events, cell.numerator),
            )
        })
        .collect();
    assert_eq!(
        actual,
        BTreeMap::from([
            ("credential".into(), (1, 12)),
            ("selected".into(), (1, 24)),
            ("account_unknown".into(), (1, 36)),
            ("account".into(), (1, 48)),
        ])
    );
}

// Explicit ledger prices are deliberately unrelated to the list rates supplied
// by rate(), so the recorded and valuation amounts cannot accidentally alias.
fn recorded_price(conn: &Connection, key: i64, nanos: i64) -> i64 {
    let (id, basis): (String, String) = conn
        .query_row(
            "SELECT event_id,charge_basis FROM usage_event WHERE event_key=?1",
            [key],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    if basis != "metered_api" {
        conn.execute(
            "UPDATE usage_event SET pricing_generation=1,currency='USD' WHERE event_key=?1",
            [key],
        )
        .unwrap();
        for role in ["input", "output"] {
            conn.execute("INSERT INTO priced_charge(event_key,role,unit,pricing_generation,outcome,charge_basis,basis_covered,currency,charge_nanos,origin,recorded_at_ms) VALUES(?1,?2,'token',1,'covered','subscription_included',1,'USD',0,'pricing_pass',1)",params![key,role]).unwrap();
        }
        let tx = conn.unchecked_transaction().unwrap();
        ingest::record_adjustment_in_tx(
            &tx,
            &ingest::AdjustmentRecordRequest {
                event_id: id,
                currency: "USD".into(),
                amount_nanos: -nanos,
                reason: ingest::AdjustmentReason::Refund,
                occurred_at_ms: 2,
                note: None,
            },
            &ingest::AdjustmentWriteContext::correction_ingest(format!("refund-{key}")),
            3,
        )
        .unwrap();
        tx.commit().unwrap();
        return -nanos;
    }
    conn.execute(
        "UPDATE usage_event SET pricing_generation=1,currency='USD' WHERE event_key=?1",
        [key],
    )
    .unwrap();
    for (role, amount) in [("input", nanos), ("output", 0)] {
        conn.execute("INSERT INTO priced_charge(event_key,role,unit,pricing_generation,outcome,charge_basis,basis_covered,currency,charge_nanos,rate_units,rate_exponent,floor_state,catalog_source,resolved_at_ms,catalog_entry,origin,recorded_at_ms) SELECT ?1,?2,'token',1,'priced',charge_basis,0,'USD',?3,1,9,'none','fixture',1,'{}','pricing_pass',1 FROM usage_event WHERE event_key=?1",params![key,role,amount]).unwrap();
    }
    nanos
}

#[test]
fn valuation_randomized_cells_equal_per_call_ledger_and_split_recorded_spend() {
    use chrono::TimeZone;
    let mut conn = conn();
    let config = config();
    let mut seed = 7129_u64;
    let mut next = || {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        seed >> 32
    };
    let mut oracle: BTreeMap<(i64, Key, PlanMember), (i128, i128)> = BTreeMap::new();
    let mut calls = Vec::new();
    for index in 0..360 {
        let hour = i64::try_from(next() % 72).unwrap();
        let at = if index < 4 {
            18 * HOUR_MS
                + if index % 2 == 0 {
                    HOUR_MS / 4
                } else {
                    3 * HOUR_MS / 4
                }
        } else {
            hour * HOUR_MS + i64::try_from(next() % 3_599_999).unwrap()
        };
        let hour = at / HOUR_MS;
        let quantity = i64::try_from(next() % 1000).unwrap();
        let model = if index < 4 || next() % 2 == 0 {
            "A"
        } else {
            "B"
        };
        let basis = if index >= 4 && next() % 3 == 0 {
            "metered_api"
        } else {
            "subscription_included"
        };
        let account = if index < 2 || (index >= 4 && next() % 2 == 0) {
            Some("shared")
        } else {
            None
        };
        let credential = Some("shared");
        let key = event(
            &mut conn,
            &format!("random-{index}"),
            at,
            model,
            account,
            credential,
            basis,
            quantity,
        );
        let spend = i64::try_from(next() % 10000).unwrap();
        let spend = recorded_price(&conn, key, spend);
        // Plan effective dates can assign an otherwise identical summary key
        // to a recorded plan member or to account_unknown within the same hour.
        let member = if basis == "metered_api" {
            PlanMember::non_pool("api")
        } else {
            PlanMember {
                plan: "pool".into(),
                plan_account: Some(
                    if (HOUR_MS / 2..36 * HOUR_MS + HOUR_MS / 2).contains(&at) {
                        "shared"
                    } else {
                        "account_unknown"
                    }
                    .into(),
                ),
                record: (HOUR_MS / 2..36 * HOUR_MS + HOUR_MS / 2)
                    .contains(&at)
                    .then(|| {
                        record_id(
                            if account.is_some() {
                                PlanLedger::Account
                            } else {
                                PlanLedger::Credential
                            },
                            "shared",
                            if at < 18 * HOUR_MS + HOUR_MS / 2 {
                                HOUR_MS / 2
                            } else {
                                18 * HOUR_MS + HOUR_MS / 2
                            },
                        )
                    }),
            }
        };
        let unit_rate = if model == "A" { 3_i128 } else { 7 };
        let numerator = unit_rate * (i128::from(quantity) + 2 * i128::from(quantity / 2));
        let ledger =
            load_scoped_ledger(&conn, &DIMENSIONS, Some(&format!("event_key={key}"))).unwrap();
        let summary_key = super::super::key(&ledger, &ledger.events[0]);
        let sums = oracle
            .entry((hour, summary_key.clone(), member.clone()))
            .or_default();
        sums.0 += numerator;
        sums.1 += i128::from(spend);
        calls.push((at, summary_key, member, numerator, i128::from(spend)));
    }
    for ledger in [PlanLedger::Account, PlanLedger::Credential] {
        plan(
            &conn,
            ledger,
            "provider",
            "shared",
            HOUR_MS / 2,
            Some(18 * HOUR_MS + HOUR_MS / 2),
        );
        plan(
            &conn,
            ledger,
            "provider",
            "shared",
            18 * HOUR_MS + HOUR_MS / 2,
            Some(36 * HOUR_MS + HOUR_MS / 2),
        );
    }
    rate(&conn, "A", 3);
    rate(&conn, "B", 7);
    summaries(&mut conn);
    drain(&mut conn, &config);
    let mut split = false;
    for hour in 0..72 {
        let cells = stored_cells(&conn, hour);
        let mut totals: BTreeMap<Key, Recorded> = BTreeMap::new();
        for ((key, member), cell) in &cells {
            assert_eq!(
                (cell.numerator, cell.recorded.cell.total),
                oracle[&(hour, key.clone(), member.clone())]
            );
            assert_eq!(cell.unvalued, Unvalued::default());
            let total = totals.entry(key.clone()).or_default();
            total.cell.merge(&cell.recorded.cell).unwrap();
            for (key, value) in &cell.recorded.quantities {
                merge_quantity(&mut total.quantities, key, value).unwrap();
            }
            for (key, value) in &cell.recorded.money {
                merge_money(&mut total.money, key, value).unwrap();
            }
            split |= cells
                .keys()
                .filter(|(candidate, _)| candidate == key)
                .count()
                > 1;
        }
        let source = stored(
            &conn,
            Hour {
                kind: 0,
                number: hour,
            },
        )
        .unwrap();
        assert_eq!(totals.len(), source.cells.len());
        for (key, total) in totals {
            assert_eq!(total.cell, source.cells[&key]);
            assert_eq!(
                total.quantities,
                source
                    .quantities
                    .iter()
                    .filter(|((candidate, _), _)| candidate == &key)
                    .map(|((_, key), value)| (key.clone(), value.clone()))
                    .collect::<Vec<_>>()
            );
            assert_eq!(
                total.money,
                source
                    .money
                    .iter()
                    .filter(|((candidate, _, _), _)| candidate == &key)
                    .map(|((_, kind, currency), value)| ((*kind, currency.clone()), value.clone()))
                    .collect::<Vec<_>>()
            );
        }
    }
    assert!(
        split,
        "fixture must actually split a summary key across plan members"
    );
    let records: BTreeSet<_> = stored_cells(&conn, 18)
        .into_keys()
        .filter_map(|(_, member)| member.record)
        .collect();
    assert_eq!(
        records.len(),
        4,
        "both namespaces and same-hour sequential records remain distinct"
    );
    // Non-integral UTC offsets put local-day boundaries inside UTC hours. The
    // reducer selects those partial hours by call time, then groups independent
    // call arithmetic by local day, model/account and pool member.
    for zone in [
        "UTC",
        "America/New_York",
        "Europe/Berlin",
        "Asia/Kolkata",
        "Australia/Adelaide",
    ] {
        let tz: chrono_tz::Tz = zone.parse().unwrap();
        for dimension in [Grouping::Model, Grouping::Account, Grouping::ChargeBasis] {
            for day in 1..=3 {
                let start = tz
                    .with_ymd_and_hms(1970, 1, day, 0, 0, 0)
                    .unwrap()
                    .timestamp_millis();
                let end = tz
                    .with_ymd_and_hms(1970, 1, day + 1, 0, 0, 0)
                    .unwrap()
                    .timestamp_millis();
                let mut direct: BTreeMap<(Option<String>, PlanMember), (i128, i128)> =
                    BTreeMap::new();
                for (at, key, member, numerator, spend) in &calls {
                    if (start..end).contains(at) {
                        let sums = direct
                            .entry((key.0[dimension as usize].clone(), member.clone()))
                            .or_default();
                        sums.0 += numerator;
                        sums.1 += spend;
                    }
                }
                let mut reduced = Cells::new();
                reduce(
                    &conn,
                    &format!("occurred_at_ms>={start} AND occurred_at_ms<{end}"),
                    &config,
                    &[],
                    &mut reduced,
                )
                .unwrap();
                let mut actual: BTreeMap<(Option<String>, PlanMember), (i128, i128)> =
                    BTreeMap::new();
                for ((key, member), cell) in reduced {
                    let sums = actual
                        .entry((key.0[dimension as usize].clone(), member))
                        .or_default();
                    sums.0 += cell.numerator;
                    sums.1 += cell.recorded.cell.total;
                }
                assert_eq!(actual, direct, "{zone} {dimension:?} day {day}");
            }
        }
    }
}

#[test]
fn valuation_event_error_discards_partial_value_without_stopping_hour() {
    let mut conn = conn();
    let config = config();
    event(&mut conn, "good", 1, "A", None, None, "metered_api", 10);
    event(&mut conn, "bad", 2, "B", None, None, "metered_api", 10);
    rate(&conn, "A", 3);
    rate(&conn, "B", 7);
    // A malformed reference graph is isolated to its event rather than aborting
    // the entire hour or blessing that event's partially computed value.
    conn.execute(
        "UPDATE rate_model SET reference_provider='provider',reference_model='B' WHERE model='B'",
        [],
    )
    .unwrap();
    summaries(&mut conn);
    drain(&mut conn, &config);
    assert_eq!(state(&conn, 0), "built");
    let cells = stored_cells(&conn, 0);
    assert_eq!(cells.values().map(|c| c.numerator).sum::<i128>(), 60);
    assert_eq!(
        cells.values().map(|c| c.unvalued.measurements).sum::<u64>(),
        2
    );
    assert_eq!(
        cells
            .values()
            .map(|c| c
                .unvalued
                .by_reason
                .get(&crate::valuation::UnvaluedReason::ValuationError)
                .copied()
                .unwrap_or(0))
            .sum::<u64>(),
        2
    );
}

#[test]
fn valuation_waits_for_summary_then_uses_second_rate_change_and_fences_rate_publication() {
    let mut conn = conn();
    let config = config();
    event(&mut conn, "A", 1, "A", None, None, "metered_api", 10);
    rate(&conn, "A", 3);
    summaries(&mut conn);
    drain(&mut conn, &config);
    event(&mut conn, "new-B", 2, "B", None, None, "metered_api", 10);
    rate(&conn, "B", 5);
    prepare(&mut conn, &config);
    let mut maintenance = ValuationMaintenance::default();
    assert!(!maintenance.read_chunk(&conn, &config, &[]).unwrap());
    assert!(
        !maintenance.has_pending(),
        "valuation cannot get ahead of a dirty summary"
    );
    rate(&conn, "B", 11);
    summaries(&mut conn);
    drain(&mut conn, &config);
    assert_eq!(
        stored_cells(&conn, 0)
            .values()
            .map(|cell| cell.numerator)
            .sum::<i128>(),
        280
    );
    // A rate-cache update between reduction and commit must prevent publishing
    // the stale build, even before maintenance synchronizes the rate dirty queue.
    event(&mut conn, "another-A", 3, "A", None, None, "metered_api", 2);
    summaries(&mut conn);
    prepare(&mut conn, &config);
    assert!(maintenance.read_chunk(&conn, &config, &[]).unwrap());
    rate(&conn, "A", 13);
    let tx = conn.transaction().unwrap();
    assert!(!maintenance.publish(&tx).unwrap());
    tx.commit().unwrap();
    drain(&mut conn, &config);
    assert_eq!(
        stored_cells(&conn, 0)
            .values()
            .map(|cell| cell.numerator)
            .sum::<i128>(),
        532
    );
}

#[path = "fee_tests.rs"]
mod fee_tests;
