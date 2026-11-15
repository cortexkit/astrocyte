use super::*;
use fees::{DayMember, DayWeights, FeeDays, Fraction, PoolDay, Weights, DAY_MS};
use num_bigint::BigInt;
use num_traits::{Signed, ToPrimitive};

#[allow(clippy::too_many_arguments)]
fn fee_plan(
    conn: &Connection,
    ledger: PlanLedger,
    provider: &str,
    identity: &str,
    from: i64,
    to: Option<i64>,
    minor: i64,
    exponent: u32,
    currency: &str,
    period: &str,
) {
    conn.execute(&format!("INSERT INTO {}({},provider,plan_name,recurring_cost_minor,currency,amount_exponent,period,effective_from_ms,effective_to_ms) VALUES(?1,?2,'plan',?3,?4,?5,?6,?7,?8)", ledger.table(), ledger.key_column()),params![identity,provider,minor,currency,exponent,period,from,to]).unwrap();
}

fn utc(year: i32, month: u32, day: u32) -> i64 {
    use chrono::TimeZone;
    chrono::Utc
        .with_ymd_and_hms(year, month, day, 0, 0, 0)
        .unwrap()
        .timestamp_millis()
}

fn fee_weights(conn: &Connection, from: i64, to: i64) -> (DayWeights, BTreeMap<String, Weights>) {
    let mut day = DayWeights::default();
    let mut rows: BTreeMap<String, Weights> = BTreeMap::new();
    for hour in from / HOUR_MS..to / HOUR_MS {
        for ((summary, member), cell) in stored_cells(conn, hour) {
            let key = DayMember {
                day_start_ms: (hour * HOUR_MS).div_euclid(DAY_MS) * DAY_MS,
                member,
            };
            day.weights
                .checked_add(key.clone(), cell.numerator)
                .unwrap();
            rows.entry(summary.0[Grouping::Model as usize].clone().unwrap())
                .or_default()
                .checked_add(key, cell.numerator)
                .unwrap();
        }
    }
    (day, rows)
}

fn fee_sum(book: &FeeDays) -> Fraction {
    book.days.iter().fold(Fraction::default(), |sum, day| {
        sum.checked_add(&day.fee).unwrap()
    })
}

#[test]
fn fee_calendar_shares_are_exact_for_months_years_and_partial_days() {
    for (year, month, days) in [(2024, 1, 31), (2024, 4, 30), (2024, 2, 29), (2023, 2, 28)] {
        let conn = conn();
        let start = utc(year, month, 1);
        let end = start + days * DAY_MS;
        fee_plan(
            &conn,
            PlanLedger::Account,
            "provider",
            "account",
            start,
            Some(end),
            12345,
            2,
            "USD",
            "month",
        );
        let book = FeeDays::load(&conn, &config(), start, end).unwrap();
        assert_eq!(book.days.len(), usize::try_from(days).unwrap());
        assert!(book
            .days
            .iter()
            .all(|day| day.fee == Fraction::new(123_450_000_000, i128::from(days)).unwrap()));
        assert_eq!(fee_sum(&book), Fraction::new(123_450_000_000, 1).unwrap());
    }
    for (year, days) in [(2023, 365), (2024, 366)] {
        let conn = conn();
        let start = utc(year, 1, 1);
        let end = utc(year + 1, 1, 1);
        fee_plan(
            &conn,
            PlanLedger::Credential,
            "provider",
            "credential",
            start,
            Some(end),
            999,
            9,
            "USD",
            "year",
        );
        let book = FeeDays::load(&conn, &config(), start, end).unwrap();
        assert_eq!(book.days.len(), usize::try_from(days).unwrap());
        assert_eq!(fee_sum(&book), Fraction::new(999, 1).unwrap());
    }
    let conn = conn();
    let start = utc(2024, 4, 1);
    fee_plan(
        &conn,
        PlanLedger::Account,
        "provider",
        "half",
        start + DAY_MS / 2,
        Some(start + DAY_MS + DAY_MS / 4),
        30,
        9,
        "USD",
        "month",
    );
    let book = FeeDays::load(&conn, &config(), start, start + 2 * DAY_MS).unwrap();
    assert_eq!(book.days[0].fee, Fraction::new(1, 2).unwrap());
    assert_eq!(book.days[1].fee, Fraction::new(1, 4).unwrap());
    assert_eq!(fee_sum(&book), Fraction::new(3, 4).unwrap());
}

#[test]
fn fee_per_record_then_rule_spread_uses_real_cells_and_zero_routes_onward() {
    let mut conn = conn();
    let config = config();
    let day = utc(2024, 4, 1);
    let end = day + DAY_MS;
    // Equal strings in distinct ledgers, and two records within the same hour.
    for (ledger, from, to, fee) in [
        (PlanLedger::Account, day, day + HOUR_MS / 2, 1440),
        (PlanLedger::Account, day + HOUR_MS / 2, end, 1440),
        (PlanLedger::Credential, day, end, 60),
    ] {
        fee_plan(
            &conn,
            ledger,
            "provider",
            "same",
            from,
            Some(to),
            fee,
            9,
            "USD",
            "month",
        );
    }
    for (identity, fee, currency) in [
        ("no-calls", 90, "USD"),
        ("zero", 30, "USD"),
        ("non-usd", 999, "EUR"),
        ("free", 0, "USD"),
    ] {
        fee_plan(
            &conn,
            PlanLedger::Account,
            "provider",
            identity,
            day,
            Some(end),
            fee,
            9,
            currency,
            "month",
        );
    }
    for model in [
        "early",
        "late",
        "credential",
        "rule",
        "non-usd",
        "api",
        "free",
    ] {
        rate(&conn, model, 1);
    }
    rate(&conn, "zero", 0);
    let early = event(
        &mut conn,
        "early",
        day + 1,
        "early",
        Some("same"),
        None,
        "subscription_included",
        1,
    );
    recorded_price(&conn, early, 7);
    event(
        &mut conn,
        "late",
        day + HOUR_MS / 2 + 1,
        "late",
        Some("same"),
        None,
        "subscription_included",
        100,
    );
    event(
        &mut conn,
        "credential",
        day + 1,
        "credential",
        None,
        Some("same"),
        "subscription_included",
        1,
    );
    event(&mut conn, "rule", day + 1, "rule", None, None, "unknown", 4);
    event(
        &mut conn,
        "zero",
        day + 1,
        "zero",
        Some("zero"),
        None,
        "subscription_included",
        100,
    );
    event(
        &mut conn,
        "non-usd",
        day + 1,
        "non-usd",
        Some("non-usd"),
        None,
        "subscription_included",
        1,
    );
    event(
        &mut conn,
        "free",
        day + 1,
        "free",
        Some("free"),
        None,
        "subscription_included",
        1,
    );
    event(
        &mut conn,
        "api",
        day + 1,
        "api",
        Some("same"),
        None,
        "metered_api",
        1000,
    );
    summaries(&mut conn);
    drain(&mut conn, &config);
    let (weights, rows) = fee_weights(&conn, day, end);
    let book = FeeDays::load(&conn, &config, day, end).unwrap();
    let allocation = book.allocate(&weights, end).unwrap();
    // First half-hour owes 1 nano, later 47. Different call weights must not
    // mix these records, nor the credential record's independent 2 nanos.
    for (model, expected) in [
        ("early", 1),
        ("late", 47),
        ("credential", 2),
        ("rule", 4),
        ("zero", 0),
        ("non-usd", 0),
        ("free", 0),
        ("api", 0),
    ] {
        let (share, pending) = allocation.row(&rows[model]).unwrap();
        assert!(pending.is_empty());
        assert_eq!(share.fee, Fraction::new(expected, 1).unwrap(), "{model}");
        let deduced = share
            .deduced("USD", if model == "early" { 7 } else { 0 }, 0)
            .unwrap()
            .unwrap();
        assert_eq!(
            deduced.nanos,
            i64::try_from(expected).unwrap() + if model == "early" { 7 } else { 0 }
        );
        assert_eq!(
            deduced.by_rule_share,
            if model == "rule" { Some(1.0) } else { None }
        );
        assert!(share.deduced("EUR", 5, 0).unwrap().is_none());
    }
    let totals = allocation.totals(true, |_| true).unwrap();
    assert_eq!(totals.deduced_unspread_nanos, Some(0));
    assert_eq!(totals.deduced_non_usd_plans, 1);
    assert_eq!(totals.deduced_conflict_plans, 0);
}

#[test]
fn fee_empty_ledger_days_unspread_overlap_filters_and_rounding_once() {
    let conn = conn();
    let config = config();
    let day = utc(2024, 4, 1);
    for identity in ["one", "two"] {
        fee_plan(
            &conn,
            PlanLedger::Account,
            "provider",
            identity,
            day,
            Some(day + DAY_MS),
            30,
            9,
            "USD",
            "month",
        );
    }
    let mut exact = Fraction::default();
    let mut nanos = 0;
    for (from, to) in [(day, day + DAY_MS / 4), (day + DAY_MS / 4, day + DAY_MS)] {
        let book = FeeDays::load(&conn, &config, from, to).unwrap();
        assert_eq!(book.days.len(), 2, "fee-only days exist without any usage");
        let allocation = book.allocate(&DayWeights::default(), day + DAY_MS).unwrap();
        exact = exact
            .checked_add(&allocation.unspread_exact(|_| true).unwrap())
            .unwrap();
        nanos += allocation
            .totals(true, |_| true)
            .unwrap()
            .deduced_unspread_nanos
            .unwrap();
    }
    assert_eq!(exact, Fraction::new(2, 1).unwrap());
    // 0.5 and 1.5 round to even only after summing both records.
    assert_eq!(nanos, 2);
    let full = FeeDays::load(&conn, &config, day, day + DAY_MS)
        .unwrap()
        .allocate(&DayWeights::default(), day + DAY_MS)
        .unwrap();
    assert_eq!(
        full.totals(true, |_| true).unwrap().deduced_unspread_nanos,
        Some(2)
    );
    assert_eq!(
        full.totals(true, |member| member.plan_account.as_deref() == Some("one"))
            .unwrap()
            .deduced_unspread_nanos,
        Some(1)
    );
    assert_eq!(
        full.totals(true, |member| member.plan == "other")
            .unwrap()
            .deduced_unspread_nanos,
        Some(0)
    );
    assert_eq!(
        full.totals(false, |_| true).unwrap().deduced_unspread_nanos,
        None
    );
}

#[test]
fn fee_pending_days_only_open_or_non_current_pool_days_not_permanent_unvalued() {
    let mut conn = conn();
    let config = config();
    let start = utc(2024, 4, 1);
    let end = start + 2 * DAY_MS;
    fee_plan(
        &conn,
        PlanLedger::Account,
        "provider",
        "no-calls",
        start,
        Some(end),
        30,
        9,
        "USD",
        "month",
    );
    fee_plan(
        &conn,
        PlanLedger::Account,
        "other",
        "other",
        start,
        Some(end),
        30,
        9,
        "USD",
        "month",
    );
    let history = serde_json::from_value(json!({"source":"synthetic","provider_id":"provider","model_id":"missing-card","fact_prefix":"rate.","eras":[],"served_by_key":{}})).unwrap();
    publish(
        &conn,
        &fetched(history, "provider", "missing-card", None).unwrap(),
        1,
    )
    .unwrap();
    for day in [start, start + DAY_MS] {
        event(
            &mut conn,
            &format!("unvalued-{day}"),
            day + 1,
            "missing-card",
            None,
            None,
            "unknown",
            10,
        );
    }
    summaries(&mut conn);
    drain(&mut conn, &config);
    let (mut weights, rows) = fee_weights(&conn, start, end);
    assert!(weights.weights.0.values().all(|weight| *weight == 0));
    let book = FeeDays::load(&conn, &config, start, end).unwrap();
    let allocation = book.allocate(&weights, end - 1).unwrap();
    let (share, pending) = allocation.row(&rows["missing-card"]).unwrap();
    assert_eq!(share.fee, Fraction::default());
    assert_eq!(pending, BTreeSet::from([start + DAY_MS]));
    // The ended, fully built day is not pending despite permanently unvalued
    // measurements. Only its fee, in each pool, is unspread.
    assert_eq!(
        allocation
            .totals(true, |_| true)
            .unwrap()
            .deduced_unspread_nanos,
        Some(2)
    );
    weights.not_current.insert(PoolDay {
        day_start_ms: start,
        pool: "pool".into(),
    });
    let allocation = book.allocate(&weights, end).unwrap();
    let (_, pending) = allocation.row(&rows["missing-card"]).unwrap();
    assert_eq!(pending, BTreeSet::from([start]));
    assert_eq!(
        allocation
            .totals(true, |_| true)
            .unwrap()
            .deduced_unspread_nanos,
        Some(3),
        "other pool remains current"
    );
    let allocation = book.allocate(&weights, end - 1).unwrap();
    let (_, pending) = allocation.row(&rows["missing-card"]).unwrap();
    assert_eq!(pending.len(), 2);
    assert_eq!(
        allocation
            .totals(true, |_| true)
            .unwrap()
            .deduced_unspread_nanos,
        Some(1)
    );
}

#[test]
fn fee_conflicting_record_is_unspread_once_without_changing_either_pool() {
    let mut conn = conn();
    let mut config = config();
    let mut other = config.quota_providers["pool"].clone();
    other.scope[0].producer = "other".into();
    config.quota_providers.insert("second".into(), other);
    let day = utc(2024, 4, 1);
    let end = day + DAY_MS;
    fee_plan(
        &conn,
        PlanLedger::Account,
        "provider",
        "conflict",
        day,
        Some(end),
        300,
        9,
        "USD",
        "month",
    );
    rate(&conn, "A", 1);
    rate(&conn, "B", 1);
    for (provider, minor) in [("pool", 60), ("second", 90)] {
        fee_plan(
            &conn,
            PlanLedger::Account,
            provider,
            provider,
            day,
            Some(end),
            minor,
            9,
            "USD",
            "month",
        );
    }
    event(
        &mut conn,
        "conflict",
        day + 1,
        "A",
        Some("conflict"),
        None,
        "subscription_included",
        100,
    );
    event(&mut conn, "rule", day + 1, "A", None, None, "unknown", 100);
    let request: ingest::EventRecordRequest = serde_json::from_value(json!({
        "producer":"other","producer_ref":"other-rule","provider":"provider","model":"B",
        "occurred_at_ms":day+1,"charge_basis":"unknown",
        "measurements":[{"role":"input","unit":"token","kind":"billed","source":"fixture","quantity_units":1,"quantity_exponent":0}]
    })).unwrap();
    ingest::record_event(&mut conn, &request, 1).unwrap();
    summaries(&mut conn);
    drain(&mut conn, &config);
    let (mut weights, rows) = fee_weights(&conn, day, end);
    let book = FeeDays::load(&conn, &config, day, end).unwrap();
    let current = book.allocate(&weights, end).unwrap();
    assert_eq!(
        current.row(&rows["A"]).unwrap().0.fee,
        Fraction::new(2, 1).unwrap()
    );
    assert_eq!(
        current.row(&rows["B"]).unwrap().0.fee,
        Fraction::new(3, 1).unwrap()
    );
    assert_eq!(
        current
            .totals(true, |_| true)
            .unwrap()
            .deduced_unspread_nanos,
        Some(10)
    );
    // Even if both candidate pools are stale and the UTC day is still open,
    // ambiguous mapping is not evidence that its fee belongs in either pool.
    for pool in ["pool", "second"] {
        weights.not_current.insert(PoolDay {
            day_start_ms: day,
            pool: pool.into(),
        });
    }
    let allocation = FeeDays::load(&conn, &config, day, end)
        .unwrap()
        .allocate(&weights, day + 1)
        .unwrap();
    assert_eq!(
        allocation.row(&rows["A"]).unwrap().0.fee,
        Fraction::default()
    );
    assert_eq!(
        allocation.row(&rows["B"]).unwrap().0.fee,
        Fraction::default()
    );
    let totals = allocation.totals(true, |_| true).unwrap();
    assert_eq!(totals.deduced_unspread_nanos, Some(10));
    assert_eq!(totals.deduced_conflict_plans, 1);
    assert_eq!(
        allocation
            .totals(true, |m| m.plan_account.as_deref() == Some("conflict"))
            .unwrap()
            .deduced_unspread_nanos,
        Some(10)
    );
    assert_eq!(
        allocation
            .totals(true, |m| m.plan == "pool")
            .unwrap()
            .deduced_unspread_nanos,
        Some(0)
    );
}

fn assert_half_nano_bound(exact: &Fraction, rounded: i64) {
    let difference = (BigInt::from(rounded) * exact.denominator() - exact.numerator()).abs();
    assert!(
        2_u8 * difference <= *exact.denominator(),
        "{exact:?} rounded to {rounded}"
    );
}

#[test]
fn fee_randomized_cells_conserve_fees_and_disjoint_window_exact_shares() {
    let mut seed = 710993_u64;
    let mut next = || {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        seed >> 32
    };
    for case in 0..24 {
        let mut conn = conn();
        let config = config();
        let start = utc(2024, 2, 1);
        let end = start + 2 * DAY_MS;
        let split = start + (1 + i64::try_from(next() % 22).unwrap()) * HOUR_MS;
        for (ledger, identity) in [
            (PlanLedger::Account, "same"),
            (PlanLedger::Credential, "same"),
            (PlanLedger::Account, "empty"),
        ] {
            let fee = i64::try_from(next() % 10000).unwrap();
            let from = start + i64::try_from(next() % HOUR_MS.unsigned_abs()).unwrap();
            fee_plan(
                &conn,
                ledger,
                "provider",
                identity,
                from,
                Some(end),
                fee,
                9,
                "USD",
                "month",
            );
        }
        for model in ["r0", "r1", "r2", "r3"] {
            rate(&conn, model, 1);
        }
        rate(&conn, "zero", 0);
        for index in 0..48 {
            let at = start + i64::try_from(next() % u64::try_from(2 * DAY_MS).unwrap()).unwrap();
            let model = format!("r{}", next() % 4);
            let choice = next() % 6;
            let account = if choice == 0 { Some("same") } else { None };
            let credential = if choice == 1 { Some("same") } else { None };
            let model = if choice == 2 { "zero" } else { &model };
            let basis = if choice == 3 {
                "metered_api"
            } else {
                "unknown"
            };
            let quantity = i64::try_from(next() % 15).unwrap();
            event(
                &mut conn,
                &format!("{case}-{index}"),
                at,
                model,
                account,
                credential,
                basis,
                quantity,
            );
        }
        summaries(&mut conn);
        drain(&mut conn, &config);
        let (day, rows) = fee_weights(&conn, start, end);
        let book = FeeDays::load(&conn, &config, start, end).unwrap();
        let allocation = book.allocate(&day, end).unwrap();
        let first = FeeDays::load(&conn, &config, start, split)
            .unwrap()
            .allocate(&day, end)
            .unwrap();
        let second = FeeDays::load(&conn, &config, split, end)
            .unwrap()
            .allocate(&day, end)
            .unwrap();
        let (_, first_rows) = fee_weights(&conn, start, split);
        let (_, second_rows) = fee_weights(&conn, split, end);
        let mut shares = Fraction::default();
        let mut rounded = 0_i64;
        for (model, weights) in &rows {
            let (share, pending) = allocation.row(weights).unwrap();
            assert!(pending.is_empty());
            shares = shares.checked_add(&share.fee).unwrap();
            let nanos = share.deduced("USD", 0, 0).unwrap().unwrap().nanos;
            rounded += nanos;
            assert_half_nano_bound(&share.fee, nanos);
            let empty = Weights::default();
            let a = first
                .row(first_rows.get(model).unwrap_or(&empty))
                .unwrap()
                .0;
            let b = second
                .row(second_rows.get(model).unwrap_or(&empty))
                .unwrap()
                .0;
            assert_eq!(a.fee.checked_add(&b.fee).unwrap(), share.fee);
            assert_eq!(a.by_rule.checked_add(&b.by_rule).unwrap(), share.by_rule);
        }
        let unspread = allocation.unspread_exact(|_| true).unwrap();
        assert_eq!(
            shares.checked_add(&unspread).unwrap(),
            fee_sum(&book),
            "case {case}"
        );
        assert_eq!(
            first
                .unspread_exact(|_| true)
                .unwrap()
                .checked_add(&second.unspread_exact(|_| true).unwrap())
                .unwrap(),
            unspread
        );
        let unspread_nanos = allocation
            .totals(true, |_| true)
            .unwrap()
            .deduced_unspread_nanos
            .unwrap();
        assert_half_nano_bound(&unspread, unspread_nanos);
        rounded += unspread_nanos;
        let fee = fee_sum(&book);
        let error = (BigInt::from(rounded) * fee.denominator() - fee.numerator()).abs();
        assert!(2_u8 * error <= fee.denominator() * BigInt::from(rows.len() + 1));
    }
}

#[test]
fn fee_fraction_rounds_once_per_row_half_even_and_rejects_only_final_money_overflow() {
    let share = fees::RowShare {
        fee: Fraction::new(1, 4)
            .unwrap()
            .checked_add(&Fraction::new(1, 4).unwrap())
            .unwrap(),
        by_rule: Fraction::new(1, 4).unwrap(),
    };
    assert_eq!(share.deduced("USD", 0, 0).unwrap().unwrap().nanos, 0);
    let row = share.deduced("USD", 1, 3).unwrap().unwrap();
    assert_eq!(row.nanos, 2);
    assert_eq!(row.pending_days, 3);
    assert_eq!(row.by_rule_share, Some(0.5));
    assert_half_nano_bound(&share.fee, 0);
    assert!(Fraction::new(1, 0).is_err());
    assert!(Fraction::new(i128::MIN, 1).is_ok());
    assert!(Fraction::new(i128::MAX, 1)
        .unwrap()
        .checked_add(&Fraction::new(1, 1).unwrap())
        .is_ok());
    assert!(Fraction::new(i128::MAX, 1)
        .unwrap()
        .checked_mul(&Fraction::new(2, 1).unwrap())
        .is_ok());
    assert!(Fraction::new(i128::MAX, 1).unwrap().round().is_err());
    for (numerator, denominator, expected) in [
        (-3, 2, -2),
        (-1, 2, 0),
        (-5, 2, -2),
        (-7, 2, -4),
        (3, 2, 2),
        (5, 2, 2),
    ] {
        assert_eq!(
            Fraction::new(numerator, denominator)
                .unwrap()
                .round()
                .unwrap(),
            expected
        );
    }
}

#[test]
fn fee_month_of_coprime_day_weights_keeps_exact_denominator_beyond_i128() {
    let mut conn = conn();
    let config = config();
    let start = utc(2024, 1, 1);
    let end = utc(2024, 2, 1);
    fee_plan(
        &conn,
        PlanLedger::Account,
        "provider",
        "account",
        start,
        Some(end),
        31,
        9,
        "USD",
        "month",
    );
    for model in ["target", "rest"] {
        rate(&conn, model, 1);
    }
    let primes = [
        5, 13, 17, 29, 37, 41, 53, 61, 73, 89, 97, 101, 109, 113, 137, 149, 157, 173, 181, 193,
        197, 229, 233, 241, 257, 269, 277, 281, 293, 313, 317,
    ];
    let mut independent = num_rational::BigRational::from_integer(BigInt::from(0));
    for (index, prime) in primes.into_iter().enumerate() {
        let at = start + i64::try_from(index).unwrap() * DAY_MS + 1;
        event(
            &mut conn,
            &format!("target-{index}"),
            at,
            "target",
            Some("account"),
            None,
            "unknown",
            1,
        );
        // These primes are 1 mod 4. With the fixture's output = input/2
        // and output rate = 2, this quantity gives exactly prime - 1 weight.
        event(
            &mut conn,
            &format!("rest-{index}"),
            at,
            "rest",
            Some("account"),
            None,
            "unknown",
            (prime - 1) / 2,
        );
        independent += num_rational::BigRational::new(BigInt::from(1), BigInt::from(prime));
    }
    summaries(&mut conn);
    drain(&mut conn, &config);
    let (day, rows) = fee_weights(&conn, start, end);
    let book = FeeDays::load(&conn, &config, start, end).unwrap();
    let allocation = book.allocate(&day, end).unwrap();
    let target = allocation.row(&rows["target"]).unwrap().0;
    assert!(
        target.fee.denominator().to_i128().is_none(),
        "fixture must exceed a fixed-width common denominator"
    );
    assert_eq!(target.fee.numerator(), independent.numer());
    assert_eq!(target.fee.denominator(), independent.denom());
    let rest = allocation.row(&rows["rest"]).unwrap().0;
    assert_eq!(
        target.fee.checked_add(&rest.fee).unwrap(),
        Fraction::new(31, 1).unwrap()
    );
    assert_half_nano_bound(
        &target.fee,
        target.deduced("USD", 0, 0).unwrap().unwrap().nanos,
    );
}

#[test]
fn fee_unfiltered_day_weights_give_proportional_record_and_rule_shares_in_subday_rows() {
    let mut conn = conn();
    let config = config();
    let day = utc(2024, 4, 1);
    let end = day + DAY_MS;
    for identity in ["account", "empty"] {
        fee_plan(
            &conn,
            PlanLedger::Account,
            "provider",
            identity,
            day,
            Some(end),
            150,
            9,
            "USD",
            "month",
        );
    }
    for model in ["X", "Y"] {
        rate(&conn, model, 1);
    }
    for (model, at, quantity) in [("X", day + 1, 1), ("Y", day + DAY_MS / 2 + 1, 2)] {
        event(
            &mut conn,
            &format!("record-{model}"),
            at,
            model,
            Some("account"),
            None,
            "unknown",
            quantity,
        );
        event(
            &mut conn,
            &format!("rule-{model}"),
            at,
            model,
            None,
            None,
            "unknown",
            quantity,
        );
    }
    summaries(&mut conn);
    drain(&mut conn, &config);
    let (day_weights, rows) = fee_weights(&conn, day, end);
    let full = FeeDays::load(&conn, &config, day, end)
        .unwrap()
        .allocate(&day_weights, end)
        .unwrap();
    // In each stage the two models weigh 1 and 4. Each stage has five nanos
    // to share, so their mixed rows receive two and eight nanos respectively.
    for (model, expected) in [("X", 2), ("Y", 8)] {
        let share = full.row(&rows[model]).unwrap().0;
        assert_eq!(share.fee, Fraction::new(expected, 1).unwrap());
        assert_eq!(
            share.deduced("USD", 0, 0).unwrap().unwrap().by_rule_share,
            Some(0.5)
        );
    }
    let (_, partial_rows) = fee_weights(&conn, day, day + DAY_MS / 2);
    let partial = FeeDays::load(&conn, &config, day, day + DAY_MS / 2)
        .unwrap()
        .allocate(&day_weights, end)
        .unwrap();
    assert_eq!(partial_rows.len(), 1);
    assert_eq!(
        partial.row(&partial_rows["X"]).unwrap().0.fee,
        Fraction::new(2, 1).unwrap()
    );
}
