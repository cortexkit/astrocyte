use crate::{ops, state::SharedState};
use astrocyte_core::multiplier::{
    compute_plan_observations, insert_observation_in_tx, plan_observation, ModelRates,
    ModelResolution, MultiplierConfig, PlanPriceUsed, QuotaProviderConfig, RateState,
    ResolvedRates, ScopeRule, WindowSelector,
};
use rusqlite::{params, Connection};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

const DIR: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/test-vectors/spend-multiplier-report-v1"
);
const HOUR: i64 = 3_600_000;
const DAY: i64 = 24 * HOUR;
const ACCOUNT: &str = "synthetic-account-multiplier";
const API: &str = "synthetic-api";
const MODEL: &str = "synthetic-model";

#[derive(Clone, Copy)]
enum Case {
    Unestablished,
    LowResolution,
    Eligible,
}

impl Case {
    fn provider(self) -> &'static str {
        match self {
            Self::Unestablished => "claude",
            Self::LowResolution | Self::Eligible => "codex",
        }
    }

    fn plan(self) -> &'static str {
        match self {
            Self::Unestablished => "synthetic/quota-unknown",
            Self::LowResolution => "synthetic/sparse",
            Self::Eligible => "synthetic/steady",
        }
    }
}

fn seed_inputs(conn: &Connection, case: Case) {
    conn.execute(
        "INSERT INTO vault_plan_history(credential_id,provider,account_id,plan_tier,plan_key,
             source,effective_from_ms,observed_at_ms)
         VALUES('synthetic-credential',?1,?2,'synthetic-tier',?3,'detected',0,0)",
        params![API, ACCOUNT, case.plan()],
    )
    .unwrap();
    // Changing endpoints eight days apart identify the pool but cannot bracket
    // any hour with readings at most fifteen minutes apart. The other cases
    // have those bounded reading pairs throughout eight synthetic days.
    for quarter in 0..=8 * 24 * 4 {
        if matches!(case, Case::Unestablished) && quarter != 0 && quarter != 8 * 24 * 4 {
            continue;
        }
        let at_ms = DAY + i64::from(quarter) * HOUR / 4;
        let divisor = if matches!(case, Case::LowResolution) {
            128.0
        } else {
            16.0
        };
        conn.execute(
            "INSERT INTO window_sample(provider,account_ref,pool_key,fetched_at_instant,
                 used_percent,window_minutes,resets_at,source)
             VALUES(?1,?2,'primary',?3,?4,10080,'1970-01-21T00:00:00Z','vault')",
            params![
                case.provider(),
                ACCOUNT,
                at_ms * 1_000_000,
                f64::from(quarter) / divisor
            ],
        )
        .unwrap();
    }
    // Equal value in every six-hour bin makes stability a property of the
    // seeded observations, not an eligibility flag forced onto a fake row.
    for hour in (0..8 * 24).step_by(6) {
        let at_ms = DAY + (hour + 3) * HOUR;
        let id = format!("synthetic-event-{hour}");
        conn.execute(
            "INSERT INTO usage_event(event_id,producer,producer_lineage,producer_ref,
                 occurred_at_ms,recorded_at_ms,provider,model,charge_basis,account_ref)
             VALUES(?1,'synthetic-producer','synthetic-lineage',?1,?2,?2,?3,?4,
                 'subscription_included',?5)",
            params![id, at_ms, API, MODEL, ACCOUNT],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO measurement(event_key,role,unit,kind,source,quantity_units,quantity_exponent)
             VALUES((SELECT event_key FROM usage_event WHERE event_id=?1),
                 'input','token','billed','input',250000,0)",
            [id],
        )
        .unwrap();
    }
}

fn seed(state: &SharedState, case: Case) {
    state
        .with_conn(|conn| {
            seed_inputs(conn, case);
            let config = MultiplierConfig {
                api_twins: BTreeMap::new(),
                plan_price_overrides: BTreeMap::new(),
                quota_providers: BTreeMap::from([(
                    case.provider().into(),
                    QuotaProviderConfig {
                        window: WindowSelector {
                            pool_key: "primary".into(),
                            window_minutes: 10080,
                        },
                        scope: vec![ScopeRule {
                            producer: "synthetic-producer".into(),
                            provider: API.into(),
                            charge_basis: Some("subscription_included".into()),
                            model_prefix: None,
                        }],
                    },
                )]),
            };
            let rates = ResolvedRates {
                resolved_at_ms: 9 * DAY,
                models: BTreeMap::from([(
                    (API.into(), MODEL.into()),
                    ModelResolution::Rates(ModelRates {
                        base: BTreeMap::from([(
                            "input".into(),
                            RateState::Priced {
                                units_per_million: 1_000_000_000,
                            },
                        )]),
                        ..ModelRates::default()
                    }),
                )]),
                eras: BTreeMap::new(),
            };
            let prices = BTreeMap::from([(
                ACCOUNT.into(),
                PlanPriceUsed {
                    account_ref: ACCOUNT.into(),
                    credential_ref: None,
                    plan: case.plan().into(),
                    source: Some("plan_override".into()),
                    monthly_nanos: Some(30_000_000_000),
                    review_by_ms: None,
                    plan_price_overdue: false,
                    source_ref: None,
                    unpriceable_reason: None,
                    refusal_text: None,
                },
            )]);
            let tx = conn.unchecked_transaction().unwrap();
            for end in [8 * DAY, 9 * DAY] {
                let plan =
                    plan_observation(&tx, &config, case.provider(), end - 7 * DAY, end).unwrap();
                let rows =
                    compute_plan_observations(&tx, &config, &plan, &rates, &prices, 9 * DAY, &[])
                        .unwrap();
                assert_eq!(rows.len(), 1);
                insert_observation_in_tx(&tx, &rows[0]).unwrap();
            }
            tx.commit().unwrap();
            Ok(())
        })
        .unwrap();
}

fn served(state: &SharedState, from_ms: i64, to_ms: i64) -> Value {
    let result = ops::dispatch(
        state,
        "spend.multiplier.report",
        json!({"from_ms": from_ms, "to_ms": to_ms}),
    )
    .unwrap();
    json!({"result": result})
}

fn assert_case(body: &Value, case: Case) {
    let result = &body["result"];
    assert_eq!(result["latest"].as_array().unwrap().len(), 1);
    assert_eq!(result["series"].as_array().unwrap().len(), 2);
    let mut latest = result["latest"][0].clone();
    let last_eligible = latest.as_object_mut().unwrap().remove("last_eligible");
    if matches!(case, Case::Eligible) {
        let last_eligible = last_eligible.expect("eligible latest row describes itself");
        assert_eq!(last_eligible["day"], latest["observation_day"]);
        assert_eq!(last_eligible["interval_end_ms"], latest["interval_end_ms"]);
        assert_eq!(last_eligible["multiplier"], latest["multiplier"]);
        assert_eq!(last_eligible["fee_multiplier"], latest["fee_multiplier"]);
    } else {
        assert!(last_eligible.is_none());
    }
    assert_eq!(latest, result["series"][1]);
    for row in result["series"].as_array().unwrap() {
        assert_eq!(row["quota_provider"], case.provider());
        assert_eq!(row["plan"], case.plan());
        assert_eq!(row["accounts_included"][0]["account_ref"], ACCOUNT);
        assert_eq!(row["unknown_quantity_measurements"], 0);
        match case {
            Case::Unestablished => {
                assert_eq!(row["routing_eligible"], false);
                assert!(row["multiplier"].is_null());
                assert!(row["fee_multiplier"].is_number());
                assert!(row["eligibility_reasons"]
                    .as_array()
                    .unwrap()
                    .contains(&json!("insufficient_clean_hours")));
            }
            Case::LowResolution => {
                assert_eq!(row["low_resolution"], true);
                assert_eq!(row["routing_eligible"], false);
                assert!(row["multiplier"].is_number());
                assert_eq!(row["eligibility_reasons"], json!(["insufficient_points"]));
            }
            Case::Eligible => {
                assert_eq!(row["routing_eligible"], true);
                assert_eq!(row["low_resolution"], false);
                assert_eq!(row["eligibility_reasons"], json!([]));
                assert!(row["multiplier"].as_f64().unwrap() > 0.0);
                assert!(row["api_nanos"].as_i64().unwrap() > 0);
                assert_eq!(row["api_nanos_by_model"][0]["model"], MODEL);
                assert_eq!(row["stability_diagnostics"]["active_days"], 7);
            }
        }
    }
}

fn pin_or_export(name: &str, bytes: &[u8], export: bool) {
    let path = format!("{DIR}/{name}");
    if export {
        if let Err(error) = std::fs::write(&path, bytes) {
            eprintln!("Cannot export {name} into the checkout: {error}");
            println!(
                "VECTOR {name} {}",
                serde_json::to_string(std::str::from_utf8(bytes).unwrap()).unwrap()
            );
        }
    } else {
        assert!(
            std::fs::read(path).unwrap() == bytes,
            "{name}: pinned bytes differ from the real spend.multiplier.report reply"
        );
    }
}

#[test]
fn multiplier_report_vectors_pin_real_dispatch_and_checksums() {
    let mut vectors = Vec::new();
    for (name, case) in [
        ("quota-unestablished.json", Case::Unestablished),
        ("low-resolution.json", Case::LowResolution),
        ("routing-eligible.json", Case::Eligible),
    ] {
        let directory = tempfile::tempdir().unwrap();
        let state = crate::state::leased_test_state(&directory);
        seed(&state, case);
        let changes = state.with_conn(|conn| Ok(conn.total_changes())).unwrap();
        let body = served(&state, 0, 9 * DAY + 1);
        assert_case(&body, case);
        if matches!(case, Case::Eligible) {
            let narrow = served(&state, 8 * DAY, 8 * DAY + 1);
            assert_eq!(narrow["result"]["latest"], body["result"]["latest"]);
            assert_eq!(
                narrow["result"]["series"],
                json!([body["result"]["series"][0]])
            );
            assert_eq!(narrow["result"]["series"][0]["interval_end_ms"], 8 * DAY);
            vectors.push(("narrow-range.json", narrow));
        }
        assert_eq!(
            state.with_conn(|conn| Ok(conn.total_changes())).unwrap(),
            changes,
            "serving a report must not write its store"
        );
        vectors.push((name, body));
    }
    let export = std::env::var_os("ASTROCYTE_EXPORT_MULTIPLIER_REPORT_VECTORS").is_some();
    let mut hashes = String::new();
    for (name, value) in vectors {
        let mut bytes = serde_json::to_vec(&value).unwrap();
        bytes.push(b'\n');
        hashes.push_str(&format!("{:x}  {name}\n", Sha256::digest(&bytes)));
        pin_or_export(name, &bytes, export);
    }
    hashes.push_str(&format!(
        "{:x}  README.md\n",
        Sha256::digest(std::fs::read(format!("{DIR}/README.md")).unwrap())
    ));
    pin_or_export("SHA256SUMS", hashes.as_bytes(), export);
}
