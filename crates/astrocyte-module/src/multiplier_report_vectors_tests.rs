use crate::{bus_publications, ops, state::SharedState};
use astrocyte_core::catalog_client::{CatalogCallError, CatalogClient, CatalogConnection};
use astrocyte_core::multiplier::{
    insert_observation_in_tx, pools, provider_day::ProviderDay, utc_day,
};
use fusiform_protocol::{
    CatalogGetRequest, CatalogGetResponse, PlanAmount, PlanPriceWire, PlanPricesResponse,
    QuotaPoolWire, StatusRequest, StatusResponse,
};
use rusqlite::{params, Connection};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::Arc,
};

const DIR: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/test-vectors/spend-multiplier-report-v2"
);
const HOUR: i64 = 3_600_000;
const DAY: i64 = 24 * HOUR;
const END: i64 = 9 * DAY;

struct Pool {
    id: &'static str,
    scope: &'static str,
    model: Option<&'static str>,
}

const PRIMARY: Pool = Pool {
    id: "primary",
    scope: "all",
    model: None,
};

struct Plan {
    quota_provider: &'static str,
    catalog: &'static str,
    tier: &'static str,
    reading_catalog: Option<&'static str>,
    pools: &'static [Pool],
    models: &'static [&'static str],
}

impl Plan {
    fn key(&self) -> String {
        format!("{}/{}", self.catalog, self.tier)
    }
}

struct VectorCatalog;

impl CatalogConnection for VectorCatalog {
    fn catalog_status(&self, _: StatusRequest) -> Result<StatusResponse, CatalogCallError> {
        Ok(
            serde_json::from_value(json!({"source":"synthetic", "catalog_version":1,
            "model_count":0,"era_count":0,"recent_polls":[],"history_begins_at_ms":0}))
            .unwrap(),
        )
    }

    fn catalog_get(
        &self,
        request: CatalogGetRequest,
    ) -> Result<CatalogGetResponse, CatalogCallError> {
        let model = format!(
            "{}/{}",
            request.provider_id.unwrap(),
            request.model_id.unwrap()
        );
        let rate = json!({"state":"priced","units":1_000_000_000,"exponent":9,
            "currency":"USD","unit_provenance":{"kind":"stated"}});
        Ok(CatalogGetResponse {
            overrides: vec![],
            source: "synthetic".into(),
            resolved_at_ms: request.at_ms.unwrap(),
            catalog_version: 1,
            models: BTreeMap::from([(model, BTreeMap::from([("rate.input".into(), rate)]))]),
            withheld: vec![],
            uncertain: vec![],
            next_page_token: None,
            curated: vec![],
            quality_scores: None,
            canonical_links: vec![],
            wire_corrections: vec![],
            fact_overrides: vec![],
            retired: vec![],
            aliased: vec![],
            plane: None,
            billing_rules: vec![],
            login_methods: vec![],
            speed_twins: vec![],
        })
    }
}

fn seed_inputs(conn: &Connection, plan: &Plan) {
    let account = format!("synthetic-{}-account", plan.quota_provider);
    conn.execute(
        "INSERT INTO vault_plan_history(credential_id,provider,account_id,plan_tier,plan_key,
             source,effective_from_ms,observed_at_ms)
         VALUES(?1,?2,?3,?4,?5,'detected',0,0)",
        params![
            format!("oauth:{account}"),
            plan.catalog,
            account,
            plan.tier,
            plan.key()
        ],
    )
    .unwrap();
    for pool in plan.pools {
        let key = if pool.id == "primary" {
            pool.id.into()
        } else {
            format!("extra:{}:{}", pool.id.len(), pool.id)
        };
        for quarter in 0..=8 * 24 * 4 {
            let at_ms = DAY + i64::from(quarter) * HOUR / 4;
            conn.execute(
                "INSERT INTO window_sample(provider,account_ref,pool_key,fetched_at_instant,
                     used_percent,window_minutes,resets_at,source,model_scope,api_provider)
                 VALUES(?1,?2,?3,?4,?5,10080,'1970-01-21T00:00:00Z','oauth',?6,?7)",
                params![
                    plan.quota_provider,
                    account,
                    key,
                    at_ms * 1_000_000,
                    f64::from(quarter) / 16.0,
                    pool.scope,
                    plan.reading_catalog
                ],
            )
            .unwrap();
        }
    }
    // Equal value in each six-hour bin gives the measured pools stable data.
    for hour in (0..8 * 24).step_by(6) {
        for model in plan.models {
            let at_ms = DAY + (hour + 3) * HOUR;
            let id = format!("{}-{model}-{hour}", plan.quota_provider);
            conn.execute(
                "INSERT INTO usage_event(event_id,producer,producer_lineage,producer_ref,
                    occurred_at_ms,recorded_at_ms,provider,model,charge_basis,account_ref)
                 VALUES(?1,'synthetic-producer','synthetic-lineage',?1,?2,?2,?3,?4,
                    'subscription_included',?5)",
                params![id, at_ms, plan.catalog, model, account],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO measurement(event_key,role,unit,kind,source,quantity_units,quantity_exponent)
                 VALUES((SELECT event_key FROM usage_event WHERE event_id=?1),
                    'input','token','billed','input',250000,0)", [id],
            ).unwrap();
        }
    }
}

fn seed(state: &SharedState, plan: &Plan, ends: &[i64], legacy: bool) {
    state.with_conn(|conn| {
        seed_inputs(conn, plan);
        let catalog = CatalogClient::connect(Arc::new(VectorCatalog)).unwrap();
        let models = plan.models.iter().map(|model| (plan.catalog.into(), (*model).into())).collect();
        let (identities, rates) = pools::resolve_pool_models(&catalog, &models, END).unwrap();
        let prices = PlanPricesResponse {
            prices: vec![PlanPriceWire {
                provider_id: plan.catalog.into(), tier: plan.tier.into(),
                price: Some(PlanAmount { minor_units: 30, exponent: 0, currency: "USD".into(), period: "month".into() }),
                quota_value: None, service_tiers: None, concurrency: None, refusal_reason: None,
                boundary_at_ms: 0, established_by: "synthetic".into(), established_at_ms: 0,
                review_by_ms: END + DAY, source_ref: "https://example.invalid/prices".into(),
            }],
            quota_pools: plan.pools.iter().filter_map(|pool| {
                Some(QuotaPoolWire {
                    provider_id: plan.catalog.into(), pool_id: pool.id.into(),
                    models: vec![format!("{}/{}", plan.catalog, pool.model?)],
                    source_ref: "https://example.invalid/pools".into(), quote: "synthetic coverage".into(),
                    established_at_ms: 0, review_by_ms: END + DAY,
                })
            }).collect(),
            resolved_at_ms: END, tier_vocabulary: "vendor".into(), unit_policy: "list".into(),
        };
        let tx = conn.unchecked_transaction().unwrap();
        tx.execute("UPDATE cortexkit_schema_version SET applied_at_unix=0 WHERE namespace='astrocyte' AND version=68", []).unwrap();
        for &end in ends {
            let start = end - 7 * DAY;
            let capture = pools::capture_pool_day(&tx, plan.quota_provider, start, end).unwrap();
            let planned = pools::plan_pools(&capture.readings, &prices.quota_pools);
            let mut rows = pools::compute_pool_observations(&capture, &planned, &rates, &identities, &prices, END).unwrap();
            assert_eq!(rows.len(), plan.pools.len());
            if legacy {
                assert_eq!(rows.len(), 1);
                rows[0].pool_scope = None;
                rows[0].catalog_provider = None;
                insert_observation_in_tx(&tx, &rows[0]).unwrap();
            } else {
                let day = utc_day(end).unwrap();
                assert!(ProviderDay { provider: plan.quota_provider, day: &day, interval_start_ms: start, interval_end_ms: end }
                    .record(&tx, END, &rows).unwrap());
            }
        }
        tx.commit().unwrap();
        Ok(())
    }).unwrap();
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

fn json_bytes(value: &Value) -> Vec<u8> {
    let mut bytes = serde_json::to_vec(value).unwrap();
    bytes.push(b'\n');
    bytes
}

fn pin_or_export(name: &str, bytes: &[u8], export_to: Option<&Path>) {
    if let Some(dir) = export_to {
        std::fs::write(dir.join(name), bytes).unwrap();
    } else {
        assert_eq!(
            std::fs::read(format!("{DIR}/{name}")).unwrap(),
            bytes,
            "{name}: pinned bytes differ from the real report or publication"
        );
    }
}

#[tokio::test]
async fn multiplier_report_vectors_pin_real_dispatch_and_checksums() {
    use futures_util::TryStreamExt;
    let export_to = std::env::var_os("ASTROCYTE_EXPORT_MULTIPLIER_REPORT_VECTORS").map(|value| {
        if value == "1" {
            PathBuf::from(DIR)
        } else {
            PathBuf::from(value)
        }
    });
    let directory = tempfile::tempdir().unwrap();
    let state = crate::state::leased_test_state(&directory);
    seed(
        &state,
        &Plan {
            quota_provider: "legacy",
            catalog: "synthetic",
            tier: "historical",
            reading_catalog: None,
            pools: &[PRIMARY],
            models: &["legacy-model"],
        },
        &[END - DAY],
        true,
    );
    let legacy = served(&state, 0, END + 1);
    assert_eq!(legacy["result"]["latest"], json!([]));
    let legacy_row = &legacy["result"]["series"][0];
    assert_eq!(legacy_row["pool_id"], "primary");
    assert!(legacy_row["pool_scope"].is_null());
    assert!(legacy_row["catalog_provider"].is_null());

    bus_publications::publish_multiplier(&state);
    let bus = bus_publications::tests::TestBus::start(&state).await;
    if let Some(bus) = &bus {
        assert!(bus
            .multiplier
            .keys()
            .await
            .unwrap()
            .try_next()
            .await
            .unwrap()
            .is_none());
    }
    seed(
        &state,
        &Plan {
            quota_provider: "claude",
            catalog: "anthropic",
            tier: "max_20x",
            reading_catalog: Some("anthropic"),
            pools: &[
                PRIMARY,
                Pool {
                    id: "Fable",
                    scope: "subset",
                    model: Some("fable"),
                },
            ],
            models: &["fable", "sonnet"],
        },
        &[END],
        false,
    );
    seed(
        &state,
        &Plan {
            quota_provider: "antigravity",
            catalog: "google",
            tier: "ultra",
            reading_catalog: Some("google"),
            pools: &[
                Pool {
                    id: "Gemini Pro",
                    scope: "subset",
                    model: Some("gemini-pro"),
                },
                Pool {
                    id: "Gemini Flash",
                    scope: "subset",
                    model: Some("gemini-flash"),
                },
                Pool {
                    id: "Gemini Preview",
                    scope: "subset",
                    model: None,
                },
            ],
            models: &["gemini-pro", "gemini-flash"],
        },
        &[END],
        false,
    );
    seed(
        &state,
        &Plan {
            quota_provider: "codex",
            catalog: "openai",
            tier: "pro_20x",
            reading_catalog: None,
            pools: &[PRIMARY],
            models: &["synthetic-model"],
        },
        &[END - DAY, END],
        false,
    );

    let changes = state.with_conn(|conn| Ok(conn.total_changes())).unwrap();
    let body = served(&state, 0, END + 1);
    let narrow = served(&state, END - DAY, END - DAY + 1);
    assert_eq!(
        state.with_conn(|conn| Ok(conn.total_changes())).unwrap(),
        changes
    );
    let latest = body["result"]["latest"].as_array().unwrap();
    let series = body["result"]["series"].as_array().unwrap();
    assert_eq!(latest.len(), 6);
    assert_eq!(series.len(), 8);
    assert_eq!(narrow["result"]["latest"], body["result"]["latest"]);
    assert!(narrow["result"]["series"]
        .as_array()
        .unwrap()
        .iter()
        .all(|row| row["interval_end_ms"] == END - DAY));
    assert_eq!(narrow["result"]["series"].as_array().unwrap().len(), 2);

    for row in latest {
        let mut history = row.clone();
        let last_eligible = history.as_object_mut().unwrap().remove("last_eligible");
        assert!(series.contains(&history));
        if row["pool_id"] == "Gemini Preview" {
            assert_eq!(row["eligibility_reasons"], json!(["pool_list_missing"]));
            assert_eq!(row["pool_scope"], "subset");
            assert!(row["pool_models"].is_null());
            assert!(row["multiplier"].is_null());
            assert!(last_eligible.is_none());
        } else {
            assert_eq!(
                row["routing_eligible"], true,
                "{}: {}",
                row["pool_id"], row["eligibility_reasons"]
            );
            let eligible = last_eligible.unwrap();
            assert_eq!(eligible["day"], row["observation_day"]);
            assert_eq!(eligible["multiplier"], row["multiplier"]);
        }
    }
    let mut vectors = BTreeMap::from([
        ("legacy-only.json", json_bytes(&legacy)),
        ("pool-heads.json", json_bytes(&body)),
        ("narrow-range.json", json_bytes(&narrow)),
    ]);
    let Some(bus) = bus else {
        assert!(
            export_to.is_none(),
            "export requires nats-server for the bus vectors"
        );
        for (name, value) in vectors {
            pin_or_export(name, &value, None);
        }
        return;
    };
    let before = bus.attempts();
    bus_publications::publish_multiplier(&state);
    bus.after(before).await;
    let keys: Vec<String> = bus
        .multiplier
        .keys()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(keys.len(), latest.len());
    let mut key_vector = BTreeMap::new();
    for (file, provider, plan, pool, catalog, scope) in [
        (
            "kv-claude-all.json",
            "claude",
            "anthropic/max_20x",
            "primary",
            Some("anthropic"),
            "all",
        ),
        (
            "kv-claude-fable.json",
            "claude",
            "anthropic/max_20x",
            "Fable",
            Some("anthropic"),
            "subset",
        ),
        (
            "kv-gemini-pro.json",
            "antigravity",
            "google/ultra",
            "Gemini Pro",
            Some("google"),
            "subset",
        ),
        (
            "kv-gemini-flash.json",
            "antigravity",
            "google/ultra",
            "Gemini Flash",
            Some("google"),
            "subset",
        ),
        (
            "kv-pool-list-missing.json",
            "antigravity",
            "google/ultra",
            "Gemini Preview",
            Some("google"),
            "subset",
        ),
        (
            "kv-single-pool.json",
            "codex",
            "openai/pro_20x",
            "primary",
            None,
            "all",
        ),
    ] {
        let key = [provider, plan, pool]
            .map(|part| {
                use std::fmt::Write;
                part.as_bytes()
                    .iter()
                    .fold("x".to_owned(), |mut encoded, byte| {
                        write!(encoded, "{byte:02x}").unwrap();
                        encoded
                    })
            })
            .join(".");
        assert!(keys.contains(&key), "missing bus key {key}");
        let row = latest
            .iter()
            .find(|row| {
                row["quota_provider"] == provider && row["plan"] == plan && row["pool_id"] == pool
            })
            .unwrap();
        assert_eq!(row["catalog_provider"], json!(catalog));
        assert_eq!(row["pool_scope"], scope);
        assert_eq!(row["window_minutes"], 10080);
        if scope == "subset" {
            assert_eq!(row["pool_models_at_ms"], END);
        }
        let bytes = bus.multiplier.get(&key).await.unwrap().unwrap().to_vec();
        assert_eq!(bytes, serde_json::to_vec(row).unwrap());
        key_vector.insert(file, key);
        vectors.insert(file, bytes);
    }
    bus.finish().await;
    vectors.insert("bus-keys.json", json_bytes(&json!(key_vector)));
    let mut hashes = String::new();
    for (name, bytes) in vectors {
        use std::fmt::Write;
        writeln!(hashes, "{:x}  {name}", Sha256::digest(&bytes)).unwrap();
        pin_or_export(name, &bytes, export_to.as_deref());
    }
    use std::fmt::Write;
    writeln!(
        hashes,
        "{:x}  README.md",
        Sha256::digest(std::fs::read(format!("{DIR}/README.md")).unwrap())
    )
    .unwrap();
    pin_or_export("SHA256SUMS", hashes.as_bytes(), export_to.as_deref());
}
