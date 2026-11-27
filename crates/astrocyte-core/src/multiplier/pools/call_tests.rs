use std::sync::Arc;

use fusiform_protocol::{CatalogGetRequest, CatalogGetResponse, StatusRequest, StatusResponse};

use crate::catalog_client::{CatalogCallError, CatalogClient, CatalogConnection};
use crate::fact::ChargeBasis;
use crate::ingest::{correct_basis_in_tx, BasisCorrectRequest};
use crate::multiplier::{ModelLookup, ModelResolution};
use crate::subscription;
use crate::valuation::value_event;

use super::*;

pub(super) const INCLUDED: &str = "subscription_included";
pub(super) const MATERIAL: i64 = 23_000;

pub(super) fn call(
    conn: &Connection,
    id: &str,
    (provider, model): (Option<&str>, Option<&str>),
    (account, resolved, selected): (Option<&str>, Option<&str>, Option<&str>),
    (at, basis): (i64, &str),
) -> i64 {
    let key = conn
        .query_row(
            "INSERT INTO usage_event (event_id, producer, producer_ref, occurred_at_ms,
                    recorded_at_ms, provider, model, account_ref, resolved_credential,
                    auth_selection, charge_basis)
             VALUES (?1, 'fixture-producer', ?1, ?2, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
             RETURNING event_key",
            params![id, at, provider, model, account, resolved, selected, basis],
            |row| row.get(0),
        )
        .unwrap();
    conn.execute(
        "INSERT INTO measurement (event_key,role,unit,kind,source,quantity_units,quantity_exponent)
         VALUES (?1,'input','token','billed','fixture',?2,0)",
        params![key, MATERIAL],
    )
    .unwrap();
    key
}

pub(super) fn capture_day(conn: &Connection) -> PoolDayCapture {
    let tx = conn.unchecked_transaction().unwrap();
    let day = capture_pool_day(&tx, PROVIDER, START, END).unwrap();
    tx.commit().unwrap();
    day
}

fn selected_ids(day: &PoolDayCapture, pool: &PlannedPool<'_>) -> Vec<i64> {
    day.select_calls(pool, &PoolModelIdentities::default())
        .unwrap()
        .calls
        .into_iter()
        .map(|call| call.id)
        .collect()
}

#[test]
fn corrected_basis_and_vault_plan_at_call_instant_decide_provider_day_traffic() {
    let conn = fixture();
    pair(&conn, "primary", WEEK, ALL);
    let planless = (PlanLedger::Account, "planless");
    let late = (PlanLedger::Account, "late");
    let credential_two = (PlanLedger::Credential, "login-two");
    let other_plan = "anthropic/other";
    vault_plan(&conn, late, Some(PLAN), START + 2);
    vault_plan(&conn, CREDENTIAL, Some(PLAN), START);
    vault_plan(&conn, credential_two, Some(other_plan), START);
    for login in [late, CREDENTIAL, credential_two] {
        for (at, used) in [(START + 2, 10), (END, 40)] {
            sample(&conn, login, (PROVIDER, "primary"), (at, used), (WEEK, ALL));
        }
    }
    let event = |id, model, login, at, basis| {
        call(
            &conn,
            id,
            (Some(CATALOG_PROVIDER), Some(model)),
            login,
            (at, basis),
        )
    };
    let account = (Some(ACCOUNT.1), None, None);
    let included = event("included", "any-model", account, START, INCLUDED);
    let corrected = event("corrected", "corrected-model", account, START + 1, INCLUDED);
    event("metered", "metered-model", account, START, "metered_api");
    event(
        "no-login",
        "no-login-model",
        (None, None, None),
        START,
        INCLUDED,
    );
    event(
        "planless",
        "planless-model",
        (Some(planless.1), None, None),
        START,
        INCLUDED,
    );
    event(
        "not-yet",
        "too-early-model",
        (Some(late.1), None, None),
        START + 1,
        INCLUDED,
    );
    let starts_now = event(
        "now",
        "at-boundary",
        (Some(late.1), None, None),
        START + 2,
        INCLUDED,
    );
    let resolved = event(
        "resolved",
        "any-model",
        (None, Some(CREDENTIAL.1), Some(credential_two.1)),
        START + 2,
        INCLUDED,
    );
    let fallback = event(
        "fallback",
        "any-model",
        (None, None, Some(credential_two.1)),
        START + 2,
        INCLUDED,
    );
    event("end-exclusive", "end-model", account, END, INCLUDED);

    let before = capture_day(&conn);
    let pools = plan_pools(&before.readings, &[]);
    assert_eq!(pools.len(), 2);
    let pool = pools.iter().find(|pool| pool.plan == PLAN).unwrap();
    assert_eq!(pool.quota_provider, PROVIDER);
    assert_eq!(
        selected_ids(&before, pool),
        [included, corrected, starts_now, resolved]
    );

    let request = BasisCorrectRequest {
        event_id: "corrected".into(),
        corrected_basis: ChargeBasis::Unknown,
        reason: "fixture correction".into(),
    };
    let correction = subscription::read_basis_correction(&conn, &request).unwrap();
    let resolution = subscription::resolve_reprice_without_catalog(&correction).unwrap();
    let tx = conn.unchecked_transaction().unwrap();
    correct_basis_in_tx(&tx, &correction, &resolution, END).unwrap();
    tx.commit().unwrap();

    let without_planless_readings = capture_day(&conn);
    for (at, used) in [(START, 25), (END, 75)] {
        sample(
            &conn,
            planless,
            (PROVIDER, "primary"),
            (at, used),
            (WEEK, ALL),
        );
        sample(
            &conn,
            planless,
            (PROVIDER, "secondary"),
            (at, used),
            (WEEK, ALL),
        );
    }
    let after = capture_day(&conn);
    let pools = plan_pools(&after.readings, &[]);
    // Planless readings cannot add a row or change any sibling input.
    assert_eq!(pools, plan_pools(&without_planless_readings.readings, &[]));
    let pool = pools.iter().find(|pool| pool.plan == PLAN).unwrap();
    let selection = after
        .select_calls(pool, &PoolModelIdentities::default())
        .unwrap();
    assert_eq!(
        selection
            .calls
            .iter()
            .map(|call| call.id)
            .collect::<Vec<_>>(),
        [included, starts_now, resolved]
    );
    assert_eq!(selection.calls[2].login.ledger, PlanLedger::Credential);
    assert_eq!(selection.calls[0].tokens.roles[0].3, Some(MATERIAL));
    assert_eq!(
        selected_ids(
            &after,
            pools.iter().find(|pool| pool.plan == other_plan).unwrap()
        ),
        [fallback]
    );
    assert_eq!(
        after.models_for_pools(&pools),
        BTreeSet::from([
            (CATALOG_PROVIDER.into(), "any-model".into()),
            (CATALOG_PROVIDER.into(), "at-boundary".into()),
        ])
    );
    // The older owned snapshot stays unchanged after a correction.
    let old_pools = plan_pools(&before.readings, &[]);
    let old = old_pools.iter().find(|pool| pool.plan == PLAN).unwrap();
    assert_eq!(
        selected_ids(&before, old),
        [included, corrected, starts_now, resolved]
    );
}

struct Catalog;

impl CatalogConnection for Catalog {
    fn catalog_status(&self, _: StatusRequest) -> Result<StatusResponse, CatalogCallError> {
        unreachable!("the fixture supplies the history boundary")
    }

    fn catalog_get(
        &self,
        request: CatalogGetRequest,
    ) -> Result<CatalogGetResponse, CatalogCallError> {
        let model = request.model_id.as_deref().unwrap();
        assert_eq!(request.provider_id.as_deref(), Some(CATALOG_PROVIDER));
        let mut rate = json!({"state":"priced","units":2_000_000_000_i64,"exponent":9,
            "currency":"USD","unit_provenance":{"kind":"stated"}});
        let current = request.at_ms.is_none();
        match (model, current, request.auth_method.as_deref()) {
            ("fable" | "exact-low", _, None) => (),
            ("alias" | "alias-listed", true, None) => {
                rate["inherited_from"] = json!({"provider_id": CATALOG_PROVIDER,
                    "model_id":"fable","family":"claude","basis":"alias"});
            }
            ("proxy", true, Some("apikey")) => {
                rate["inherited_from"] = json!({"provider_id": CATALOG_PROVIDER,
                    "model_id":"fable","family":"claude","basis":"quota_proxy"});
            }
            ("unpriced", _, None) | ("proxy", false, None) => {
                rate = json!({"state":"unpriced","reason":"missing_rate"});
            }
            _ => {
                return Err(CatalogCallError {
                    code: "no_coverage".into(),
                    message: "fixture has no such model".into(),
                });
            }
        }
        let key = format!("{CATALOG_PROVIDER}/{model}");
        Ok(
            serde_json::from_value(json!({"source":"fixture","resolved_at_ms": END,
            "catalog_version":1,"models":{key:{"rate.input":rate}}}))
            .unwrap(),
        )
    }
}

#[test]
fn subset_matches_exact_then_suffix_then_alias_but_never_quota_proxy() {
    let conn = fixture();
    pair(&conn, "primary", WEEK, ALL);
    pair(&conn, FABLE, WEEK, SUBSET);
    let lists = [list(
        CATALOG_PROVIDER,
        "Fable",
        &[
            "anthropic/fable",
            "anthropic/exact",
            "anthropic/unpriced",
            "anthropic/alias-listed",
        ],
    )];
    let models = [
        "fable",
        "fable-low",
        "fable-high",
        "exact-low",
        "proxy",
        "alias",
        "alias-listed",
        "unpriced",
        "unknown",
    ];
    for model in models {
        call(
            &conn,
            model,
            (Some(CATALOG_PROVIDER), Some(model)),
            (Some(ACCOUNT.1), None, None),
            (START, INCLUDED),
        );
    }
    let missing = call(
        &conn,
        "missing-provider",
        (None, Some("fable")),
        (Some(ACCOUNT.1), None, None),
        (START, INCLUDED),
    );
    let day = capture_day(&conn);
    let pools = plan_pools(&day.readings, &lists);
    let catalog = CatalogClient::new(Arc::new(Catalog), START);
    let keys = day.models_for_pools(&pools);
    assert_eq!(keys.len(), models.len());
    let (identities, rates) = resolve_pool_models(&catalog, &keys, START).unwrap();
    let lookup = ModelLookup::new(&rates);
    // The quota proxy uses the listed target's rate, but coverage is its own identity.
    assert!(
        matches!(lookup.get(CATALOG_PROVIDER, "proxy"), Some(ModelResolution::Rates(rates))
        if rates.reference_model.as_ref().unwrap().model_id == "fable")
    );

    let subset = by_id(&pools, "Fable");
    assert_eq!((subset.quota_provider, subset.plan), (PROVIDER, PLAN));
    let selected = day.select_calls(subset, &identities).unwrap();
    assert_eq!(
        selected
            .calls
            .iter()
            .filter_map(|call| call.tokens.model.as_deref())
            .collect::<Vec<_>>(),
        [
            "fable",
            "fable-low",
            "fable-high",
            "alias",
            "alias-listed",
            "unpriced"
        ]
    );
    assert_eq!(
        selected.pool_member_models.unwrap(),
        [
            "anthropic/alias",
            "anthropic/fable-high",
            "anthropic/fable-low"
        ]
    );
    let all = day
        .select_calls(by_id(&pools, "primary"), &identities)
        .unwrap();
    assert_eq!(all.calls.len(), models.len() + 1);
    assert_eq!(all.calls.last().unwrap().id, missing);
    assert_eq!(all.pool_member_models, None);

    let unpriced: Vec<_> = selected
        .calls
        .iter()
        .flat_map(|call| {
            let model = call.tokens.model.as_deref().unwrap();
            let resolution = lookup.get_at(&call.tokens.provider, model, call.occurred_at_ms);
            value_event(resolution, &call.tokens).unwrap().unpriced
        })
        .collect();
    assert_eq!(unpriced.len(), 1);
    assert_eq!(
        (unpriced[0].role.as_str(), unpriced[0].quantity),
        ("input", MATERIAL)
    );
}

#[test]
fn listed_models_before_catalog_history_keep_identity_and_unpriced_tokens() {
    let conn = fixture();
    pair(&conn, FABLE, WEEK, SUBSET);
    let lists = [list(
        CATALOG_PROVIDER,
        "Fable",
        &["anthropic/fable", "anthropic/list-only"],
    )];
    for model in ["fable", "fable-low", "alias", "list-only", "unknown"] {
        call(
            &conn,
            model,
            (Some(CATALOG_PROVIDER), Some(model)),
            (Some(ACCOUNT.1), None, None),
            (START, INCLUDED),
        );
    }
    let day = capture_day(&conn);
    let pools = plan_pools(&day.readings, &lists);
    let catalog = CatalogClient::new(Arc::new(Catalog), START + 1);
    let (identities, rates) =
        resolve_pool_models(&catalog, &day.models_for_pools(&pools), START).unwrap();
    let selection = day
        .select_calls(by_id(&pools, "Fable"), &identities)
        .unwrap();
    assert_eq!(
        selection
            .calls
            .iter()
            .filter_map(|call| call.tokens.model.as_deref())
            .collect::<Vec<_>>(),
        ["fable", "fable-low", "alias", "list-only"]
    );
    assert_eq!(
        selection.pool_member_models.unwrap(),
        ["anthropic/alias", "anthropic/fable-low"]
    );
    let lookup = ModelLookup::new(&rates);
    for call in selection.calls {
        let model = call.tokens.model.as_deref().unwrap();
        let resolution = lookup.get_at(&call.tokens.provider, model, call.occurred_at_ms);
        let valued = value_event(resolution, &call.tokens).unwrap();
        assert_eq!(valued.numerator, 0);
        assert_eq!(valued.unpriced.len(), 1);
        assert_eq!(valued.unpriced[0].reason, "catalog_pre_history");
        assert_eq!(valued.unpriced[0].quantity, MATERIAL);
    }
}
