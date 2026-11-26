use super::*;
use crate::test_support::conn;

fn vault_account_membership_with_secondary(
    provider: Option<&str>,
    plan: Option<&str>,
    effective_from_ms: i64,
) -> Option<String> {
    let mut conn = conn();
    let tx = conn.transaction().unwrap();
    for (credential, provider, plan, source, effective) in [
        (
            "config:meta",
            Some("meta"),
            Some("meta/power_usage"),
            "config",
            0,
        ),
        (
            "oauth:meta",
            Some("meta"),
            Some("meta/power_usage"),
            "operator",
            2,
        ),
        ("apikey:muse", provider, plan, "operator", effective_from_ms),
    ] {
        crate::vault_plan::append_in_tx(
            &tx,
            &crate::vault_plan::VaultPlan {
                credential_id: credential.into(),
                provider: provider.map(str::to_string),
                account_id: Some("acct".into()),
                plan_tier: plan.and_then(|key| key.split_once('/').map(|(_, tier)| tier.into())),
                plan_key: plan.map(str::to_string),
                source: source.into(),
                effective_from_ms: effective,
                observed_at_ms: effective,
            },
        )
        .unwrap();
    }
    tx.commit().unwrap();
    let tx = conn.transaction().unwrap();
    VaultPlanTimeline::capture(&tx, (0, 4))
        .unwrap()
        .plan_at(PlanLedger::Account, "acct", 4)
        .map(str::to_owned)
}

fn assert_vault_account_membership_ignores_empty_record(effective_from_ms: i64) {
    let resolved = vault_account_membership_with_secondary(None, None, effective_from_ms);
    assert_eq!(resolved.as_deref(), Some("meta/power_usage"));
}

#[test]
fn vault_account_membership_ignores_older_providerless_planless_record() {
    assert_vault_account_membership_ignores_empty_record(1);
}

#[test]
fn vault_account_membership_ignores_equal_time_providerless_planless_record() {
    assert_vault_account_membership_ignores_empty_record(2);
}

#[test]
fn vault_account_membership_ignores_newer_providerless_planless_record() {
    assert_vault_account_membership_ignores_empty_record(3);
}

#[test]
fn vault_regression_real_provider_overrides_config_seed_for_the_same_account() {
    let mut conn = conn();
    let tx = conn.transaction().unwrap();
    for (credential, provider, tier, source, effective) in [
        ("config:acct", "anthropic", "max", "config", 0),
        ("oauth:acct", "openai", "pro", "operator", 1),
    ] {
        crate::vault_plan::append_in_tx(
            &tx,
            &crate::vault_plan::VaultPlan {
                credential_id: credential.into(),
                provider: Some(provider.into()),
                account_id: Some("acct".into()),
                plan_tier: Some(tier.into()),
                plan_key: Some(format!("{provider}/{tier}")),
                source: source.into(),
                effective_from_ms: effective,
                observed_at_ms: effective,
            },
        )
        .unwrap();
    }
    tx.commit().unwrap();
    let tx = conn.transaction().unwrap();
    let timeline = VaultPlanTimeline::capture(&tx, (0, 2)).unwrap();
    assert_eq!(
        timeline.plan_at(PlanLedger::Account, "acct", 0),
        Some("anthropic/max")
    );
    assert_eq!(
        timeline.plan_at(PlanLedger::Account, "acct", 1),
        Some("openai/pro")
    );
}

#[test]
fn vault_account_membership_two_known_providers_are_ambiguous() {
    let resolved =
        vault_account_membership_with_secondary(Some("anthropic"), Some("anthropic/max"), 2);
    assert_eq!(resolved, None);
}

#[test]
fn vault_account_membership_known_provider_without_plan_stays_unknown() {
    let resolved = vault_account_membership_with_secondary(Some("meta"), None, 3);
    assert_eq!(resolved, None);
}

#[test]
fn vault_account_membership_equal_time_conflicting_plans_are_ambiguous() {
    let resolved = vault_account_membership_with_secondary(Some("meta"), Some("meta/basic"), 2);
    assert_eq!(resolved, None);
}
