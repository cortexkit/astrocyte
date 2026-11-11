fn vault_account_membership_with_secondary(
    provider: Option<&str>,
    plan: Option<&str>,
    effective_from_ms: i64,
) -> ResolvedQuotaProvider {
    let mut conn = conn();
    let declaration = QuotaProviderConfig {
        window: window(),
        scope: vec![rule("broca", "meta", None)],
    };
    conn.execute(
        "INSERT INTO window_sample(provider,account_ref,pool_key,fetched_at_instant,
             used_percent,window_minutes) VALUES('muse','acct','primary',0,1,10080)",
        [],
    )
    .unwrap();
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
    resolve_membership(&conn, &declaration, "muse", 4).unwrap()
}

fn assert_vault_account_membership_ignores_empty_record(effective_from_ms: i64) {
    let resolved = vault_account_membership_with_secondary(None, None, effective_from_ms);
    assert!(resolved.unknown_reasons.is_empty(), "{resolved:?}");
    assert_eq!(resolved.accounts.len(), 1);
    assert_eq!(resolved.accounts["acct"].plan, "meta/power_usage");
    assert!(resolved.credentials.is_empty());
    assert!(!resolved.credential_keyed);
    assert!(resolved.membership_reason.is_none());
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
fn vault_account_membership_two_known_providers_are_ambiguous() {
    let resolved =
        vault_account_membership_with_secondary(Some("anthropic"), Some("anthropic/max"), 2);
    assert!(resolved.accounts.is_empty());
    assert!(resolved.credentials.is_empty());
    assert_eq!(
        resolved.unknown_reasons,
        BTreeMap::from([("acct".into(), "vault_account_ambiguous".into())])
    );
}

#[test]
fn vault_account_membership_known_provider_without_plan_stays_unknown() {
    let resolved = vault_account_membership_with_secondary(Some("meta"), None, 3);
    assert!(resolved.accounts.is_empty());
    assert!(resolved.credentials.is_empty());
    assert_eq!(
        resolved.unknown_reasons,
        BTreeMap::from([("acct".into(), "vault_plan_unknown".into())])
    );
}

#[test]
fn vault_account_membership_equal_time_conflicting_plans_are_ambiguous() {
    let resolved = vault_account_membership_with_secondary(Some("meta"), Some("meta/basic"), 2);
    assert!(resolved.accounts.is_empty());
    assert!(resolved.credentials.is_empty());
    assert_eq!(
        resolved.unknown_reasons,
        BTreeMap::from([("acct".into(), "vault_account_ambiguous".into())])
    );
}
