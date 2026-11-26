#[test]
fn post_cut_observation_writers_keep_one_chain_per_pool() {
    let mut conn = conn();
    let mut pre_cut = eligible_plan_observation(&conn);
    pre_cut.window_pool_key = "primary".into();
    let tx = conn.transaction().unwrap();
    insert_observation_in_tx(&tx, &pre_cut).unwrap();
    let pre_cut_original = tx.last_insert_rowid();
    pre_cut.window_pool_key = "secondary".into();
    let pre_cut_head = insert_superseding_observation_in_tx(&tx, &pre_cut).unwrap();
    assert_eq!(pre_cut_head.supersedes, pre_cut_original);

    let plan = pre_cut.plan.as_deref().unwrap();
    let head = |key: ObservationHeadKey<'_>| {
        observation_plan_head(&tx, &pre_cut.quota_provider, &pre_cut.observation_day, key).unwrap()
    };
    let pool_head = |window_pool_key| {
        head(ObservationHeadKey::PostCut {
            plan,
            window_pool_key,
        })
    };
    let mut primary = pre_cut.clone();
    primary.window_pool_key = "primary".into();
    primary.pool_scope = Some(PoolScope::All);
    let mut secondary = pre_cut.clone();
    secondary.pool_scope = Some(PoolScope::Subset);
    secondary.pool_models = Some(vec!["provider/model".into()]);
    secondary.pool_models_at_ms = Some(secondary.interval_end_ms);
    secondary.pool_member_models = Some(vec!["provider/model-low".into()]);

    assert_eq!(pool_head("secondary"), None);
    assert!(matches!(
        insert_superseding_observation_in_tx(&tx, &secondary),
        Err(MultiplierError::NothingToSupersede { .. })
    ));
    insert_observation_in_tx(&tx, &primary).unwrap();
    let primary_original = tx.last_insert_rowid();
    insert_observation_in_tx(&tx, &secondary).unwrap();
    let secondary_original = tx.last_insert_rowid();
    assert!(matches!(
        insert_observation_in_tx(&tx, &secondary),
        Err(MultiplierError::AlreadyRecorded { .. })
    ));
    assert_eq!(pool_head("primary"), Some(primary_original));
    assert_eq!(pool_head("secondary"), Some(secondary_original));

    let corrected_secondary = insert_superseding_observation_in_tx(&tx, &secondary).unwrap();
    let corrected_primary = insert_superseding_observation_in_tx(&tx, &primary).unwrap();
    assert_eq!(corrected_secondary.supersedes, secondary_original);
    assert_eq!(corrected_primary.supersedes, primary_original);
    assert_eq!(pool_head("primary"), Some(corrected_primary.observation_id));
    assert_eq!(
        pool_head("secondary"),
        Some(corrected_secondary.observation_id)
    );
    assert_eq!(
        head(ObservationHeadKey::PreCut { plan: Some(plan) }),
        Some(pre_cut_head.observation_id)
    );

    let rows: Vec<(i64, String, Option<String>, Option<i64>)> = tx
        .prepare(
            "SELECT observation_id, window_pool_key, pool_scope, supersedes
                  FROM multiplier_observation ORDER BY observation_id",
        )
        .unwrap()
        .query_map([], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        })
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    assert_eq!(
        rows,
        [
            (pre_cut_original, "primary".into(), None, None),
            (
                pre_cut_head.observation_id,
                "secondary".into(),
                None,
                Some(pre_cut_original)
            ),
            (primary_original, "primary".into(), Some("all".into()), None),
            (
                secondary_original,
                "secondary".into(),
                Some("subset".into()),
                None
            ),
            (
                corrected_secondary.observation_id,
                "secondary".into(),
                Some("subset".into()),
                Some(secondary_original)
            ),
            (
                corrected_primary.observation_id,
                "primary".into(),
                Some("all".into()),
                Some(primary_original)
            ),
        ]
    );
    let stored = tx
        .query_row(
            "SELECT pool_models, pool_models_at_ms, pool_member_models FROM multiplier_observation
         WHERE observation_id=?1",
            [corrected_secondary.observation_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                ))
            },
        )
        .unwrap();
    assert_eq!(
        stored,
        (
            r#"["provider/model"]"#.into(),
            secondary.interval_end_ms,
            r#"["provider/model-low"]"#.into()
        )
    );
    tx.commit().unwrap();
}
