//! Hourly pace from a recorded pool head and the vault's plan history.
//!
//! This reader deliberately has no catalog or configuration input. Coverage is
//! the coverage frozen into the selected observation, and valuation uses that
//! observation's recorded model resolutions.
use super::*;

/// Hourly usage valued using a recorded multiplier's list rates.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MultiplierPaceRequest {
    pub quota_provider: String,
    #[serde(default)]
    pub plan: Option<String>,
    #[serde(default)]
    pub pool: Option<String>,
    pub from_ms: i64,
    pub to_ms: i64,
}

// A weekly calibration may describe the current workload for daily routing,
// but a head older than two days needs a refreshed estimate or fallback.
const PACE_MAX_HEAD_AGE_MS: i64 = 2 * 24 * HOUR_MS;
// Bound the read work while allowing a trailing week plus UTC alignment tails.
const PACE_MAX_RANGE_HOURS: i64 = 192;

#[derive(Deserialize)]
pub(crate) struct PaceRateLine {
    pub(crate) producer: String,
    pub(crate) provider: String,
    pub(crate) model: Option<String>,
    pub(crate) rates: Option<ModelResolution>,
}

struct PaceHead {
    observation_id: i64,
    observation_day: String,
    interval_end_ms: i64,
    plan: String,
    window_pool_key: String,
    pool_scope: String,
    pool_models: Option<String>,
    pool_member_models: Option<String>,
    api_nanos: i64,
    delta_percent: f64,
    api_nanos_by_model: String,
}

impl PaceHead {
    fn pool_id(&self) -> Result<&str, MultiplierError> {
        crate::capacity::pool_identity(&self.window_pool_key).ok_or_else(|| {
            MultiplierError::Encode(format!(
                "post-cut observation {} has no pool identity",
                self.observation_id
            ))
        })
    }
}

type PaceRates<'a> =
    BTreeMap<&'a str, BTreeMap<&'a str, BTreeMap<Option<&'a str>, Option<&'a ModelResolution>>>>;

fn pace_rates(lines: &[PaceRateLine]) -> PaceRates<'_> {
    let mut index: PaceRates<'_> = BTreeMap::new();
    for line in lines {
        index
            .entry(&line.producer)
            .or_default()
            .entry(&line.provider)
            .or_default()
            .insert(line.model.as_deref(), line.rates.as_ref());
    }
    index
}

#[derive(Default)]
struct PaceClass {
    lines: BTreeMap<(String, String, Option<String>), ModelLine>,
    unpriced: BTreeMap<(String, String, String), (i64, u64)>,
    tokens: RoleTokens,
}

fn refuse<T>(code: &'static str, message: impl Into<String>) -> Result<T, MultiplierError> {
    Err(MultiplierError::Refused {
        code,
        message: message.into(),
    })
}

fn no_observation<T>() -> Result<T, MultiplierError> {
    refuse(
        "no_multiplier_observation",
        "no eligible multiplier pool head is recorded in the requested bounds",
    )
}

fn read_candidates(
    conn: &Connection,
    request: &MultiplierPaceRequest,
) -> Result<Vec<PaceHead>, MultiplierError> {
    let lower = request.to_ms.saturating_sub(PACE_MAX_HEAD_AGE_MS);
    let sql = format!(
        "SELECT o.observation_id, o.observation_day, o.interval_end_ms, o.plan,
                o.window_pool_key, o.pool_scope, o.pool_models, o.pool_member_models,
                o.api_nanos, o.delta_percent, o.api_nanos_by_model
         FROM multiplier_observation o
         WHERE o.quota_provider=?1 AND o.pool_scope IS NOT NULL
           AND o.routing_eligible=1 AND o.interval_end_ms>=?2 AND o.interval_end_ms<=?3
           AND {UNREPLACED} AND {UNRETIRED}
         ORDER BY o.interval_end_ms, o.observation_id"
    );
    let mut statement = conn.prepare(&sql)?;
    let rows = statement.query_map(
        params![request.quota_provider, lower, request.to_ms],
        |row| {
            Ok(PaceHead {
                observation_id: row.get(0)?,
                observation_day: row.get(1)?,
                interval_end_ms: row.get(2)?,
                plan: row.get(3)?,
                window_pool_key: row.get(4)?,
                pool_scope: row.get(5)?,
                pool_models: row.get(6)?,
                pool_member_models: row.get(7)?,
                api_nanos: row.get(8)?,
                delta_percent: row.get(9)?,
                api_nanos_by_model: row.get(10)?,
            })
        },
    )?;
    Ok(rows.collect::<rusqlite::Result<_>>()?)
}

fn select_head(
    mut candidates: Vec<PaceHead>,
    request: &MultiplierPaceRequest,
) -> Result<(PaceHead, String), MultiplierError> {
    if candidates.is_empty() {
        return no_observation();
    }
    let selected_plan = match request.plan.as_deref() {
        Some(plan) => plan.to_owned(),
        None => {
            let plans: BTreeSet<_> = candidates.iter().map(|head| head.plan.as_str()).collect();
            if plans.len() != 1 {
                return refuse(
                    "plan_required",
                    "pace requires a plan when bounded candidates span different plans",
                );
            }
            (*plans.first().expect("one candidate plan")).to_owned()
        }
    };
    candidates.retain(|head| head.plan == selected_plan);
    if candidates.is_empty() {
        return no_observation();
    }

    let selected_pool = match request.pool.as_deref() {
        Some(pool) => pool.to_owned(),
        None => {
            let pools: BTreeSet<String> = candidates
                .iter()
                .map(|head| head.pool_id().map(str::to_owned))
                .collect::<Result<_, _>>()?;
            if pools.len() != 1 {
                return refuse(
                    "pool_required",
                    "pace requires a pool when bounded candidates of the plan span different pools",
                );
            }
            pools.into_iter().next().expect("one candidate pool")
        }
    };
    let mut selected = Vec::new();
    for head in candidates {
        if head.pool_id()? == selected_pool {
            selected.push(head);
        }
    }
    candidates = selected;
    if candidates.is_empty() {
        return no_observation();
    }
    let head = candidates
        .into_iter()
        .max_by_key(|head| head.interval_end_ms)
        .expect("nonempty selected candidates");
    Ok((head, selected_pool))
}

fn aligned_range(request: &MultiplierPaceRequest) -> Result<(i64, i64), MultiplierError> {
    if request.from_ms >= request.to_ms {
        return refuse("invalid_range", "from_ms must be earlier than to_ms");
    }
    // Euclidean division also aligns times before the Unix epoch correctly.
    let from = i128::from(request.from_ms).div_euclid(i128::from(HOUR_MS)) * i128::from(HOUR_MS);
    let to = (i128::from(request.to_ms) + i128::from(HOUR_MS) - 1).div_euclid(i128::from(HOUR_MS))
        * i128::from(HOUR_MS);
    if to - from > i128::from(PACE_MAX_RANGE_HOURS) * i128::from(HOUR_MS) {
        return refuse(
            "range_too_large",
            format!("aligned range must not exceed {PACE_MAX_RANGE_HOURS} hours"),
        );
    }
    let from = i64::try_from(from).map_err(|_| MultiplierError::Refused {
        code: "invalid_range",
        message: "aligned from_ms overflows i64".into(),
    })?;
    let to = i64::try_from(to).map_err(|_| MultiplierError::Refused {
        code: "invalid_range",
        message: "aligned to_ms overflows i64".into(),
    })?;
    Ok((from, to))
}

fn decode_string_set(
    value: Option<&str>,
    column: &str,
) -> Result<BTreeSet<String>, MultiplierError> {
    value
        .map(|json| {
            serde_json::from_str::<Vec<String>>(json)
                .map(|values| values.into_iter().collect())
                .map_err(|error| {
                    MultiplierError::Encode(format!("invalid {column} on pace head: {error}"))
                })
        })
        .transpose()
        .map(Option::unwrap_or_default)
}

/// Value plan-attributed traffic in one recorded pool's frozen coverage.
pub fn multiplier_pace(
    conn: &Connection,
    request: &MultiplierPaceRequest,
) -> Result<Value, MultiplierError> {
    let post_cut_exists: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM multiplier_observation
                       WHERE quota_provider=?1 AND pool_scope IS NOT NULL)",
        [&request.quota_provider],
        |row| row.get(0),
    )?;
    if !post_cut_exists {
        return refuse(
            "unknown_quota_provider",
            format!("unknown quota provider '{}'", request.quota_provider),
        );
    }
    let (from, to) = aligned_range(request)?;
    let (head, selected_pool) = select_head(read_candidates(conn, request)?, request)?;
    let decode_error = |error: serde_json::Error| MultiplierError::Encode(error.to_string());
    let rate_lines: Vec<PaceRateLine> =
        serde_json::from_str(&head.api_nanos_by_model).map_err(decode_error)?;
    let stored_rates = pace_rates(&rate_lines);
    // No provider mapping or provider-wide reasoning list is applied; only
    // the selected head's recorded model rates decide reasoning billing.
    // Like the report's multipliers, an unusable denominator yields null, not
    // an invented zero rate. Nanodollars are rounded once to an integer.
    let per_point_nanos = if head.delta_percent > 0.0 && head.api_nanos > 0 {
        let percent = NanoRatio::percent(head.delta_percent)?;
        Some(
            NanoRatio::new(i128::from(head.api_nanos), 1)?
                .mul(NanoRatio::new(percent.denominator, percent.numerator)?)?
                .round()?,
        )
        .filter(|nanos| *nanos > 0)
    } else {
        None
    };
    let points = |nanos: i64| per_point_nanos.map(|per_point| nanos as f64 / per_point as f64);
    let pool_models = decode_string_set(head.pool_models.as_deref(), "pool_models")?;
    let pool_member_models =
        decode_string_set(head.pool_member_models.as_deref(), "pool_member_models")?;
    if head.pool_scope == "subset" && head.pool_models.is_none() {
        return Err(MultiplierError::Encode(
            "an eligible subset pace head has no pool_models".into(),
        ));
    }
    if head.pool_scope != "all" && head.pool_scope != "subset" {
        return Err(MultiplierError::Encode(format!(
            "eligible pace head has invalid pool_scope '{}'",
            head.pool_scope
        )));
    }
    let timeline = pools::VaultPlanTimeline::capture(conn, (from, to))?;
    let mut hours: BTreeMap<i64, BTreeMap<String, BTreeMap<String, PaceClass>>> = (from..to)
        .step_by(usize::try_from(HOUR_MS).expect("one hour in milliseconds fits usize"))
        .map(|hour| (hour, BTreeMap::new()))
        .collect();
    for_events(
        conn,
        "e.charge_basis='subscription_included'",
        &[SqlValue::Integer(from), SqlValue::Integer(to)],
        |event, paying_credential| {
            let Some((ledger, login)) = crate::subscription::plan_lookup_key(
                event.account_ref.as_deref(),
                paying_credential.as_deref(),
            ) else {
                return Ok(());
            };
            if timeline.plan_at(ledger, login, event.occurred_at_ms) != Some(head.plan.as_str()) {
                return Ok(());
            }
            if head.pool_scope == "subset" {
                let Some(model) = event.tokens.model.as_deref() else {
                    return Ok(());
                };
                let raw_id = format!("{}/{model}", event.tokens.provider);
                if !pool_models.contains(&raw_id) && !pool_member_models.contains(&raw_id) {
                    return Ok(());
                }
            }
            let tokens = &event.tokens;
            let hour = event.occurred_at_ms.div_euclid(HOUR_MS) * HOUR_MS;
            let work_class = event.work_class.unwrap_or_else(|| "unlabelled".into());
            // Rates belong to the exact producer/provider/model line recorded
            // on the head. No alias or suffix resolution happens in this reader.
            let resolution = stored_rates
                .get(tokens.producer.as_str())
                .and_then(|providers| providers.get(tokens.provider.as_str()))
                .and_then(|models| models.get(&tokens.model.as_deref()))
                .copied()
                .flatten();
            let class = hours
                .get_mut(&hour)
                .expect("aligned hour")
                .entry(tokens.producer.clone())
                .or_default()
                .entry(work_class)
                .or_default();
            value_event(
                resolution,
                tokens,
                &mut class.lines,
                &mut class.unpriced,
                &mut class.tokens,
            )
        },
    )?;
    let hours: Vec<Value> = hours.into_iter().map(|(hour, producers)| -> Result<_, MultiplierError> {
        let producers: BTreeMap<_, _> = producers.into_iter().map(|(producer, classes)| -> Result<_, MultiplierError> {
            let mut events = 0_u64;
            let mut nanos = 0_i64;
            let mut unpriced = 0_i64;
            let classes: BTreeMap<_, _> = classes.into_iter().map(|(name, class)| -> Result<_, MultiplierError> {
                let class_events = class.lines.values().try_fold(0_u64, |sum, line|
                    sum.checked_add(line.events).ok_or(MoneyError::OutOfRange))?;
                let class_nanos = class.lines.values().try_fold(0_i64, |sum, line|
                    sum.checked_add(round_nano_num(line.numerator)?).ok_or(MoneyError::OutOfRange))?;
                events = events.checked_add(class_events).ok_or(MoneyError::OutOfRange)?;
                nanos = nanos.checked_add(class_nanos).ok_or(MoneyError::OutOfRange)?;
                unpriced = class.unpriced.values().try_fold(unpriced, |sum, (tokens, _)|
                    sum.checked_add(*tokens).ok_or(MoneyError::OutOfRange))?;
                Ok((name, json!({"events": class_events, "list_nanos": class_nanos, "estimated_points": points(class_nanos)})))
            }).collect::<Result<_, _>>()?;
            Ok((producer, json!({"events": events, "list_nanos": nanos, "estimated_points": points(nanos), "unpriced_tokens": unpriced, "by_work_class": classes})))
        }).collect::<Result<_, _>>()?;
        Ok(json!({"hour_start_ms": hour, "producers": producers}))
    }).collect::<Result<_, _>>()?;
    Ok(json!({
        "quota_provider": request.quota_provider,
        "plan": head.plan,
        "pool_id": selected_pool,
        "from_ms": from,
        "to_ms": to,
        "basis": "pro_rata_list_value",
        "routing_eligible": true,
        "per_point_nanos": per_point_nanos,
        "observation_id": head.observation_id,
        "observation_day": head.observation_day,
        // An undated call has no instant at which the vault can attribute it
        // to the selected plan, so it cannot enter this plan's pace traffic.
        "undated_events": 0,
        "hours": hours,
    }))
}
