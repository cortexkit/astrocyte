//! Money and token arithmetic shared by the multiplier estimators.
use super::*;

#[derive(Default)]
pub(super) struct UsageTotals {
    pub tokens: RoleTokens,
    pub api_nanos: i64,
    pub api_nanos_by_model: Vec<ModelLine>,
    pub unpriced_tokens: Vec<UnpricedTokens>,
    pub tokens_outside_quota_coverage: i64,
    pub base_rate_tier_untested_tokens: i64,
}

impl UsageTotals {
    pub(super) fn ratio(&self, denominator: i64) -> Option<f64> {
        (denominator > 0
            && !self.unpriced_tokens.iter().any(|token| {
                token.reason == "missing_quantity" || (self.api_nanos == 0 && token.tokens > 0)
            }))
        .then(|| self.api_nanos as f64 / denominator as f64)
    }
}

pub(super) type UnpricedTotals = BTreeMap<(String, String, String), (i64, u64)>;

pub(super) fn unpriced_tokens(unpriced: UnpricedTotals) -> Vec<UnpricedTokens> {
    // A zero quantity has no value to lose. A missing quantity is unknown.
    unpriced
        .into_iter()
        .filter(|((reason, _, _), (tokens, _))| *tokens > 0 || reason == "missing_quantity")
        .map(
            |((reason, model, role), (tokens, measurements))| UnpricedTokens {
                reason,
                model,
                role,
                tokens,
                measurements,
            },
        )
        .collect()
}

pub(super) fn measure_usage<'a>(
    events: impl IntoIterator<Item = (&'a EventTokens, i64)>,
    token_interval_ms: Option<(i64, i64)>,
    lookup: &ModelLookup<'_>,
) -> Result<UsageTotals, MultiplierError> {
    let mut totals = UsageTotals::default();
    let mut lines = BTreeMap::new();
    let mut unpriced = BTreeMap::new();
    for (event, occurred_at_ms) in events {
        if token_interval_ms.is_some_and(|(from, to)| occurred_at_ms >= from && occurred_at_ms < to)
        {
            let resolution = event
                .model
                .as_deref()
                .and_then(|model| lookup.get_at(&event.provider, model, occurred_at_ms));
            value_event(
                resolution,
                event,
                &mut lines,
                &mut unpriced,
                &mut totals.tokens,
            )?;
        } else {
            let roles = event_quantities(
                event,
                &mut RoleTokens::default(),
                &mut 0,
                &mut |_, _, _, _| Ok(()),
            )?;
            for quantity in roles.values() {
                totals.tokens_outside_quota_coverage = totals
                    .tokens_outside_quota_coverage
                    .checked_add(*quantity)
                    .ok_or(MoneyError::OutOfRange)?;
            }
        }
    }
    let mut api_numerator = 0_i128;
    for (_, mut line) in lines {
        line.api_nanos = round_nano_num(line.numerator)?;
        api_numerator = api_numerator
            .checked_add(line.numerator)
            .ok_or(MoneyError::OutOfRange)?;
        totals.base_rate_tier_untested_tokens = totals
            .base_rate_tier_untested_tokens
            .checked_add(line.base_rate_tier_untested_tokens)
            .ok_or(MoneyError::OutOfRange)?;
        line.rates = line
            .model
            .as_deref()
            .and_then(|model| lookup.get(&line.provider, model))
            .cloned();
        totals.api_nanos_by_model.push(line);
    }
    totals.api_nanos = round_nano_num(api_numerator)?;
    totals.unpriced_tokens = unpriced_tokens(unpriced);
    Ok(totals)
}

pub(super) struct CounterFee<'a> {
    pub counter: &'a AccountDelta,
    pub identity_field: &'static str,
    pub price: Option<&'a PlanPriceUsed>,
    pub segments: u64,
}

pub(super) struct QuotaTotals {
    pub subscription_nanos: i64,
    pub fee_paid_nanos: i64,
    pub delta_percent: f64,
    pub accounts_included: Vec<Value>,
    pub accounts_excluded: Vec<Value>,
    pub excluded_pairs: Value,
    pub low_resolution: bool,
}

pub(super) fn measure_quota<'a>(
    window_minutes: i64,
    token_interval_ms: Option<(i64, i64)>,
    counters: impl IntoIterator<Item = CounterFee<'a>>,
) -> Result<QuotaTotals, MultiplierError> {
    let token_span_ms = token_interval_ms
        .map(|(from, to)| to.checked_sub(from).ok_or(MoneyError::OutOfRange))
        .transpose()?
        .unwrap_or(0);
    let window_share = NanoRatio::new(
        i128::from(window_minutes),
        i128::from(MINUTES_PER_MONTH) * 100,
    )?;
    let fee_share = NanoRatio::new(i128::from(token_span_ms), i128::from(MS_PER_MONTH))?;
    let mut subscription = NanoRatio::new(0, 1)?;
    let mut fee_paid = NanoRatio::new(0, 1)?;
    let mut subscription_shares = Vec::new();
    let mut delta_percent = 0.0;
    let mut segments = 0;
    let mut excluded_pairs = ExcludedPairs::default();
    let mut accounts_included = Vec::new();
    let mut accounts_excluded = Vec::new();
    for CounterFee {
        counter,
        identity_field,
        price,
        segments: counter_segments,
    } in counters
    {
        excluded_pairs.decrease_boundary += counter.decrease_boundaries;
        excluded_pairs.fetched_at_backwards += counter.fetched_at_backwards;
        match price.and_then(|price| price.monthly_nanos) {
            Some(monthly) => {
                let monthly = NanoRatio::new(i128::from(monthly), 1)?;
                let account_subscription = monthly
                    .mul(window_share)?
                    .mul(NanoRatio::percent(counter.delta_percent)?)?;
                subscription = subscription.add(account_subscription)?;
                fee_paid = fee_paid.add(monthly.mul(fee_share)?)?;
                subscription_shares.push((counter.account_ref.clone(), account_subscription));
                delta_percent += counter.delta_percent;
                segments += counter_segments;
                accounts_included.push(json!({
                    (identity_field): counter.account_ref,
                    "delta_percent": counter.delta_percent,
                    "subscription_nanos": 0,
                    "readings": counter.readings,
                    "decrease_boundaries": counter.decrease_boundaries,
                    "fetched_at_backwards": counter.fetched_at_backwards,
                    "stale_reset_readings": counter.stale_reset_readings,
                    "reset_boundaries": counter.reset_boundaries,
                    "coverage_ms": counter.coverage_ms,
                }));
            }
            None => accounts_excluded.push(json!({
                (identity_field): counter.account_ref,
                "reason": price.and_then(|price| price.unpriceable_reason.as_deref())
                    .unwrap_or("plan_price_unresolved"),
                "delta_percent": counter.delta_percent,
                "readings": counter.readings,
            })),
        }
    }
    let subscription_nanos = subscription.round()?;
    let fee_paid_nanos = fee_paid.round()?;
    for (account, amount) in accounts_included.iter_mut().zip(allocate_subscription(
        &subscription_shares,
        subscription_nanos,
    )?) {
        account["subscription_nanos"] = json!(amount);
    }
    Ok(QuotaTotals {
        subscription_nanos,
        fee_paid_nanos,
        delta_percent,
        accounts_included,
        accounts_excluded,
        excluded_pairs: json!(excluded_pairs.counts()),
        low_resolution: delta_percent <= 0.0
            || segments as f64 / delta_percent > LOW_RESOLUTION_ERROR_SHARE,
    })
}

pub(super) fn value_event(
    resolution: Option<&ModelResolution>,
    event: &EventTokens,
    lines: &mut BTreeMap<(String, String, Option<String>), ModelLine>,
    unpriced: &mut BTreeMap<(String, String, String), (i64, u64)>,
    totals: &mut RoleTokens,
) -> Result<(), MultiplierError> {
    let value = crate::valuation::value_event(resolution, event)?;
    let line = lines
        .entry((
            event.producer.clone(),
            event.provider.clone(),
            event.model.clone(),
        ))
        .or_insert_with(|| ModelLine {
            producer: event.producer.clone(),
            provider: event.provider.clone(),
            model: event.model.clone(),
            ..ModelLine::default()
        });
    line.events += 1;
    line.measured_tokens.merge(&value.measured_tokens)?;
    totals.merge(&value.tokens)?;
    macro_rules! add {
        ($($field:ident),+ $(,)?) => {$(
            line.$field = line.$field.checked_add(value.$field).ok_or(MoneyError::OutOfRange)?;
        )+};
    }
    add!(
        numerator,
        inclusive_output_adjustment,
        tiered_tokens,
        base_rate_tier_untested_tokens,
        mode_unrecorded_tokens
    );
    record_unpriced(event, value.unpriced, unpriced)
}

pub(super) fn record_unpriced(
    event: &EventTokens,
    diagnostics: Vec<crate::valuation::UnpricedQuantity>,
    unpriced: &mut UnpricedTotals,
) -> Result<(), MultiplierError> {
    for diagnostic in diagnostics {
        let model_label = format!(
            "{}/{}/{}",
            event.producer,
            event.provider,
            event.model.as_deref().unwrap_or("<none>")
        );
        let entry = unpriced
            .entry((diagnostic.reason, model_label, diagnostic.role))
            .or_insert((0_i64, 0_u64));
        entry.0 = entry
            .0
            .checked_add(diagnostic.quantity)
            .ok_or(MoneyError::OutOfRange)?;
        entry.1 = entry.1.checked_add(1).ok_or(MoneyError::OutOfRange)?;
    }
    Ok(())
}
