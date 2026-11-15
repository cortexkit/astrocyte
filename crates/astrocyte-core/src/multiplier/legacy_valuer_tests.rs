// Frozen pre-extraction oracle from 3c0b8a2f63bcb2b1f23940a919dadc1b2e660fdf.
// Keep its policy independent of the shared implementation for differential tests.
const LEGACY_PER_REQUEST_PRODUCERS: [&str; 4] = ["opencode", "pi", "claude-code", "codex"];

struct LegacyValuationRules<'a> {
    api_twins: &'a BTreeMap<String, String>,
    reasoning_billed_as_output: &'a [String],
}

impl LegacyValuationRules<'_> {
    fn catalog_provider<'a>(&'a self, event_provider: &'a str) -> &'a str {
        self.api_twins
            .get(event_provider)
            .map(String::as_str)
            .unwrap_or(event_provider)
    }
}

type TokenQuantities<'a> = BTreeMap<(&'a str, Option<&'static str>), i64>;

fn legacy_event_quantities<'a>(
    event: &'a EventTokens,
    measured: &mut RoleTokens,
    inclusive_output_adjustment: &mut i64,
    count_unpriced: &mut impl FnMut(&str, &str, i64) -> Result<(), MultiplierError>,
) -> Result<TokenQuantities<'a>, MultiplierError> {
    // Quantities retain role and lifetime so split writes cannot overwrite
    // one another. A quantity that is not a whole token count
    // cannot be valued and is counted by reason.
    let mut quantities = TokenQuantities::new();
    let mut output_source = None;
    for (role, unit, source, units, exponent) in &event.roles {
        // Missing quantities exclude the hour even for an unsupported unit;
        // absence is unknown usage, not evidence of a zero-sized measurement.
        if units.is_none() || exponent.is_none() {
            count_unpriced("missing_quantity", role, units.unwrap_or(0))?;
            continue;
        }
        let ttl = (role == "cache_write")
            .then(|| cache_write_ttl(unit))
            .flatten();
        if unit != "token" && ttl.is_none() {
            count_unpriced(
                "unit_not_priced",
                &format!("{role}/{unit}"),
                units.unwrap_or(0),
            )?;
            continue;
        }
        match (units, exponent) {
            (Some(units), Some(0)) => {
                measured.add(role, *units)?;
                if role == "output" {
                    output_source = Some(source.as_str());
                }
                if RATE_ROLES.contains(&role.as_str()) {
                    quantities.insert((role.as_str(), ttl), *units);
                } else {
                    count_unpriced("unrecognized_role", role, *units)?;
                }
            }
            (None, _) => count_unpriced("missing_quantity", role, 0)?,
            (Some(units), Some(_)) => count_unpriced("fractional_quantity", role, *units)?,
            (Some(units), None) => count_unpriced("missing_quantity", role, *units)?,
        }
    }
    // Some BROCA output counts include reasoning tokens. Remove that overlap
    // once, using the ledger's rule, for both valuation and token counts on
    // either side of quota coverage.
    if let (Some(output), Some(source)) =
        (quantities.get(&("output", None)).copied(), output_source)
    {
        let reasoning = quantities.get(&("reasoning", None)).copied();
        match effective_output(
            &event.producer,
            Some(&event.provider),
            source,
            output,
            reasoning,
        ) {
            EffectiveOutput::AsStored => {}
            EffectiveOutput::ReasoningRemoved(effective) => {
                quantities.insert(("output", None), effective);
                let removed = output
                    .checked_sub(effective)
                    .ok_or(MoneyError::OutOfRange)?;
                *inclusive_output_adjustment = inclusive_output_adjustment
                    .checked_add(removed)
                    .ok_or(MoneyError::OutOfRange)?;
            }
            EffectiveOutput::ReasoningExceedsOutput => {
                // Output below its own reasoning cannot be made disjoint.
                count_unpriced("inclusive_output_below_reasoning", "output", output)?;
                count_unpriced(
                    "inclusive_output_below_reasoning",
                    "reasoning",
                    reasoning.unwrap_or(0),
                )?;
                quantities.remove(&("output", None));
                quantities.remove(&("reasoning", None));
            }
        }
    }
    Ok(quantities)
}

fn legacy_value_event(
    rules: &LegacyValuationRules<'_>,
    resolution: Option<&ModelResolution>,
    event: &EventTokens,
    lines: &mut BTreeMap<(String, String, Option<String>), ModelLine>,
    unpriced: &mut BTreeMap<(String, String, String), (i64, u64)>,
    totals: &mut RoleTokens,
) -> Result<(), MultiplierError> {
    let mut model_label = None;
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
    let mut count_unpriced = |reason: &str, role: &str, tokens: i64| {
        let model_label = model_label.get_or_insert_with(|| {
            format!(
                "{}/{}/{}",
                event.producer,
                event.provider,
                event.model.as_deref().unwrap_or("<none>")
            )
        });
        let entry = unpriced
            .entry((reason.to_string(), model_label.clone(), role.to_string()))
            .or_insert((0, 0));
        entry.0 = entry.0.checked_add(tokens).ok_or(MoneyError::OutOfRange)?;
        entry.1 = entry.1.checked_add(1).ok_or(MoneyError::OutOfRange)?;
        Ok::<_, MultiplierError>(())
    };

    let quantities = legacy_event_quantities(
        event,
        &mut line.measured_tokens,
        &mut line.inclusive_output_adjustment,
        &mut count_unpriced,
    )?;
    for ((role, _), quantity) in &quantities {
        totals.add(role, *quantity)?;
    }
    let event_total = quantities.values().try_fold(0_i64, |sum, quantity| {
        sum.checked_add(*quantity).ok_or(MoneyError::OutOfRange)
    })?;

    if event.model.is_none() {
        for ((role, _), quantity) in &quantities {
            count_unpriced("missing_model", role, *quantity)?;
        }
        return Ok(());
    }
    let api_provider = rules.catalog_provider(&event.provider);
    let model_rates = match resolution {
        Some(ModelResolution::Rates(model_rates)) => model_rates,
        Some(ModelResolution::Unpriceable { reason }) => {
            for ((role, _), quantity) in &quantities {
                count_unpriced(reason, role, *quantity)?;
            }
            return Ok(());
        }
        None => {
            for ((role, _), quantity) in &quantities {
                count_unpriced("rate_not_resolved", role, *quantity)?;
            }
            return Ok(());
        }
    };

    // Tier: decided from this event's own prompt, and only where one event is
    // one call. Each role uses its own largest exceeded threshold.
    // The prompt is read from the event's measurements as stored, including
    // lifetime-split cache writes, using the ledger's prompt-size function.
    let per_request = LEGACY_PER_REQUEST_PRODUCERS.contains(&event.producer.as_str());
    let prompt =
        prompt_size(event.roles.iter().map(|(role, unit, _, units, exponent)| {
            (role.as_str(), unit.as_str(), *units, *exponent)
        }));
    if per_request && !model_rates.tiers.is_empty() && prompt.is_none() {
        // A prompt measurement with no usable quantity leaves the prompt's
        // size unknown, so neither the base nor a tier rate can be chosen.
        // Ledger pricing refuses the same request as `rate_tiered`.
        for ((role, _), quantity) in &quantities {
            count_unpriced("rate_tiered", role, *quantity)?;
        }
        return Ok(());
    }
    let event_tier = if per_request {
        prompt.and_then(|prompt| {
            model_rates
                .tiers
                .iter()
                .rev()
                .find(|(threshold, _)| prompt.exceeds(**threshold))
        })
    } else {
        if !model_rates.tiers.is_empty() {
            line.base_rate_tier_untested_tokens = line
                .base_rate_tier_untested_tokens
                .checked_add(event_total)
                .ok_or(MoneyError::OutOfRange)?;
        }
        None
    };
    if event_tier.is_some() {
        // Count the whole event when its prompt crosses any model threshold.
        line.tiered_tokens = line
            .tiered_tokens
            .checked_add(event_total)
            .ok_or(MoneyError::OutOfRange)?;
    }
    // Mode: a request recorded in a non-standard mode prices at that mode's
    // rates only. One recorded as standard, or with no recorded mode, prices
    // at the base; the unrecorded ones are counted where a mode rate existed
    // that they might have owed.
    let stated_mode = stated_mode_condition(event.mode.as_deref());
    if event.mode.is_none() && !model_rates.modes.is_empty() {
        line.mode_unrecorded_tokens = line
            .mode_unrecorded_tokens
            .checked_add(event_total)
            .ok_or(MoneyError::OutOfRange)?;
    }
    let mode_rates = stated_mode.map(|mode| model_rates.modes.get(mode));
    let reasoning_as_output = rules
        .reasoning_billed_as_output
        .iter()
        .any(|provider| provider == api_provider);
    for ((role, ttl), quantity) in quantities {
        if quantity == 0 {
            continue;
        }
        let rate_role = if role == "reasoning" && reasoning_as_output {
            "output"
        } else {
            role
        };
        let tier = if per_request {
            prompt.and_then(|prompt| model_rates.context_tier_for_role(rate_role, prompt))
        } else {
            None
        };
        let mut selectors = BTreeMap::new();
        if let Some(ttl) = ttl {
            selectors.insert("ttl".into(), ttl.into());
        }
        if let Some(mode) = stated_mode {
            selectors.insert("mode".into(), mode.into());
        }
        if let Some(threshold) = tier {
            selectors.insert("above_context".into(), threshold.to_string());
        }
        let exact = model_rates
            .conditional
            .get(&token_rate_key(rate_role, &selectors));
        let state = if ttl.is_some() {
            // A lifetime is a measurement dimension, never a hint to use the
            // vendor's default write price or a less-specific condition set.
            exact
        } else {
            match mode_rates {
                // Without a complete mode/tier rate, neither condition's
                // price alone describes the recorded request.
                Some(_) if tier.is_some() && exact.is_none() => {
                    count_unpriced("mode_rate_above_tier", role, quantity)?;
                    continue;
                }
                // A stated mode is priced at its own rate or not at all.
                Some(_) if tier.is_some() => exact,
                Some(rates) => rates.and_then(|rates| rates.get(rate_role)),
                // A role without a crossed tier uses its base rate. Conditional
                // tiers alone do not supply an unconditional tier price.
                None => tier
                    .and_then(|threshold| model_rates.tiers.get(&threshold))
                    .and_then(|tier_rates| tier_rates.get(rate_role))
                    .or_else(|| model_rates.base.get(rate_role)),
            }
        };
        match state {
            Some(RateState::Priced { units_per_million }) => {
                let product = i128::from(*units_per_million)
                    .checked_mul(i128::from(quantity))
                    .ok_or(MoneyError::OutOfRange)?;
                line.numerator = line
                    .numerator
                    .checked_add(product)
                    .ok_or(MoneyError::OutOfRange)?;
            }
            Some(RateState::Unpriced { reason }) => count_unpriced(reason, role, quantity)?,
            None => count_unpriced("no_rate", role, quantity)?,
        }
    }
    Ok(())
}
