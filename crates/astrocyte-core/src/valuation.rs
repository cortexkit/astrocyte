//! Per-event API list valuation, shared by multiplier observations and reports.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::catalog_client::{
    cache_write_ttl, reasoning_uses_output, stated_mode_condition, token_rate_key, BillingRateKey,
    RATE_ROLES,
};
use crate::money::MoneyError;
use crate::multiplier::{
    ModelResolution, MultiplierConfig, MultiplierError, RateState, RoleTokens,
};
use crate::pricing::{effective_output, prompt_size, EffectiveOutput};

/// Producers whose usage event is exactly one model call, so a tiered rate
/// (`.above_context.<size>`) can be decided from the event's own prompt. Every
/// other producer (BROCA) may bundle many calls into one event -- BROCA events
/// reach prompts of 10.7M tokens, ten times any context limit -- so its events
/// are priced at the base rate and their tokens are counted as flagged.
const PER_REQUEST_PRODUCERS: [&str; 4] = ["opencode", "pi", "claude-code", "codex"];

/// `(role, unit, source, quantity units, quantity exponent)` of one measurement.
pub type MeasuredRole = (String, String, String, Option<i64>, Option<i32>);

#[derive(Debug, Clone, PartialEq)]
pub struct EventTokens {
    pub producer: String,
    pub provider: String,
    pub model: Option<String>,
    /// The pricing mode recorded for this request, such as a fast service tier;
    /// it selects mode-specific catalog rates. None when not reported.
    pub mode: Option<String>,
    pub broca_attempt: bool,
    pub roles: Vec<MeasuredRole>,
}

pub struct ValuationRules<'a> {
    pub(crate) api_twins: &'a BTreeMap<String, String>,
    pub(crate) reasoning_billed_as_output: &'a [String],
}

impl<'a> ValuationRules<'a> {
    pub fn from_config(
        config: &'a MultiplierConfig,
        reasoning_billed_as_output: &'a [String],
    ) -> Self {
        Self {
            api_twins: &config.api_twins,
            reasoning_billed_as_output,
        }
    }

    pub fn catalog_provider<'b>(&'b self, event_provider: &'b str) -> &'b str {
        self.api_twins
            .get(event_provider)
            .map(String::as_str)
            .unwrap_or(event_provider)
    }
}

/// Exact value and diagnostics for one event, before any row-level rounding.
#[derive(Debug, Default)]
pub struct EventValuation {
    pub numerator: i128,
    pub measured_tokens: RoleTokens,
    pub tokens: RoleTokens,
    pub inclusive_output_adjustment: i64,
    pub tiered_tokens: i64,
    pub base_rate_tier_untested_tokens: i64,
    pub mode_unrecorded_tokens: i64,
    pub unpriced: Vec<UnpricedQuantity>,
}

/// A legacy diagnostic; report adapters normalize its reason separately.
#[derive(Debug)]
pub struct UnpricedQuantity {
    pub reason: String,
    pub role: String,
    pub quantity: i64,
    /// Original measurement positions, retained even when diagnostics group roles.
    pub measurements: Vec<usize>,
}

/// The closed report vocabulary; multiplier diagnostics keep their original reasons.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UnvaluedReason {
    NoRate,
    Refused,
    NonUsd,
    MissingModel,
    ValuationError,
}

impl UnvaluedReason {
    fn from_diagnostic(reason: &str) -> Self {
        // Missing or unusable rates (including unit_not_priced and
        // rate_not_resolved) become no_rate only in the list-value adapter.
        // Withheld/uncertain rates and refused history segments are refused;
        // currency, absent model identity and arithmetic errors stay distinct.
        match reason {
            "refused" | "rate_withheld" | "rate_uncertain" => Self::Refused,
            "non_usd" => Self::NonUsd,
            "missing_model" => Self::MissingModel,
            "valuation_error" => Self::ValuationError,
            _ => Self::NoRate,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Unvalued {
    pub measurements: u64,
    pub tokens: i64,
    pub by_reason: BTreeMap<UnvaluedReason, u64>,
}

impl Unvalued {
    pub(crate) fn count(
        &mut self,
        unit: &str,
        quantity: i64,
        reason: UnvaluedReason,
    ) -> Result<(), MoneyError> {
        if quantity <= 0 {
            return Ok(());
        }
        self.measurements = self
            .measurements
            .checked_add(1)
            .ok_or(MoneyError::OutOfRange)?;
        let count = self.by_reason.entry(reason).or_default();
        *count = count.checked_add(1).ok_or(MoneyError::OutOfRange)?;
        // Only token* units contribute to unvalued.tokens. Seconds, characters,
        // images and requests still count as measurements, never as tokens.
        if unit.starts_with("token") {
            self.tokens = self
                .tokens
                .checked_add(quantity)
                .ok_or(MoneyError::OutOfRange)?;
        }
        Ok(())
    }
}

/// List-value projection, still in exact nano-USD-per-million numerator units.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct ListValue {
    pub numerator: i128,
    pub unvalued: Unvalued,
    pub base_rate_tier_untested_tokens: i64,
}

/// Adapt a shared valuation to report counters after rates have been fetched.
/// A failed event contributes no partial value and counts all positive measurements.
pub fn list_value_event(
    rules: &ValuationRules<'_>,
    resolution: Option<&ModelResolution>,
    event: &EventTokens,
) -> Result<ListValue, MoneyError> {
    let valuation = value_event(rules, resolution, event);
    let mut reasons = vec![None; event.roles.len()];
    let mut result = ListValue::default();
    match valuation {
        Ok(value) => {
            result.numerator = value.numerator;
            result.base_rate_tier_untested_tokens = value.base_rate_tier_untested_tokens;
            for diagnostic in value.unpriced {
                let reason = UnvaluedReason::from_diagnostic(&diagnostic.reason);
                for index in diagnostic.measurements {
                    reasons[index] = Some(reason);
                }
            }
        }
        Err(_) => reasons.fill(Some(UnvaluedReason::ValuationError)),
    }
    for ((_, unit, _, quantity, _), reason) in event.roles.iter().zip(reasons) {
        if let (Some(quantity), Some(reason)) = (quantity, reason) {
            result.unvalued.count(unit, *quantity, reason)?;
        }
    }
    Ok(result)
}

type TokenQuantities<'a> = BTreeMap<(&'a str, Option<&'static str>), i64>;

pub(crate) fn event_quantities<'a>(
    event: &'a EventTokens,
    measured: &mut RoleTokens,
    inclusive_output_adjustment: &mut i64,
    count_unpriced: &mut impl FnMut(&str, &str, i64, &[usize]) -> Result<(), MultiplierError>,
) -> Result<TokenQuantities<'a>, MultiplierError> {
    // Quantities retain role and lifetime so split writes cannot overwrite
    // one another. A quantity that is not a whole token count
    // cannot be valued and is counted by reason.
    let mut quantities = TokenQuantities::new();
    let mut output_source = None;
    for (index, (role, unit, source, units, exponent)) in event.roles.iter().enumerate() {
        // Missing quantities exclude the hour even for an unsupported unit;
        // absence is unknown usage, not evidence of a zero-sized measurement.
        if units.is_none() || exponent.is_none() {
            count_unpriced("missing_quantity", role, units.unwrap_or(0), &[index])?;
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
                &[index],
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
                    count_unpriced("unrecognized_role", role, *units, &[index])?;
                }
            }
            (None, _) => count_unpriced("missing_quantity", role, 0, &[index])?,
            (Some(units), Some(_)) => {
                count_unpriced("fractional_quantity", role, *units, &[index])?
            }
            (Some(units), None) => count_unpriced("missing_quantity", role, *units, &[index])?,
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
                count_unpriced(
                    "inclusive_output_below_reasoning",
                    "output",
                    output,
                    &quantity_measurements(event, "output", None),
                )?;
                count_unpriced(
                    "inclusive_output_below_reasoning",
                    "reasoning",
                    reasoning.unwrap_or(0),
                    &quantity_measurements(event, "reasoning", None),
                )?;
                quantities.remove(&("output", None));
                quantities.remove(&("reasoning", None));
            }
        }
    }
    Ok(quantities)
}

fn quantity_measurements(event: &EventTokens, role: &str, ttl: Option<&str>) -> Vec<usize> {
    event
        .roles
        .iter()
        .enumerate()
        .filter_map(|(index, (measured_role, unit, _, units, exponent))| {
            let measured_ttl = (measured_role == "cache_write")
                .then(|| cache_write_ttl(unit))
                .flatten();
            (measured_role == role
                && units.is_some()
                && *exponent == Some(0)
                && (unit == "token" || measured_ttl.is_some())
                && measured_ttl == ttl)
                .then_some(index)
        })
        .collect()
}

/// Value one event without mutating a multiplier line or rounding its numerator.
pub fn value_event(
    rules: &ValuationRules<'_>,
    resolution: Option<&ModelResolution>,
    event: &EventTokens,
) -> Result<EventValuation, MultiplierError> {
    let mut value = EventValuation::default();
    let mut count_unpriced = |reason: &str, role: &str, quantity: i64, measurements: &[usize]| {
        value.unpriced.push(UnpricedQuantity {
            reason: reason.into(),
            role: role.into(),
            quantity,
            measurements: measurements.to_vec(),
        });
        Ok::<_, MultiplierError>(())
    };
    let quantities = event_quantities(
        event,
        &mut value.measured_tokens,
        &mut value.inclusive_output_adjustment,
        &mut count_unpriced,
    )?;
    for ((role, _), quantity) in &quantities {
        value.tokens.add(role, *quantity)?;
    }
    let mut count_quantity = |reason: &str, role: &str, ttl: Option<&str>, quantity: i64| {
        count_unpriced(
            reason,
            role,
            quantity,
            &quantity_measurements(event, role, ttl),
        )
    };
    let event_total = quantities.values().try_fold(0_i64, |sum, quantity| {
        sum.checked_add(*quantity).ok_or(MoneyError::OutOfRange)
    })?;

    if event.model.is_none() {
        for ((role, ttl), quantity) in &quantities {
            count_quantity("missing_model", role, *ttl, *quantity)?;
        }
        return Ok(value);
    }
    let api_provider = rules.catalog_provider(&event.provider);
    let model_rates = match resolution {
        Some(ModelResolution::Rates(model_rates)) => model_rates,
        Some(ModelResolution::Unpriceable { reason }) => {
            for ((role, ttl), quantity) in &quantities {
                count_quantity(reason, role, *ttl, *quantity)?;
            }
            return Ok(value);
        }
        None => {
            for ((role, ttl), quantity) in &quantities {
                count_quantity("rate_not_resolved", role, *ttl, *quantity)?;
            }
            return Ok(value);
        }
    };

    // Tier: decided from this event's own prompt, and only where one event is
    // one call. Each role uses its own largest exceeded threshold.
    // The prompt is read from the event's measurements as stored, including
    // lifetime-split cache writes, using the ledger's prompt-size function.
    let per_request =
        event.broca_attempt || PER_REQUEST_PRODUCERS.contains(&event.producer.as_str());
    let prompt =
        prompt_size(event.roles.iter().map(|(role, unit, _, units, exponent)| {
            (role.as_str(), unit.as_str(), *units, *exponent)
        }));
    if per_request && !model_rates.tiers.is_empty() && prompt.is_none() {
        // A prompt measurement with no usable quantity leaves the prompt's
        // size unknown, so neither the base nor a tier rate can be chosen.
        // Ledger pricing refuses the same request as `rate_tiered`.
        for ((role, ttl), quantity) in &quantities {
            count_quantity("rate_tiered", role, *ttl, *quantity)?;
        }
        return Ok(value);
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
            value.base_rate_tier_untested_tokens = value
                .base_rate_tier_untested_tokens
                .checked_add(event_total)
                .ok_or(MoneyError::OutOfRange)?;
        }
        None
    };
    if event_tier.is_some() {
        // Count the whole event when its prompt crosses any model threshold.
        value.tiered_tokens = value
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
        value.mode_unrecorded_tokens = value
            .mode_unrecorded_tokens
            .checked_add(event_total)
            .ok_or(MoneyError::OutOfRange)?;
    }
    let mode_rates = stated_mode.map(|mode| model_rates.modes.get(mode));
    let select_rate = |rate_role: &str, ttl: Option<&str>| {
        let tier = if per_request {
            prompt.and_then(|prompt| {
                model_rates.context_tier_for_role(rate_role, prompt, stated_mode)
            })
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
        if ttl.is_some() {
            // A lifetime is a measurement dimension, never a hint to use the
            // vendor's default write price or a less-specific condition set.
            Ok(exact)
        } else {
            match mode_rates {
                // Without a complete mode/tier rate, neither condition's
                // price alone describes the recorded request.
                Some(_) if tier.is_some() && exact.is_none() => Err("mode_rate_above_tier"),
                // A stated mode is priced at its own rate or not at all.
                Some(_) if tier.is_some() => Ok(exact),
                Some(rates) => Ok(rates.and_then(|rates| rates.get(rate_role))),
                // A role without a crossed tier uses its base rate. Conditional
                // tiers alone do not supply an unconditional tier price.
                None => Ok(tier
                    .and_then(|threshold| model_rates.tiers.get(&threshold))
                    .and_then(|tier_rates| tier_rates.get(rate_role))
                    .or_else(|| model_rates.base.get(rate_role))),
            }
        }
    };
    let reasoning_rule = matches!(
        model_rates.base.get("reasoning"),
        Some(RateState::BilledAs {
            key: BillingRateKey::Output
        })
    );
    let cache_write_rule = matches!(
        model_rates.base.get("cache_write"),
        Some(RateState::BilledAs {
            key: BillingRateKey::Input
        })
    );
    for ((role, ttl), quantity) in quantities {
        if quantity == 0 {
            continue;
        }
        let state = if role == "reasoning" {
            let reasoning = reasoning_rule.then(|| select_rate(role, ttl));
            // Only a complete selector match is a more specific verdict.
            let has_variant = matches!(reasoning, Some(Ok(Some(state))) if !matches!(state, RateState::BilledAs { .. }));
            if reasoning_uses_output(
                reasoning_rule,
                has_variant,
                Some(api_provider),
                rules.reasoning_billed_as_output,
            ) {
                select_rate("output", ttl)
            } else {
                reasoning.unwrap_or_else(|| select_rate(role, ttl))
            }
        } else if role == "cache_write" && ttl.is_none() {
            match select_rate(role, ttl) {
                // A tier rule names its precise input key. A different input
                // tier might otherwise be selected for the same prompt.
                Ok(Some(RateState::BilledAs {
                    key: BillingRateKey::InputAboveContext(threshold),
                })) => Ok(model_rates
                    .tiers
                    .get(threshold)
                    .and_then(|rates| rates.get("input"))),
                Ok(Some(RateState::BilledAs {
                    key: BillingRateKey::Input,
                })) => select_rate("input", ttl),
                Ok(None) | Err(_) if cache_write_rule => select_rate("input", ttl),
                selected => selected,
            }
        } else {
            select_rate(role, ttl)
        };
        match state {
            Ok(Some(RateState::Priced { units_per_million })) => {
                let product = i128::from(*units_per_million)
                    .checked_mul(i128::from(quantity))
                    .ok_or(MoneyError::OutOfRange)?;
                value.numerator = value
                    .numerator
                    .checked_add(product)
                    .ok_or(MoneyError::OutOfRange)?;
            }
            Ok(Some(RateState::Unpriced { reason })) => {
                count_quantity(reason, role, ttl, quantity)?
            }
            Err(reason) => count_quantity(reason, role, ttl, quantity)?,
            Ok(Some(RateState::BilledAs { .. }) | None) => {
                count_quantity("no_rate", role, ttl, quantity)?
            }
        }
    }
    Ok(value)
}
