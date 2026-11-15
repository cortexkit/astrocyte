use super::*;
use chrono::Offset;
use rusqlite::types::Value as SqlValue;

const HOUR: i64 = 3_600_000;
const DAY: i64 = 24 * HOUR;

pub(super) struct ParsedReport {
    pub grain: Grain,
    pub timezone: Tz,
    pub grouping: Vec<Grouping>,
    pub range: Option<(i64, i64)>,
    pub filters: Vec<(Grouping, Vec<Option<String>>)>,
}

pub(super) fn dimension(name: &str) -> Result<Grouping, ReportError> {
    if matches!(
        name,
        "alfonso" | "workload" | "workspace" | "parent_alfonso"
    ) {
        return Err(ReportError::Refused(ReportRefusal::DimensionUnavailable(
            name.into(),
        )));
    }
    Grouping::parse(name)
        .ok_or_else(|| ReportError::Refused(ReportRefusal::UnknownGrouping(name.into())))
}

impl ParsedReport {
    pub fn new(request: &ReportRequest) -> Result<Self, ReportError> {
        let grain = Grain::parse(&request.grain).ok_or_else(|| {
            ReportError::Refused(ReportRefusal::UnknownGrain(request.grain.clone()))
        })?;
        let timezone: Tz = request.timezone.parse().map_err(|_| {
            ReportError::Refused(ReportRefusal::UnknownTimezone(request.timezone.clone()))
        })?;
        let grouping = parse_grouping(&request.grouping)?;
        let filters = request
            .filters
            .iter()
            .map(|(name, values)| {
                let dimension = dimension(name)?;
                if values.is_empty() {
                    return Err(ReportError::Refused(ReportRefusal::InvalidFilter(
                        name.clone(),
                    )));
                }
                Ok((dimension, values.clone()))
            })
            .collect::<Result<Vec<_>, ReportError>>()?;
        let range = match (request.from_ms, request.to_ms) {
            (None, None) if grain != Grain::Hour => None,
            (None, None) => return Err(ReportError::Refused(ReportRefusal::RangeTooLarge)),
            (Some(from), Some(to)) if from < to => {
                let span = i128::from(to) - i128::from(from);
                let max_days = if grain == Grain::Hour { 31 } else { 400 };
                if span > i128::from(max_days * DAY) {
                    return Err(ReportError::Refused(ReportRefusal::RangeTooLarge));
                }
                let from = bounds(from, timezone, grain)?.0;
                let to = bounds(to - 1, timezone, grain)?.1;
                if matches!(grain, Grain::Hour | Grain::Day)
                    && i128::from(to) - i128::from(from) > i128::from(max_days * DAY)
                {
                    return Err(ReportError::Refused(ReportRefusal::RangeTooLarge));
                }
                if grain == Grain::Hour {
                    // Hour buckets are UTC hours, so they are local hours only
                    // where the zone's offset is a whole number of hours. A zone
                    // keeps a fractional offset (India's +5:30, Nepal's +5:45) for
                    // years, never for minutes, so sampling the offset every 15
                    // minutes, plus the range's last instant, finds one anywhere in
                    // the range: 2,977 checks for 31 days, where checking every
                    // second would take 2.7 million.
                    let step = 15 * 60 * 1000;
                    let samples = (from..to).step_by(step).chain(std::iter::once(to - 1));
                    for at in samples {
                        let instant = Utc
                            .timestamp_millis_opt(at)
                            .single()
                            .ok_or(ReportError::Calendar)?;
                        if instant
                            .with_timezone(&timezone)
                            .offset()
                            .fix()
                            .local_minus_utc()
                            % 3600
                            != 0
                        {
                            return Err(ReportError::Refused(
                                ReportRefusal::HourTimezoneUnsupported,
                            ));
                        }
                    }
                }
                Some((from, to))
            }
            _ => return Err(ReportError::Refused(ReportRefusal::InvalidRange)),
        };
        Ok(Self {
            grain,
            timezone,
            grouping,
            range,
            filters,
        })
    }

    pub fn selects(&self, dimensions: &[Option<String>; GROUPING_DIMENSIONS]) -> bool {
        self.filters
            .iter()
            .all(|(dimension, allowed)| allowed.contains(&dimensions[*dimension as usize]))
    }

    pub fn sql(&self, internal: Option<&str>) -> (String, Vec<SqlValue>) {
        let mut parts = Vec::new();
        let mut parameters = Vec::new();
        if let Some(internal) = internal {
            parts.push(format!("({internal})"));
        }
        if let Some((from, to)) = self.range {
            parts.push("occurred_at_ms>=? AND occurred_at_ms<?".into());
            parameters.extend([SqlValue::Integer(from), SqlValue::Integer(to)]);
        }
        for (dimension, values) in &self.filters {
            let column = match dimension {
                Grouping::Account => "account_ref",
                Grouping::Project => "project_root",
                _ => dimension.name(),
            };
            let mut alternatives = Vec::new();
            for value in values {
                if let Some(value) = value {
                    alternatives.push(format!("{column}=?"));
                    parameters.push(SqlValue::Text(value.clone()));
                } else {
                    alternatives.push(format!("{column} IS NULL"));
                }
            }
            parts.push(format!("({})", alternatives.join(" OR ")));
        }
        (
            if parts.is_empty() {
                "1".into()
            } else {
                parts.join(" AND ")
            },
            parameters,
        )
    }
}

pub(super) fn bounds(ms: i64, timezone: Tz, grain: Grain) -> Result<(i64, i64), ReportError> {
    if grain == Grain::Hour {
        let start = ms
            .div_euclid(HOUR)
            .checked_mul(HOUR)
            .ok_or(ReportError::Calendar)?;
        return Ok((start, start.checked_add(HOUR).ok_or(ReportError::Calendar)?));
    }
    bucket_bounds_for_date(local_date(ms, timezone)?, timezone, grain)
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct ReportTokens {
    #[serde(flatten)]
    pub counts: crate::runs::Tokens,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_write_5m: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_write_1h: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct UnitQuantity {
    pub role: String,
    pub unit: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quantity: Option<crate::runs::Quantity>,
    #[serde(skip_serializing_if = "is_zero")]
    pub unknown_quantities: u64,
}
fn is_zero(value: &u64) -> bool {
    *value == 0
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct UsageQuantities {
    pub tokens: ReportTokens,
    pub units: Vec<UnitQuantity>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub unknown_quantities: BTreeMap<String, u64>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct Quantities(BTreeMap<(String, String), (DecimalSum, u64)>);
fn whole_tokens(sum: &DecimalSum) -> Result<Option<u64>, ReportError> {
    if !sum.initialized {
        return Ok(None);
    }
    let value = if sum.exponent >= 0 {
        sum.units
            .checked_mul(pow10_i128(sum.exponent.unsigned_abs())?)
            .ok_or(ReportError::Arithmetic)?
    } else {
        let divisor = pow10_i128(sum.exponent.unsigned_abs())?;
        if sum.units % divisor != 0 {
            return Ok(None);
        }
        sum.units / divisor
    };
    Ok(Some(
        u64::try_from(value).map_err(|_| ReportError::Arithmetic)?,
    ))
}
impl Quantities {
    pub fn observe(
        &mut self,
        role: &str,
        unit: &str,
        quantity: Option<(i128, i32)>,
        unknown: u64,
    ) -> Result<(), ReportError> {
        let target = self.0.entry((role.into(), unit.into())).or_default();
        if let Some((units, exponent)) = quantity {
            target.0.add_exact(units, exponent)?;
        }
        add_count(&mut target.1, unknown)
    }
    pub fn merge(&mut self, other: &Self) -> Result<(), ReportError> {
        for ((role, unit), (sum, unknown)) in &other.0 {
            self.observe(
                role,
                unit,
                sum.initialized.then_some((sum.units, sum.exponent)),
                *unknown,
            )?;
        }
        Ok(())
    }
    pub fn render(&self) -> Result<UsageQuantities, ReportError> {
        let mut result = UsageQuantities::default();
        let mut token_roles: BTreeMap<String, DecimalSum> = BTreeMap::new();
        for ((role, unit), (sum, unknown)) in &self.0 {
            if unit == "token" || unit.starts_with("token_cache_") {
                if *unknown != 0 {
                    add_count(
                        result.unknown_quantities.entry(role.clone()).or_default(),
                        *unknown,
                    )?;
                }
                if !sum.initialized {
                    continue;
                }
                token_roles
                    .entry(role.clone())
                    .or_default()
                    .add_exact(sum.units, sum.exponent)?;
                let split = match (role.as_str(), unit.as_str()) {
                    ("cache_write", "token_cache_5m") => Some(&mut result.tokens.cache_write_5m),
                    ("cache_write", "token_cache_1h") => Some(&mut result.tokens.cache_write_1h),
                    _ => None,
                };
                if let Some(split) = split {
                    *split = whole_tokens(sum)?;
                }
            } else {
                result.units.push(UnitQuantity {
                    role: role.clone(),
                    unit: unit.clone(),
                    quantity: sum
                        .initialized
                        .then(|| {
                            i64::try_from(sum.units)
                                .map(|units| crate::runs::Quantity {
                                    units,
                                    exponent: sum.exponent,
                                })
                                .map_err(|_| ReportError::Arithmetic)
                        })
                        .transpose()?,
                    unknown_quantities: *unknown,
                });
            }
        }
        for (role, sum) in token_roles {
            let target = match role.as_str() {
                "input" => &mut result.tokens.counts.input,
                "output" => &mut result.tokens.counts.output,
                "reasoning" => &mut result.tokens.counts.reasoning,
                "cache_read" => &mut result.tokens.counts.cache_read,
                "cache_write" => &mut result.tokens.counts.cache_write,
                _ => continue,
            };
            *target = whole_tokens(&sum)?;
        }
        let counts = &mut result.tokens.counts;
        if let Some(output) = counts.output {
            output
                .checked_add(counts.reasoning.unwrap_or(0))
                .ok_or(ReportError::Arithmetic)?;
        }
        result.tokens.counts.output =
            crate::cache_diagnostics::served_output(counts.output, counts.reasoning);
        Ok(result)
    }
}
