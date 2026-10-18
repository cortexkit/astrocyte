//! Desired-configuration decoding for the live producer connections.

use astrocyte_core::canon::{canonical_hash, jcs_serialize};
use astrocyte_core::multiplier::MultiplierConfig;
use serde::{Deserialize, Serialize, Serializer};

pub const DEFAULT_MODULE_ID: &str = "astrocyte";

pub(crate) const MAX_POLL_INTERVAL_SECS: u64 = 2_305_843_009_213_693;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AstrocyteConfig {
    pub poll_interval_secs: u64,
    pub qta_connection_file: Option<String>,
    pub fusiform_connection_file: Option<String>,
    /// Providers whose reasoning tokens use output rates in ledger pricing and valuation.
    pub reasoning_billed_as_output: Vec<String>,
    pub subscription_multiplier: MultiplierSetting,
    pub harness_ingest: HarnessIngest,
}

/// Every producer `harness_ingest.exclude` may name, by its `source_runtime`
/// key: the local harness readers and BROCA's store reader. A producer added
/// later joins this list and is ingested unless the operator excludes it.
pub(crate) const INGEST_SOURCES: [&str; 10] = [
    "claude-code",
    "claude-code-workers",
    "codex",
    "opencode",
    "pi",
    "gemini",
    "goose",
    "omp",
    "kimi",
    "broca",
];

/// Which producers the refresher drains. `enabled: false` turns every one
/// off; otherwise each producer named in `exclude` is skipped. A skipped
/// producer is not read at all, and its runtime row is removed so it never
/// reads as stale.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HarnessIngest {
    pub enabled: bool,
    pub exclude: Vec<String>,
}

impl HarnessIngest {
    /// Every producer on. A stored generation that predates the
    /// `harness_ingest` column also hydrates as this, because it ingested
    /// every producer.
    pub fn all_on() -> Self {
        Self {
            enabled: true,
            exclude: Vec::new(),
        }
    }

    /// Whether the producer with this source key is drained.
    pub fn ingests(&self, source_key: &str) -> bool {
        self.enabled && !self.exclude.iter().any(|excluded| excluded == source_key)
    }

    fn validate(&self) -> Result<(), ConfigError> {
        let mut seen = std::collections::BTreeSet::new();
        for key in &self.exclude {
            if !INGEST_SOURCES.contains(&key.as_str()) {
                return Err(ConfigError::Validation(format!(
                    "harness_ingest.exclude names unknown producer '{key}'; known producers are {}",
                    INGEST_SOURCES.join(", ")
                )));
            }
            if !seen.insert(key.as_str()) {
                return Err(ConfigError::Validation(format!(
                    "harness_ingest.exclude names producer '{key}' more than once"
                )));
            }
        }
        if !self.enabled && !self.exclude.is_empty() {
            return Err(ConfigError::Validation(
                "harness_ingest.enabled is false but harness_ingest.exclude is not empty; \
                 with ingestion off there is nothing to exclude"
                    .into(),
            ));
        }
        Ok(())
    }
}

/// The subscription multiplier's declarations as a generation carries them.
///
/// A decoded document always yields `Recorded`: the member is required. The
/// other state exists only for hydration, because a generation committed
/// before the store could hold the member has none. That is not the same as
/// an operator declaring no quota providers, so it is its own state and the
/// daily multiplier stage skips it under its own reason.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MultiplierSetting {
    Recorded(MultiplierConfig),
    NotRecorded,
}

impl Serialize for MultiplierSetting {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Recorded(config) => config.serialize(serializer),
            Self::NotRecorded => serializer.serialize_none(),
        }
    }
}

impl AstrocyteConfig {
    /// Validation bounds the seconds value so both conversion to milliseconds
    /// and four consecutive poll intervals fit in a signed database integer.
    pub(crate) fn poll_interval_ms(&self) -> i64 {
        i64::try_from(self.poll_interval_secs)
            .expect("validated poll interval fits i64")
            .checked_mul(1_000)
            .expect("validated poll interval converts to milliseconds")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DesiredContent {
    pub config: AstrocyteConfig,
    pub canonical_bytes: Vec<u8>,
    pub content_hash: String,
}

/// The result of reading a user-supplied desired-configuration path.
#[derive(Debug)]
pub enum ConfigReadOutcome {
    Contents(Vec<u8>),
    Missing,
    Unreadable(std::io::Error),
}

/// Preserve the daemon's distinction between an absent file and every other
/// read error before callers decide whether to decode the file contents.
pub fn classify_read_result(result: Result<Vec<u8>, std::io::Error>) -> ConfigReadOutcome {
    match result {
        Ok(contents) => ConfigReadOutcome::Contents(contents),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => ConfigReadOutcome::Missing,
        Err(error) => ConfigReadOutcome::Unreadable(error),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigError {
    Syntax(String),
    Decode(String),
    Validation(String),
    Canonical(String),
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Syntax(message)
            | Self::Decode(message)
            | Self::Validation(message)
            | Self::Canonical(message) => write!(f, "{message}"),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DecodedConfig {
    poll_interval_secs: u64,
    qta_connection_file: Option<String>,
    fusiform_connection_file: Option<String>,
    reasoning_billed_as_output: Vec<String>,
    subscription_multiplier: MultiplierConfig,
    harness_ingest: HarnessIngest,
}

impl From<DecodedConfig> for AstrocyteConfig {
    fn from(decoded: DecodedConfig) -> Self {
        Self {
            poll_interval_secs: decoded.poll_interval_secs,
            qta_connection_file: decoded.qta_connection_file,
            fusiform_connection_file: decoded.fusiform_connection_file,
            reasoning_billed_as_output: decoded.reasoning_billed_as_output,
            subscription_multiplier: MultiplierSetting::Recorded(decoded.subscription_multiplier),
            harness_ingest: decoded.harness_ingest,
        }
    }
}

/// The `desired_config.subscription_multiplier` column value: the member's
/// canonical JSON, or `None` for a generation that did not record it.
pub(crate) fn multiplier_column(config: &AstrocyteConfig) -> Result<Option<String>, ConfigError> {
    match &config.subscription_multiplier {
        MultiplierSetting::NotRecorded => Ok(None),
        MultiplierSetting::Recorded(multiplier) => {
            let value = serde_json::to_value(multiplier)
                .map_err(|error| ConfigError::Canonical(error.to_string()))?;
            jcs_serialize(&value)
                .map(Some)
                .map_err(|error| ConfigError::Canonical(format!("{error:?}")))
        }
    }
}

/// Reads the column back. NULL is the pre-migration state, never an empty
/// declaration; a present value goes through the same closed decode and
/// validation as the desired document.
pub(crate) fn decode_multiplier_column(
    column: Option<&str>,
) -> Result<MultiplierSetting, ConfigError> {
    let Some(text) = column else {
        return Ok(MultiplierSetting::NotRecorded);
    };
    // Older generations stored the pricing declaration inside this member.
    // Hydration lifts it separately; desired documents still reject that shape.
    let mut value: serde_json::Value = serde_json::from_str(text)
        .map_err(|error| ConfigError::Decode(format!("subscription_multiplier: {error}")))?;
    if let Some(object) = value.as_object_mut() {
        object.remove("reasoning_billed_as_output");
    }
    let config: MultiplierConfig = serde_json::from_value(value)
        .map_err(|error| ConfigError::Decode(format!("subscription_multiplier: {error}")))?;
    config.validate().map_err(ConfigError::Validation)?;
    Ok(MultiplierSetting::Recorded(config))
}

/// Read `reasoning_billed_as_output` from the generation JSON. If absent, use
/// the old `subscription_multiplier.reasoning_billed_as_output` column member;
/// generations without either declaration use an empty provider list.
pub(crate) fn decode_reasoning_generation(
    canonical_bytes: &[u8],
    multiplier_column: Option<&str>,
) -> Result<Vec<String>, ConfigError> {
    let generation: serde_json::Value = serde_json::from_slice(canonical_bytes)
        .map_err(|error| ConfigError::Decode(format!("desired generation: {error}")))?;
    let nested: serde_json::Value = multiplier_column
        .map(serde_json::from_str)
        .transpose()
        .map_err(|error| ConfigError::Decode(format!("subscription_multiplier: {error}")))?
        .unwrap_or(serde_json::Value::Null);
    let value = generation
        .get("reasoning_billed_as_output")
        .or_else(|| nested.get("reasoning_billed_as_output"))
        .cloned()
        .unwrap_or_else(|| serde_json::json!([]));
    let providers: Vec<String> = serde_json::from_value(value)
        .map_err(|error| ConfigError::Decode(format!("reasoning_billed_as_output: {error}")))?;
    validate_reasoning_providers(&providers)?;
    Ok(providers)
}

/// The `desired_config.harness_ingest` column value: the member's canonical
/// JSON.
pub(crate) fn harness_ingest_column(config: &AstrocyteConfig) -> Result<String, ConfigError> {
    let value = serde_json::to_value(&config.harness_ingest)
        .map_err(|error| ConfigError::Canonical(error.to_string()))?;
    jcs_serialize(&value).map_err(|error| ConfigError::Canonical(format!("{error:?}")))
}

/// Reads the column back. NULL is a generation committed before the store
/// could hold the member; that generation ingested every producer, so it
/// hydrates as all on, never as off. A present value goes through the same
/// closed decode and validation as the desired document.
pub(crate) fn decode_harness_ingest_column(
    column: Option<&str>,
) -> Result<HarnessIngest, ConfigError> {
    let Some(text) = column else {
        return Ok(HarnessIngest::all_on());
    };
    let ingest: HarnessIngest = serde_json::from_str(text)
        .map_err(|error| ConfigError::Decode(format!("harness_ingest: {error}")))?;
    ingest.validate()?;
    Ok(ingest)
}

/// Reject invalid source encoding before any syntax or semantic work begins.
pub fn decode_bytes(contents: &[u8]) -> Result<DesiredContent, ConfigError> {
    let contents = std::str::from_utf8(contents)
        .map_err(|error| ConfigError::Decode(format!("desired content is not UTF-8: {error}")))?;
    decode(contents)
}

/// Decode, validate, canonicalize, and identify one desired document.
pub fn decode(contents: &str) -> Result<DesiredContent, ConfigError> {
    let json = subc_jsonc::jsonc_to_json(contents).map_err(ConfigError::Syntax)?;
    let value: serde_json::Value =
        serde_json::from_str(&json).map_err(|error| ConfigError::Syntax(error.to_string()))?;
    validate_closed_shape(&value)?;
    // Deserialize the original JSON too: serde's struct visitor sees duplicate
    // members that a Value map would already have collapsed.
    let decoded: DecodedConfig =
        serde_json::from_str(&json).map_err(|error| ConfigError::Decode(error.to_string()))?;
    canonicalize(decoded.into())
}

fn validate_closed_shape(value: &serde_json::Value) -> Result<(), ConfigError> {
    const MEMBERS: [&str; 6] = [
        "poll_interval_secs",
        "qta_connection_file",
        "fusiform_connection_file",
        "subscription_multiplier",
        "harness_ingest",
        "reasoning_billed_as_output",
    ];
    let object = value
        .as_object()
        .ok_or_else(|| ConfigError::Decode("desired root must be an object".into()))?;
    if let Some(unknown) = object
        .keys()
        .find(|member| !MEMBERS.contains(&member.as_str()))
    {
        return Err(ConfigError::Decode(format!(
            "desired root contains unknown member '{unknown}'"
        )));
    }
    if let Some(missing) = MEMBERS.iter().find(|member| !object.contains_key(**member)) {
        return Err(ConfigError::Decode(format!(
            "desired root is missing member '{missing}'"
        )));
    }
    Ok(())
}

pub(crate) fn canonicalize(config: AstrocyteConfig) -> Result<DesiredContent, ConfigError> {
    validate_config(&config)?;
    let value =
        serde_json::to_value(&config).map_err(|error| ConfigError::Canonical(error.to_string()))?;
    let canonical =
        jcs_serialize(&value).map_err(|error| ConfigError::Canonical(format!("{error:?}")))?;
    let content_hash =
        canonical_hash(&value).map_err(|error| ConfigError::Canonical(format!("{error:?}")))?;
    Ok(DesiredContent {
        config,
        canonical_bytes: canonical.into_bytes(),
        content_hash,
    })
}

pub fn intentional_empty() -> DesiredContent {
    canonicalize(AstrocyteConfig {
        poll_interval_secs: 60,
        qta_connection_file: None,
        fusiform_connection_file: None,
        reasoning_billed_as_output: Vec::new(),
        subscription_multiplier: MultiplierSetting::Recorded(MultiplierConfig {
            api_twins: Default::default(),
            plan_price_overrides: Default::default(),
            quota_providers: Default::default(),
        }),
        harness_ingest: HarnessIngest::all_on(),
    })
    .expect("the closed intentional-empty value is valid")
}

fn validate_config(config: &AstrocyteConfig) -> Result<(), ConfigError> {
    validate_poll_interval(config.poll_interval_secs)?;
    validate_nullable_nonempty("qta_connection_file", config.qta_connection_file.as_deref())?;
    validate_nullable_nonempty(
        "fusiform_connection_file",
        config.fusiform_connection_file.as_deref(),
    )?;
    config.harness_ingest.validate()?;
    validate_reasoning_providers(&config.reasoning_billed_as_output)?;
    match &config.subscription_multiplier {
        MultiplierSetting::Recorded(multiplier) => {
            multiplier.validate().map_err(ConfigError::Validation)
        }
        MultiplierSetting::NotRecorded => Ok(()),
    }
}

fn validate_reasoning_providers(providers: &[String]) -> Result<(), ConfigError> {
    let mut seen = std::collections::BTreeSet::new();
    for provider in providers {
        if provider.is_empty() || !seen.insert(provider) {
            return Err(ConfigError::Validation(format!(
                "reasoning_billed_as_output entry '{provider}' must be non-empty and listed once"
            )));
        }
    }
    Ok(())
}

fn validate_poll_interval(value: u64) -> Result<(), ConfigError> {
    if value == 0 {
        return Err(ConfigError::Validation(
            "poll_interval_secs must be at least 1".into(),
        ));
    }
    if value > MAX_POLL_INTERVAL_SECS {
        return Err(ConfigError::Validation(format!(
            "poll_interval_secs must be at most {MAX_POLL_INTERVAL_SECS}"
        )));
    }
    Ok(())
}

fn validate_nullable_nonempty(name: &str, value: Option<&str>) -> Result<(), ConfigError> {
    if value.is_some_and(str::is_empty) {
        Err(ConfigError::Validation(format!(
            "{name} must be null or a non-empty string"
        )))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The multiplier member with nothing declared, as every other test
    /// document carries it so the document stays closed.
    const NO_MULTIPLIER: &str =
        r#"{"api_twins":{},"plan_price_overrides":{},"quota_providers":{}}"#;

    /// The `harness_ingest` member with every producer on. Every test document
    /// carries it, because the document is closed and the member is required.
    const INGEST_ON: &str = r#"{"enabled":true,"exclude":[]}"#;

    fn empty() -> String {
        format!(
            r#"{{
        "poll_interval_secs": 60,
        "qta_connection_file": null,
        "fusiform_connection_file": null,
        "reasoning_billed_as_output": [],
        "subscription_multiplier": {NO_MULTIPLIER},
        "harness_ingest": {INGEST_ON}
    }}"#
        )
    }

    /// A document with the supplied JSON inserted verbatim as its
    /// `subscription_multiplier` member.
    fn with_multiplier(multiplier: &str) -> String {
        format!(
            r#"{{"poll_interval_secs":60,"qta_connection_file":null,"fusiform_connection_file":null,"reasoning_billed_as_output":[],"subscription_multiplier":{multiplier},"harness_ingest":{INGEST_ON}}}"#
        )
    }

    const CODEX: &str = r#"{
        "api_twins": {"openai-codex": "openai"},
        "plan_price_overrides": {},
        "quota_providers": {"codex": {
            "window": {"pool_key": "primary", "window_minutes": 10080},
            "scope": [{"producer": "pi", "provider": "openai-codex", "charge_basis": null}],
            "accounts": {"acct": {"plan": "openai/pro_20x", "price_override": null}}
        }}
    }"#;

    #[test]
    fn desired_jsonc_comments_and_trailing_commas_are_accepted() {
        let desired = decode(&format!(
            r#"{{
                // Connections are optional until their clients are configured.
                "poll_interval_secs": 60,
                "qta_connection_file": null,
                "fusiform_connection_file": null,
                "reasoning_billed_as_output": [],
                "subscription_multiplier": {NO_MULTIPLIER},
                "harness_ingest": {INGEST_ON},
            }}"#
        ))
        .unwrap();
        assert_eq!(desired.config.poll_interval_secs, 60);
    }

    #[test]
    fn catalog_file_is_rejected_as_an_unknown_member() {
        let error = decode(&format!(
            r#"{{
                "poll_interval_secs": 60,
                "catalog_file": null,
                "qta_connection_file": null,
                "fusiform_connection_file": null,
                "reasoning_billed_as_output": [],
                "subscription_multiplier": {NO_MULTIPLIER},
                "harness_ingest": {INGEST_ON}
            }}"#
        ))
        .unwrap_err();
        assert!(error.to_string().contains("catalog_file"));
    }

    /// The THALAMUS producer lane was removed, so a config still naming its
    /// connection file is stale. It must be refused by name, never ignored:
    /// silently dropping an unknown member is how a stale config would hide.
    #[test]
    fn retired_thalamus_connection_file_is_refused_by_name() {
        let error = decode(&format!(
            r#"{{
                "poll_interval_secs": 60,
                "qta_connection_file": null,
                "thalamus_connection_file": null,
                "fusiform_connection_file": null,
                "reasoning_billed_as_output": [],
                "subscription_multiplier": {NO_MULTIPLIER},
                "harness_ingest": {INGEST_ON}
            }}"#
        ))
        .unwrap_err();
        assert_eq!(
            error,
            ConfigError::Decode(
                "desired root contains unknown member 'thalamus_connection_file'".into()
            )
        );
    }

    /// BROCA is read from its own store, so the connection file its retired
    /// `usage.export` pull used is stale in any config that still names it,
    /// and is refused by name like any other unknown member.
    #[test]
    fn retired_broca_connection_file_is_refused_by_name() {
        let error = decode(&format!(
            r#"{{
                "poll_interval_secs": 60,
                "qta_connection_file": null,
                "broca_connection_file": null,
                "fusiform_connection_file": null,
                "reasoning_billed_as_output": [],
                "subscription_multiplier": {NO_MULTIPLIER},
                "harness_ingest": {INGEST_ON}
            }}"#
        ))
        .unwrap_err();
        assert_eq!(
            error,
            ConfigError::Decode(
                "desired root contains unknown member 'broca_connection_file'".into()
            )
        );
    }

    #[test]
    fn unknown_and_duplicate_members_are_rejected_before_canonicalization() {
        assert!(decode(&format!(
            r#"{{"poll_interval_secs":60,"qta_connection_file":null,"fusiform_connection_file":null,"reasoning_billed_as_output":[],"subscription_multiplier":{NO_MULTIPLIER},"harness_ingest":{INGEST_ON},"future":null}}"#
        ))
        .is_err());
        let error = decode(&format!(
            r#"{{"poll_interval_secs":60,"qta_connection_file":null,"qta_connection_file":"other","fusiform_connection_file":null,"reasoning_billed_as_output":[],"subscription_multiplier":{NO_MULTIPLIER},"harness_ingest":{INGEST_ON}}}"#
        ))
        .unwrap_err();
        assert!(error.to_string().contains("duplicate field"));
    }

    #[test]
    fn required_members_and_value_bounds_are_enforced() {
        assert!(decode(r#"{"poll_interval_secs":60,"qta_connection_file":null}"#).is_err());
        assert!(decode(&format!(
            r#"{{"poll_interval_secs":0,"qta_connection_file":null,"fusiform_connection_file":null,"reasoning_billed_as_output":[],"subscription_multiplier":{NO_MULTIPLIER},"harness_ingest":{INGEST_ON}}}"#
        ))
        .is_err());
        assert!(decode(&format!(
            r#"{{"poll_interval_secs":60,"qta_connection_file":"","fusiform_connection_file":null,"reasoning_billed_as_output":[],"subscription_multiplier":{NO_MULTIPLIER},"harness_ingest":{INGEST_ON}}}"#
        ))
        .is_err());
    }

    #[test]
    fn canonical_content_is_stable_and_explicitly_nullable() {
        let desired = decode(&empty()).unwrap();
        assert_eq!(desired.config, intentional_empty().config);
        assert_eq!(
            String::from_utf8(desired.canonical_bytes).unwrap(),
            r#"{"fusiform_connection_file":null,"harness_ingest":{"enabled":true,"exclude":[]},"poll_interval_secs":60,"qta_connection_file":null,"reasoning_billed_as_output":[],"subscription_multiplier":{"api_twins":{},"plan_price_overrides":{},"quota_providers":{}}}"#
        );
    }

    /// The multiplier member is required like every other member: a document
    /// written before it existed is refused by name, never given a default.
    #[test]
    fn a_document_without_the_multiplier_member_is_refused_by_name() {
        let error = decode(
            r#"{"poll_interval_secs":60,"qta_connection_file":null,"fusiform_connection_file":null}"#,
        )
        .unwrap_err();
        assert_eq!(
            error,
            ConfigError::Decode("desired root is missing member 'subscription_multiplier'".into())
        );
    }

    #[test]
    fn the_multiplier_member_is_closed_at_every_level() {
        assert!(decode(&with_multiplier(CODEX)).is_ok());
        // An unknown member inside a nested object.
        let unknown = CODEX.replace(
            r#""window_minutes": 10080"#,
            r#""window_minutes": 10080, "largest": true"#,
        );
        let error = decode(&with_multiplier(&unknown)).unwrap_err();
        assert!(error.to_string().contains("largest"), "{error}");
        // A nullable member left out is missing, not null.
        let missing = CODEX.replace(r#", "charge_basis": null"#, "");
        let error = decode(&with_multiplier(&missing)).unwrap_err();
        assert!(error.to_string().contains("charge_basis"), "{error}");
        let missing = CODEX.replace(r#", "price_override": null"#, "");
        let error = decode(&with_multiplier(&missing)).unwrap_err();
        assert!(error.to_string().contains("price_override"), "{error}");
        // A repeated account would otherwise silently replace the first.
        let duplicate = CODEX.replace(
            r#""accounts": {"acct": {"plan": "openai/pro_20x", "price_override": null}}"#,
            r#""accounts": {"acct": {"plan": "openai/pro_20x", "price_override": null}, "acct": {"plan": "openai/pro_5x", "price_override": null}}"#,
        );
        let error = decode(&with_multiplier(&duplicate)).unwrap_err();
        assert!(
            error.to_string().contains("duplicate member 'acct'"),
            "{error}"
        );
    }

    #[test]
    fn multiplier_values_are_validated() {
        for (from, to, needle) in [
            ("openai/pro_20x", "pro_20x", "plan key"),
            (
                r#""charge_basis": null"#,
                r#""charge_basis": "free""#,
                "charge_basis",
            ),
            ("10080", "0", "window_minutes"),
            (
                r#""price_override": null"#,
                r#""price_override": {"minor_units": 20000, "exponent": 2, "currency": "EUR", "period": "month"}"#,
                "USD",
            ),
        ] {
            let error = decode(&with_multiplier(&CODEX.replace(from, to))).unwrap_err();
            assert!(error.to_string().contains(needle), "{needle}: {error}");
        }
    }

    /// The production document shipped with the binary must decode under this
    /// binary's closed format, or the two cannot move together.
    #[test]
    fn the_production_config_decodes() {
        let desired = decode(include_str!("../../../docs/prod-config/astrocyte-v2.jsonc")).unwrap();
        let MultiplierSetting::Recorded(multiplier) = desired.config.subscription_multiplier else {
            panic!("a decoded document always records the member");
        };
        assert_eq!(
            multiplier.quota_providers.keys().collect::<Vec<_>>(),
            ["claude", "codex"]
        );
        assert_eq!(desired.config.harness_ingest, HarnessIngest::all_on());
    }

    #[test]
    fn the_multiplier_column_round_trips_and_null_is_not_recorded() {
        let desired = decode(&with_multiplier(CODEX)).unwrap();
        let column = multiplier_column(&desired.config).unwrap();
        assert_eq!(
            decode_multiplier_column(column.as_deref()).unwrap(),
            desired.config.subscription_multiplier
        );
        assert_eq!(
            decode_multiplier_column(None).unwrap(),
            MultiplierSetting::NotRecorded
        );
    }

    #[test]
    fn reasoning_billing_is_a_required_top_level_declaration() {
        let document = empty().replace(
            "\"reasoning_billed_as_output\": []",
            "\"reasoning_billed_as_output\": [\"openai\", \"anthropic\"]",
        );
        let desired = decode(&document).unwrap();
        assert_eq!(
            desired.config.reasoning_billed_as_output,
            ["openai", "anthropic"]
        );
        let mut value: serde_json::Value = serde_json::from_str(&document).unwrap();
        value
            .as_object_mut()
            .unwrap()
            .remove("reasoning_billed_as_output");
        assert!(decode(&value.to_string())
            .unwrap_err()
            .to_string()
            .contains("missing member 'reasoning_billed_as_output'"));
        value["reasoning_billed_as_output"] = serde_json::json!([]);
        value["subscription_multiplier"]["reasoning_billed_as_output"] = serde_json::json!([]);
        assert!(decode(&value.to_string())
            .unwrap_err()
            .to_string()
            .contains("unknown field `reasoning_billed_as_output`"));
    }

    #[test]
    fn reasoning_billing_refuses_empty_and_duplicate_providers() {
        for providers in [
            serde_json::json!([""]),
            serde_json::json!(["openai", "openai"]),
        ] {
            let document = empty().replace(
                "\"reasoning_billed_as_output\": []",
                &format!("\"reasoning_billed_as_output\": {providers}"),
            );
            assert!(decode(&document)
                .unwrap_err()
                .to_string()
                .contains("must be non-empty and listed once"));
        }
    }

    /// A document with the supplied JSON inserted verbatim as its
    /// `harness_ingest` member.
    fn with_ingest(ingest: &str) -> String {
        format!(
            r#"{{"poll_interval_secs":60,"qta_connection_file":null,"fusiform_connection_file":null,"reasoning_billed_as_output":[],"subscription_multiplier":{NO_MULTIPLIER},"harness_ingest":{ingest}}}"#
        )
    }

    #[test]
    fn an_exclude_list_naming_known_producers_decodes() {
        let desired = decode(&with_ingest(
            r#"{"enabled":true,"exclude":["goose","broca"]}"#,
        ))
        .unwrap();
        let ingest = desired.config.harness_ingest;
        assert!(!ingest.ingests("goose"));
        assert!(!ingest.ingests("broca"));
        assert!(ingest.ingests("codex"));
        let off = decode(&with_ingest(r#"{"enabled":false,"exclude":[]}"#)).unwrap();
        for key in INGEST_SOURCES {
            assert!(!off.config.harness_ingest.ingests(key), "{key}");
        }
    }

    /// A typo in `exclude` must not leave the producer it meant running.
    #[test]
    fn an_unknown_excluded_producer_is_refused_by_name() {
        let error = decode(&with_ingest(r#"{"enabled":true,"exclude":["claude"]}"#)).unwrap_err();
        assert!(
            error
                .to_string()
                .starts_with("harness_ingest.exclude names unknown producer 'claude'"),
            "{error}"
        );
    }

    #[test]
    fn a_producer_excluded_twice_is_refused_by_name() {
        let error = decode(&with_ingest(
            r#"{"enabled":true,"exclude":["pi","codex","pi"]}"#,
        ))
        .unwrap_err();
        assert_eq!(
            error,
            ConfigError::Validation(
                "harness_ingest.exclude names producer 'pi' more than once".into()
            )
        );
    }

    #[test]
    fn ingestion_off_with_a_non_empty_exclude_is_refused_as_contradictory() {
        let error = decode(&with_ingest(r#"{"enabled":false,"exclude":["pi"]}"#)).unwrap_err();
        assert!(
            error.to_string().starts_with(
                "harness_ingest.enabled is false but harness_ingest.exclude is not empty"
            ),
            "{error}"
        );
    }

    #[test]
    fn a_document_without_the_ingest_member_is_refused_by_name() {
        let error = decode(&format!(
            r#"{{"poll_interval_secs":60,"qta_connection_file":null,"fusiform_connection_file":null,"reasoning_billed_as_output":[],"subscription_multiplier":{NO_MULTIPLIER}}}"#
        ))
        .unwrap_err();
        assert_eq!(
            error,
            ConfigError::Decode("desired root is missing member 'harness_ingest'".into())
        );
    }

    #[test]
    fn the_ingest_member_is_closed_at_every_level() {
        // An unknown nested member.
        let error = decode(&with_ingest(
            r#"{"enabled":true,"exclude":[],"include":["pi"]}"#,
        ))
        .unwrap_err();
        assert!(
            error.to_string().contains("unknown field `include`"),
            "{error}"
        );
        // Both nested members are required; neither has a default.
        let error = decode(&with_ingest(r#"{"enabled":true}"#)).unwrap_err();
        assert!(
            error.to_string().contains("missing field `exclude`"),
            "{error}"
        );
        let error = decode(&with_ingest(r#"{"exclude":[]}"#)).unwrap_err();
        assert!(
            error.to_string().contains("missing field `enabled`"),
            "{error}"
        );
        // A repeated nested member would otherwise let the second win.
        let error = decode(&with_ingest(
            r#"{"enabled":true,"enabled":false,"exclude":[]}"#,
        ))
        .unwrap_err();
        assert!(
            error.to_string().contains("duplicate field `enabled`"),
            "{error}"
        );
        // Wrong types are refused, not coerced.
        assert!(decode(&with_ingest(r#"{"enabled":"yes","exclude":[]}"#)).is_err());
        assert!(decode(&with_ingest(r#"{"enabled":true,"exclude":"pi"}"#)).is_err());
        assert!(decode(&with_ingest("null")).is_err());
    }

    /// NULL is a generation committed before the store had the column. It
    /// ingested everything, so it hydrates as all on, never as off.
    #[test]
    fn the_ingest_column_round_trips_and_null_is_all_on() {
        let desired = decode(&with_ingest(r#"{"enabled":true,"exclude":["kimi"]}"#)).unwrap();
        let column = harness_ingest_column(&desired.config).unwrap();
        assert_eq!(column, r#"{"enabled":true,"exclude":["kimi"]}"#);
        assert_eq!(
            decode_harness_ingest_column(Some(&column)).unwrap(),
            desired.config.harness_ingest
        );
        assert_eq!(
            decode_harness_ingest_column(None).unwrap(),
            HarnessIngest::all_on()
        );
        // A stored value is held to the same rules as the document.
        assert!(
            decode_harness_ingest_column(Some(r#"{"enabled":true,"exclude":["nope"]}"#)).is_err()
        );
    }
}
