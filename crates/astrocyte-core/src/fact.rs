//! Producer-neutral usage-event and measurement identity.
//!
//! Astrocyte owns event identity. A producer reference remains verbatim
//! provenance, while the producer lineage makes a replay from zero a distinct
//! natural key rather than a resurrection of an earlier event. A producer with
//! no restarting reference space — one that could not observe its generation,
//! or one whose references are already content-stable — reports no lineage at
//! all; that absence is part of the identity's shape, never a value standing
//! in for it.

use serde::{Deserialize, Serialize};

use crate::canon::{canonical_string_object_hash, CanonError};

/// How an event's measurements are charged.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChargeBasis {
    MeteredApi,
    SubscriptionIncluded,
    Unknown,
}

impl ChargeBasis {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::MeteredApi => "metered_api",
            Self::SubscriptionIncluded => "subscription_included",
            Self::Unknown => "unknown",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "metered_api" => Some(Self::MeteredApi),
            "subscription_included" => Some(Self::SubscriptionIncluded),
            "unknown" => Some(Self::Unknown),
            _ => None,
        }
    }
}

/// Derives a local event identifier without adopting the producer reference as
/// Astrocyte's primary key.
///
/// `producer_lineage` is `None` when the producer cannot observe its generation
/// or its references are already stable across restarts (for example, a content
/// hash). Adding a generation to a stable reference would mint a new identity
/// for the same charge after every restart. An absent lineage omits the field
/// from the canonical key; even an empty string is a distinct, present lineage.
pub fn event_id(
    producer: &str,
    producer_lineage: Option<&str>,
    producer_ref: &str,
) -> Result<String, CanonError> {
    Ok(match producer_lineage {
        Some(lineage) => canonical_string_object_hash([
            ("producer", producer),
            ("producer_lineage", lineage),
            ("producer_ref", producer_ref),
        ]),
        None => {
            canonical_string_object_hash([("producer", producer), ("producer_ref", producer_ref)])
        }
    })
}

/// One AI workload invocation as stored before settlement and pricing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UsageEvent {
    pub event_id: String,
    pub producer: String,
    /// `None` when the producer has no restarting reference space to
    /// disambiguate (see [`event_id`]); the stored row keeps the
    /// absence visible so a reader can see which key shape minted the identity.
    pub producer_lineage: Option<String>,
    pub producer_ref: String,
    pub prior_event_ref: Option<String>,
    pub harness: Option<String>,
    pub project_root: Option<String>,
    pub session: Option<String>,
    pub run_ref: Option<String>,
    pub work_class: Option<String>,
    pub occurred_at_ms: Option<i64>,
    pub emitted_at_ms: Option<i64>,
    pub recorded_at_ms: i64,
    pub terminal_status: Option<String>,
    pub attempt_ordinal: Option<u32>,
    pub vendor_request_id: Option<String>,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub account_ref: Option<String>,
    /// The producer's credential id, carried verbatim and never derived. BROCA
    /// stamps this on every export segment (the pin the run was admitted under,
    /// frozen at admission); for api-key credentials account_ref is absent by
    /// construction, so this is the only per-account discriminator a PAYG fact
    /// carries. We store the string the producer sent and infer nothing from its
    /// shape.
    pub auth_selection: Option<String>,
    /// A later change to the pricing basis creates new charge-head records instead of
    /// rewriting the original recorded measurement facts.
    pub charge_basis: ChargeBasis,
    /// Increments once for every committed act that appends priced-charge rows.
    pub pricing_generation: u64,
    /// Set once when pricing first records a priced or subscription-covered measurement.
    pub currency: Option<String>,
}

/// One producer-declared measured quantity within an event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Measurement {
    pub event_id: String,
    pub role: String,
    pub unit: String,
    pub kind: String,
    pub source: String,
    /// Decimal value is `quantity_units × 10^quantity_exponent`.
    /// Both fields are absent together when the producer reported no quantity.
    pub quantity_units: Option<i64>,
    pub quantity_exponent: Option<i32>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identity_hash_bytes_match_persisted_format() {
        for (producer, lineage, reference, expected) in [
            (
                "broca",
                None,
                "fact-1",
                "a42007808ae1d5e2e82f0b6f1b04841f837be82279886f90ade85d639f93c848",
            ),
            (
                "broca",
                Some(""),
                "fact-1",
                "7cb989655ea45dd3aef022bb8a447cb383cb9488a8bf3f3e899476be22e82205",
            ),
            (
                "a\"b",
                Some("🦀\n"),
                "\u{0001}\\",
                "54891ad014c6f015b273cb01e2bfa27b30fa232ce77ab504b8f88b729f4d1794",
            ),
        ] {
            assert_eq!(event_id(producer, lineage, reference).unwrap(), expected);
        }
    }

    #[test]
    #[ignore = "wall-time probe; run with --ignored --nocapture --test-threads=1"]
    fn performance_event_identity_hashing() {
        let start = std::time::Instant::now();
        let allocations = crate::canon::allocation_probe::count(|| {
            for _ in 0..100_000 {
                std::hint::black_box(event_id("broca", Some("epoch-1"), "fact-1").unwrap());
            }
        });
        println!(
            "identity hashing: 100000 lineage-present identities: {allocations} allocation/reallocation calls, {:?}",
            start.elapsed()
        );
        assert!(
            allocations <= 650_000,
            "identity hashing must not own temporary strings and JSON: {allocations}"
        );
    }

    #[test]
    fn event_id_uses_every_natural_key_field() {
        let baseline = event_id("plex", Some("lineage-a"), "record-17").unwrap();
        assert_ne!(
            baseline,
            event_id("broca", Some("lineage-a"), "record-17").unwrap()
        );
        assert_ne!(
            baseline,
            event_id("plex", Some("lineage-b"), "record-17").unwrap()
        );
        assert_ne!(
            baseline,
            event_id("plex", Some("lineage-a"), "record-18").unwrap()
        );
    }

    #[test]
    fn event_id_canonicalization_prevents_concatenation_collisions() {
        let first = event_id("ab", Some("c"), "d").unwrap();
        let second = event_id("a", Some("bc"), "d").unwrap();
        assert_ne!(first, second);
    }

    #[test]
    fn lineage_absent_key_never_equals_lineage_present_key() {
        let absent = event_id("plex", None, "record-17").unwrap();
        // The empty string is a lineage a producer can legitimately send; it
        // must not fuse with an honest absence.
        assert_ne!(absent, event_id("plex", Some(""), "record-17").unwrap());
        assert_ne!(
            absent,
            event_id("plex", Some("lineage-a"), "record-17").unwrap()
        );
        // Two absent-lineage keys with the same producer and reference are one
        // identity: the disambiguator is gone, not merely unrecorded.
        assert_eq!(absent, event_id("plex", None, "record-17").unwrap());
    }
}
