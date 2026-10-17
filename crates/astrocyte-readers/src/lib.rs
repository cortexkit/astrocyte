// Copied from tokscale v4.17.0, crates/tokscale-core/src/lib.rs.
// tokscale is MIT licensed, Copyright (c) 2025 Junho Yeo.
// See ../LICENSE-tokscale for the full upstream license text.

//! Readers for coding-harness session stores.

pub mod file_identity;
pub mod provider_identity;
pub mod sessions;
pub mod sqlite_uri;

/// Per-record token measurements.
///
/// ASTROCYTE DIVERGENCE: tokscale stores concrete `i64` buckets because it only
/// aggregates them. Readers must preserve whether a harness omitted a bucket,
/// so this copy uses `Option<u64>` and keeps reported zero distinct from absent.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TokenBreakdown {
    pub input: Option<u64>,
    pub output: Option<u64>,
    pub cache_read: Option<u64>,
    pub cache_write: Option<u64>,
    pub reasoning: Option<u64>,
}

impl TokenBreakdown {
    /// Add every reported token bucket from `other`, saturating each field.
    /// An absent measurement contributes nothing and does not manufacture zero.
    pub fn add_assign_saturating(&mut self, other: &Self) {
        fn add_bucket(total: &mut Option<u64>, value: Option<u64>) {
            if let Some(value) = value {
                *total = Some(total.unwrap_or(0).saturating_add(value));
            }
        }

        add_bucket(&mut self.input, other.input);
        add_bucket(&mut self.output, other.output);
        add_bucket(&mut self.cache_read, other.cache_read);
        add_bucket(&mut self.cache_write, other.cache_write);
        add_bucket(&mut self.reasoning, other.reasoning);
    }

    pub fn total(&self) -> u64 {
        self.input
            .unwrap_or(0)
            .saturating_add(self.output.unwrap_or(0))
            .saturating_add(self.cache_read.unwrap_or(0))
            .saturating_add(self.cache_write.unwrap_or(0))
            .saturating_add(self.reasoning.unwrap_or(0))
    }
}

impl std::ops::AddAssign<&TokenBreakdown> for TokenBreakdown {
    fn add_assign(&mut self, other: &TokenBreakdown) {
        self.add_assign_saturating(other);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_breakdown_add_assign_includes_every_field() {
        // ASTROCYTE DIVERGENCE: `Option` keeps an absent measurement distinct
        // from a producer-reported zero.
        let mut total = TokenBreakdown {
            input: Some(1),
            output: Some(2),
            cache_read: Some(3),
            cache_write: Some(4),
            reasoning: Some(5),
        };
        total += &TokenBreakdown {
            input: Some(10),
            output: Some(20),
            cache_read: Some(30),
            cache_write: Some(40),
            reasoning: Some(50),
        };

        assert_eq!(
            total,
            TokenBreakdown {
                input: Some(11),
                output: Some(22),
                cache_read: Some(33),
                cache_write: Some(44),
                reasoning: Some(55),
            }
        );
    }

    #[test]
    fn token_breakdown_add_assign_saturates_each_field() {
        // ASTROCYTE DIVERGENCE: token counts are unsigned optional measurements.
        let mut total = TokenBreakdown {
            input: Some(u64::MAX),
            output: Some(1),
            cache_read: Some(u64::MAX - 1),
            cache_write: None,
            reasoning: Some(100),
        };
        total += &TokenBreakdown {
            input: Some(1),
            output: None,
            cache_read: Some(10),
            cache_write: Some(7),
            reasoning: Some(23),
        };

        assert_eq!(total.input, Some(u64::MAX));
        assert_eq!(total.output, Some(1));
        assert_eq!(total.cache_read, Some(u64::MAX));
        assert_eq!(total.cache_write, Some(7));
        assert_eq!(total.reasoning, Some(123));
    }

    #[test]
    fn token_breakdown_keeps_absent_distinct_from_reported_zero() {
        let absent = TokenBreakdown::default();
        let reported_zero = TokenBreakdown {
            cache_write: Some(0),
            ..TokenBreakdown::default()
        };

        assert_ne!(absent, reported_zero);
        assert_eq!(absent.total(), reported_zero.total());
    }
}
