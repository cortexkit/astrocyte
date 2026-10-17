//! One stored token role, as a harness reader derived it.
//!
//! Astrocyte code, not copied from tokscale. The ledger stores disjoint roles
//! (`input`, `cache_read`, `cache_write`, `output`, `reasoning`). A harness
//! that reports a role gross, with another role nested inside it, only gives
//! the disjoint quantity by subtraction, and a subtraction can be impossible
//! or its inputs unknown. This type keeps those outcomes apart instead of
//! collapsing them into a number.

/// A disjoint role quantity, or why there is none.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DerivedRole {
    /// The disjoint quantity.
    Measured(u64),
    /// The harness did not report the counter: nothing to store.
    Absent,
    /// The counter was reported, but the disjoint part cannot be known
    /// without a guess: a counter to subtract is absent, or the harness's
    /// convention for a reported non-zero counter was never measured.
    Underivable,
    /// The subtraction would go negative, which the harness's measured
    /// convention forbids. Stored without a quantity so it never prices;
    /// never clamped.
    Conflicted,
}

impl DerivedRole {
    pub fn quantity(self) -> Option<u64> {
        match self {
            Self::Measured(quantity) => Some(quantity),
            Self::Absent | Self::Underivable | Self::Conflicted => None,
        }
    }

    /// `gross - nested`, for a harness whose `nested` counter is reported
    /// inside `gross`. An absent `nested` leaves the difference unknown.
    pub fn difference(gross: Option<u64>, nested: Option<u64>) -> Self {
        match (gross, nested) {
            (None, _) => Self::Absent,
            (Some(_), None) => Self::Underivable,
            (Some(gross), Some(nested)) => gross
                .checked_sub(nested)
                .map_or(Self::Conflicted, Self::Measured),
        }
    }

    /// A counter stored as reported.
    pub fn verbatim(counter: Option<u64>) -> Self {
        counter.map_or(Self::Absent, Self::Measured)
    }

    /// This role, unless `unmeasured` holds, in which case a reported
    /// counter becomes [`Self::Underivable`].
    pub fn unless(self, unmeasured: bool) -> Self {
        match self {
            Self::Measured(_) if unmeasured => Self::Underivable,
            other => other,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn difference_keeps_absent_unknown_and_impossible_apart() {
        assert_eq!(
            DerivedRole::difference(Some(10), Some(3)),
            DerivedRole::Measured(7)
        );
        assert_eq!(DerivedRole::difference(None, Some(3)), DerivedRole::Absent);
        assert_eq!(
            DerivedRole::difference(Some(10), None),
            DerivedRole::Underivable
        );
        assert_eq!(
            DerivedRole::difference(Some(3), Some(10)),
            DerivedRole::Conflicted
        );
        assert_eq!(
            DerivedRole::verbatim(Some(4)).unless(true),
            DerivedRole::Underivable
        );
        assert_eq!(
            DerivedRole::verbatim(None).unless(true),
            DerivedRole::Absent
        );
    }
}
