//! Money: signed nanodollar integers with checked arithmetic.
//!
//! The money rules, all load-bearing:
//! - Every monetary value is a SIGNED `i64` nanodollar count (1 dollar =
//!   10^9 nanos). Corrections are signed deltas, so any correctable total can
//!   transiently go negative — unsigned money types are banned everywhere.
//! - Intermediate arithmetic is `i128` and CHECKED. Overflow is a data error
//!   (`MoneyError::OutOfRange`), never a panic, wrap, or saturation: the caller
//!   records the segment as UNPRICED(arithmetic_out_of_range) and moves on.
//! - There is exactly ONE rounding site: [`round_nano_num`]. Callers build an
//!   exact integer numerator at [`NUMERATOR_SCALE`], and the single division
//!   rounds half-to-even once. No float ever touches a monetary value.

/// The fixed scale of the numerator [`round_nano_num`] accepts: the numerator
/// counts millionths of a nanodollar, so dividing by this constant yields
/// nanodollars.
pub const NUMERATOR_SCALE: i128 = 1_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MoneyError {
    /// A value or intermediate exceeded the representable range. The fact must
    /// persist as UNPRICED(`arithmetic_out_of_range`), never a wrapped number.
    OutOfRange,
}

/// THE single rounding site: divide a summed numerator by
/// [`NUMERATOR_SCALE`], rounding half-to-even, and narrow to `i64` nanodollars.
///
/// Half-to-even (banker's rounding) keeps long sums unbiased; a per-class
/// rounding scheme would accumulate systematic drift across millions of facts.
pub fn round_nano_num(numerator: i128) -> Result<i64, MoneyError> {
    let scale = NUMERATOR_SCALE;
    let quot = numerator.div_euclid(scale);
    let rem = numerator.rem_euclid(scale); // always in [0, scale)
    let double = rem.checked_mul(2).ok_or(MoneyError::OutOfRange)?;
    let rounded = if double > scale {
        quot + 1
    } else if double < scale {
        quot
    } else {
        // Exactly half: round to even.
        if quot % 2 == 0 {
            quot
        } else {
            quot + 1
        }
    };
    i64::try_from(rounded).map_err(|_| MoneyError::OutOfRange)
}

/// Rounds an exact fraction of nanodollars through [`round_nano_num`].
///
/// A month or a percentage need not divide the fixed numerator scale. Preserve
/// the whole nanos and whether the remainder is below, at, or above half a nano
/// instead of rounding to that scale first. Only those three remainder classes
/// affect the final half-even result, including for negative fractions.
pub fn round_nano_ratio(numerator: i128, denominator: i128) -> Result<i64, MoneyError> {
    if denominator <= 0 {
        return Err(MoneyError::OutOfRange);
    }
    let whole = numerator.div_euclid(denominator);
    let remainder = numerator.rem_euclid(denominator);
    let marker = match remainder.cmp(&(denominator - remainder)) {
        std::cmp::Ordering::Less => 0,
        std::cmp::Ordering::Equal => NUMERATOR_SCALE / 2,
        std::cmp::Ordering::Greater => NUMERATOR_SCALE / 2 + 1,
    };
    let encoded = whole
        .checked_mul(NUMERATOR_SCALE)
        .and_then(|whole| whole.checked_add(marker))
        .ok_or(MoneyError::OutOfRange)?;
    round_nano_num(encoded)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_half_even_midpoints() {
        // 1.5 nanos → 2 (even), 2.5 → 2 (even), 0.5 → 0, 3.5 → 4.
        assert_eq!(round_nano_num(1_500_000).unwrap(), 2);
        assert_eq!(round_nano_num(2_500_000).unwrap(), 2);
        assert_eq!(round_nano_num(500_000).unwrap(), 0);
        assert_eq!(round_nano_num(3_500_000).unwrap(), 4);
    }

    #[test]
    fn round_negative_midpoints_are_even_too() {
        // Signed money: corrections produce negative numerators. rem_euclid keeps
        // the remainder non-negative so the same half-even rule applies.
        assert_eq!(round_nano_num(-1_500_000).unwrap(), -2); // -1.5 → -2 (even)
        assert_eq!(round_nano_num(-2_500_000).unwrap(), -2); // -2.5 → -2 (even)
        assert_eq!(round_nano_num(-499_999).unwrap(), 0); // just under half → 0
    }

    #[test]
    fn round_plain_cases() {
        assert_eq!(round_nano_num(0).unwrap(), 0);
        assert_eq!(round_nano_num(1_000_000).unwrap(), 1);
        assert_eq!(round_nano_num(1_499_999).unwrap(), 1);
        assert_eq!(round_nano_num(1_500_001).unwrap(), 2);
        assert_eq!(round_nano_num(-1_000_000).unwrap(), -1);
    }

    #[test]
    fn overflow_is_error_never_wrap() {
        let huge = i128::MAX / 2;
        // round itself must reject values that cannot narrow to i64.
        assert_eq!(round_nano_num(huge), Err(MoneyError::OutOfRange));
    }

    #[test]
    fn rational_rounding_preserves_exact_half_boundaries_and_signs() {
        for (numerator, denominator, expected) in [
            (1, 2, 0),
            (3, 2, 2),
            (-3, 2, -2),
            (-5, 2, -2),
            (5_000_001, 10_000_000, 1),
            (4_999_999, 10_000_000, 0),
            (-5_000_001, 10_000_000, -1),
            (i128::MAX / 2 + 1, i128::MAX, 1),
        ] {
            assert_eq!(round_nano_ratio(numerator, denominator), Ok(expected));
        }
        assert_eq!(
            round_nano_ratio(i128::from(i64::MAX) + 1, 1),
            Err(MoneyError::OutOfRange)
        );
        assert_eq!(round_nano_ratio(1, 0), Err(MoneyError::OutOfRange));
    }
}
