//! Typed `Quantity` — K8s's `1Gi` / `100m` / `2.5` / `4Ki` /
//! `500M` notation as a parsed numeric value with the originating
//! suffix preserved for round-trip fidelity.
//!
//! ## Wire shape
//!
//! `Quantity` serializes as a **string** — bit-identical to what
//! `kubectl get -o json` emits. Consumers that need the numeric
//! value call [`Quantity::milli_value`] (canonical units: milli
//! for CPU, bytes for storage/memory). The string form survives
//! round-trip; consumers don't have to know the parser exists.
//!
//! ## Why typed
//!
//! Without typing, every consumer (engenho-kubelet's container
//! scheduler, the scheduler's resource bin-packer, the cluster
//! autoscaler at M0.5+) re-parses the same string. With Quantity
//! the parse happens once at the engenho-types boundary; the
//! numeric value flows through statically.
//!
//! ## Scope discipline
//!
//! Supports the K8s "binary SI" + "decimal SI" suffixes the
//! upstream API documents:
//!
//!   * No suffix → unitless integer or decimal (cpu: 2 means 2 cores)
//!   * `m` → milli (cpu: 100m = 0.1 cores)
//!   * `k` `M` `G` `T` `P` `E` → decimal SI (1G = 10^9)
//!   * `Ki` `Mi` `Gi` `Ti` `Pi` `Ei` → binary SI (1Gi = 2^30)
//!
//! Not yet implemented (M0.0.4 codegen target): negative
//! exponents (e.g. `1.5e-3`), `0.5Gi`-style fractional binary.
//! Operators using these will fall through to the `Other(s)`
//! variant which preserves the input verbatim.

#![allow(clippy::module_name_repetitions)]

use serde::{de::Error as DeError, Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::str::FromStr;

/// Typed K8s quantity. Carries the parsed numeric value AND the
/// originating suffix for byte-identical round-trip.
///
/// `Quantity` is the engenho equivalent of upstream's
/// `resource.Quantity`. Where upstream uses an inf-prec decimal
/// + a scale, engenho-types ships a `i128` "milli-value" — enough
/// for every reasonable cluster (max ~170 exabytes of memory in
/// a single value, ~170 trillion cores). M0.0.4 codegen can swap
/// in inf-prec if a conformance test demands it; until then this
/// is the pragmatic shape that fits in a Rust struct.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Quantity {
    /// Successfully parsed. Carries the numeric value in
    /// milli-units (×1000 of the integer value) so 100m, 0.1,
    /// and 100 all have distinct typed representations.
    Parsed {
        /// Numeric value × 1000 (so "1" → 1000, "100m" → 100,
        /// "1Ki" → 1_024_000, "2.5" → 2500).
        milli: i128,
        /// Original suffix so we can round-trip back to the
        /// exact string form. Empty for integer/decimal,
        /// `"m"` for milli, `"Ki"` etc. for binary SI.
        suffix: Suffix,
    },
    /// Failed-to-parse value — round-trips verbatim. Defensive
    /// fallthrough so a future K8s exponent we don't yet handle
    /// doesn't crash the typed pipeline.
    Other(String),
}

/// Closed enum of recognized suffixes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Suffix {
    /// No suffix — bare integer or decimal.
    None,
    /// `m` — milli (×0.001).
    Milli,
    /// Decimal SI: `k` `M` `G` `T` `P` `E` (×10^3 ... ×10^18).
    /// The inner `u8` is the exponent (k=1, M=2, ..., E=6).
    DecimalSi(u8),
    /// Binary SI: `Ki` `Mi` `Gi` `Ti` `Pi` `Ei` (×2^10 ... ×2^60).
    /// The inner `u8` is the exponent (Ki=1, ..., Ei=6).
    BinarySi(u8),
}

impl Quantity {
    /// Construct from a milli-value with no suffix (cpu: `from_millis(100)` → "100m"-equivalent).
    /// The Display will pick the most natural suffix.
    #[must_use]
    pub fn from_millis(milli: i128) -> Self {
        Self::Parsed {
            milli,
            suffix: if milli % 1000 == 0 { Suffix::None } else { Suffix::Milli },
        }
    }

    /// Get the value in milli-units (canonical numeric).
    /// Returns `None` for `Other(s)` variants we couldn't parse.
    #[must_use]
    pub fn milli_value(&self) -> Option<i128> {
        match self {
            Self::Parsed { milli, .. } => Some(*milli),
            Self::Other(_) => None,
        }
    }

    /// Get the value as i128 (rounded down). Useful for storage
    /// quantities where `1Gi` means exactly `2^30 bytes`.
    #[must_use]
    pub fn integer_value(&self) -> Option<i128> {
        Some(self.milli_value()? / 1000)
    }
}

impl FromStr for Quantity {
    type Err = QuantityParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.is_empty() {
            return Err(QuantityParseError::Empty);
        }
        let s = s.trim();
        // Find where the numeric portion ends. Numeric: optional
        // sign, digits, optional decimal point + digits.
        let suffix_start = s
            .char_indices()
            .find(|(_, c)| !c.is_ascii_digit() && *c != '.' && *c != '-' && *c != '+')
            .map(|(i, _)| i)
            .unwrap_or(s.len());
        let (num_part, suffix_part) = s.split_at(suffix_start);
        if num_part.is_empty() {
            return Err(QuantityParseError::NoNumeric);
        }
        let suffix = Suffix::from_str(suffix_part)?;
        // Parse numeric as decimal — convert to milli-units.
        let milli = parse_decimal_to_milli(num_part)?;
        let scaled = suffix.apply_to_milli(milli);
        Ok(Self::Parsed { milli: scaled, suffix })
    }
}

impl fmt::Display for Quantity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Other(s) => f.write_str(s),
            Self::Parsed { milli, suffix } => {
                // Reverse the suffix to get the un-scaled milli,
                // then format as decimal + suffix.
                let unscaled = suffix.un_apply_from_milli(*milli);
                let suffix_str = suffix.to_str();
                if unscaled % 1000 == 0 {
                    write!(f, "{}{}", unscaled / 1000, suffix_str)
                } else {
                    // Has a fractional component — print as
                    // decimal. e.g. 2500 milli unscaled → "2.5".
                    let int_part = unscaled / 1000;
                    let frac_part = (unscaled % 1000).abs();
                    if frac_part % 100 == 0 {
                        write!(f, "{}.{}{}", int_part, frac_part / 100, suffix_str)
                    } else if frac_part % 10 == 0 {
                        write!(f, "{}.{:02}{}", int_part, frac_part / 10, suffix_str)
                    } else {
                        write!(f, "{}.{:03}{}", int_part, frac_part, suffix_str)
                    }
                }
            }
        }
    }
}

impl Suffix {
    fn from_str(s: &str) -> Result<Self, QuantityParseError> {
        Ok(match s {
            "" => Self::None,
            "m" => Self::Milli,
            "k" => Self::DecimalSi(1),
            "M" => Self::DecimalSi(2),
            "G" => Self::DecimalSi(3),
            "T" => Self::DecimalSi(4),
            "P" => Self::DecimalSi(5),
            "E" => Self::DecimalSi(6),
            "Ki" => Self::BinarySi(1),
            "Mi" => Self::BinarySi(2),
            "Gi" => Self::BinarySi(3),
            "Ti" => Self::BinarySi(4),
            "Pi" => Self::BinarySi(5),
            "Ei" => Self::BinarySi(6),
            other => return Err(QuantityParseError::UnknownSuffix(other.to_string())),
        })
    }

    fn to_str(self) -> &'static str {
        match self {
            Self::None => "",
            Self::Milli => "m",
            Self::DecimalSi(1) => "k",
            Self::DecimalSi(2) => "M",
            Self::DecimalSi(3) => "G",
            Self::DecimalSi(4) => "T",
            Self::DecimalSi(5) => "P",
            Self::DecimalSi(6) => "E",
            Self::BinarySi(1) => "Ki",
            Self::BinarySi(2) => "Mi",
            Self::BinarySi(3) => "Gi",
            Self::BinarySi(4) => "Ti",
            Self::BinarySi(5) => "Pi",
            Self::BinarySi(6) => "Ei",
            Self::DecimalSi(_) | Self::BinarySi(_) => "?",
        }
    }

    /// Scale a parsed numeric (already in `*1000` form) by this
    /// suffix's multiplier. `None` is a no-op; `Milli` divides by
    /// 1000 (since `100m` = 0.1 unit = 100 milli-units, not 100
    /// units); decimal SI multiplies by 10^(3·exp); binary SI by
    /// 2^(10·exp).
    fn apply_to_milli(self, milli: i128) -> i128 {
        match self {
            Self::None => milli,
            Self::Milli => milli / 1000,
            Self::DecimalSi(exp) => milli.saturating_mul(10i128.pow(u32::from(exp) * 3)),
            Self::BinarySi(exp) => milli.saturating_mul(1i128 << (u32::from(exp) * 10)),
        }
    }

    /// Reverse of `apply_to_milli` — recover the input numeric
    /// (already in `*1000` form) so Display can reconstruct the
    /// original suffix-bearing string.
    fn un_apply_from_milli(self, scaled: i128) -> i128 {
        match self {
            Self::None => scaled,
            Self::Milli => scaled * 1000,
            Self::DecimalSi(exp) => scaled / 10i128.pow(u32::from(exp) * 3),
            Self::BinarySi(exp) => scaled / (1i128 << (u32::from(exp) * 10)),
        }
    }
}

/// Parse a decimal string into milli-units. `"2"` → 2000,
/// `"2.5"` → 2500, `"0.001"` → 1, `"100"` → 100_000.
fn parse_decimal_to_milli(s: &str) -> Result<i128, QuantityParseError> {
    let (int_part, frac_part) = match s.split_once('.') {
        Some((i, f)) => (i, f),
        None => (s, ""),
    };
    let int_val: i128 = int_part
        .parse()
        .map_err(|_| QuantityParseError::NonNumeric(s.to_string()))?;
    if frac_part.is_empty() {
        return Ok(int_val.saturating_mul(1000));
    }
    // Take up to 3 decimal digits (milli-precision); pad zeros if shorter.
    let trimmed = if frac_part.len() > 3 {
        &frac_part[..3]
    } else {
        frac_part
    };
    let mut frac_val: i128 = trimmed
        .parse()
        .map_err(|_| QuantityParseError::NonNumeric(s.to_string()))?;
    for _ in 0..(3 - trimmed.len()) {
        frac_val *= 10;
    }
    let sign = if int_val < 0 || s.starts_with('-') { -1 } else { 1 };
    Ok(int_val.saturating_mul(1000) + (frac_val * sign))
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum QuantityParseError {
    #[error("empty quantity")]
    Empty,
    #[error("no numeric portion in quantity")]
    NoNumeric,
    #[error("could not parse numeric portion: {0:?}")]
    NonNumeric(String),
    #[error("unknown quantity suffix: {0:?}")]
    UnknownSuffix(String),
}

impl Serialize for Quantity {
    fn serialize<S: Serializer>(&self, ser: S) -> Result<S::Ok, S::Error> {
        ser.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for Quantity {
    fn deserialize<D: Deserializer<'de>>(de: D) -> Result<Self, D::Error> {
        let s = String::deserialize(de)?;
        // Soft-fail to `Other(s)` for unrecognized inputs — keeps
        // the wire shape lossless. M0.0.4 codegen can decide
        // whether to harden this.
        Ok(Self::from_str(&s).unwrap_or_else(|_| Self::Other(s)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn parse(s: &str) -> Quantity {
        Quantity::from_str(s).unwrap_or_else(|e| panic!("parse {s:?}: {e}"))
    }

    #[test]
    fn parses_unitless_integers() {
        assert_eq!(parse("4").milli_value(), Some(4000));
        assert_eq!(parse("0").milli_value(), Some(0));
        assert_eq!(parse("42").integer_value(), Some(42));
    }

    #[test]
    fn parses_decimal_values() {
        assert_eq!(parse("2.5").milli_value(), Some(2500));
        assert_eq!(parse("0.001").milli_value(), Some(1));
        assert_eq!(parse("1.234").milli_value(), Some(1234));
    }

    #[test]
    fn parses_milli_suffix() {
        // 100m = 0.1 = 100 milli-units
        assert_eq!(parse("100m").milli_value(), Some(100));
        assert_eq!(parse("250m").integer_value(), Some(0));
        assert_eq!(parse("1000m").milli_value(), Some(1000));
    }

    #[test]
    fn parses_decimal_si_suffixes() {
        // 1k = 1000
        assert_eq!(parse("1k").milli_value(), Some(1_000 * 1000));
        // 1M = 1_000_000
        assert_eq!(parse("1M").milli_value(), Some(1_000_000 * 1000));
        // 5G
        assert_eq!(parse("5G").milli_value(), Some(5_000_000_000 * 1000));
    }

    #[test]
    fn parses_binary_si_suffixes() {
        // 1Ki = 1024 bytes (× 1000 milli-units)
        assert_eq!(parse("1Ki").milli_value(), Some(1024 * 1000));
        // 1Mi = 1024 * 1024 = 1_048_576 bytes
        assert_eq!(parse("1Mi").milli_value(), Some(1024 * 1024 * 1000));
        // 1Gi = 2^30 bytes
        assert_eq!(parse("1Gi").milli_value(), Some((1i128 << 30) * 1000));
        // 8Gi — the engenho-local VM's memory
        assert_eq!(parse("8Gi").integer_value(), Some(8 * (1i128 << 30)));
    }

    #[test]
    fn display_round_trips_canonical_inputs() {
        let cases = [
            "0",
            "1",
            "42",
            "100m",
            "2.5",
            "1k",
            "5G",
            "1Ki",
            "1Mi",
            "1Gi",
            "8Gi",
            "100Mi",
        ];
        for input in cases {
            let q = parse(input);
            let displayed = q.to_string();
            assert_eq!(displayed, input, "round-trip mismatch: {input}");
        }
    }

    #[test]
    fn other_variant_round_trips_verbatim() {
        // K8s could ship a unit we don't recognize; we keep it verbatim.
        let q = Quantity::Other("3.14159e+15".into());
        assert_eq!(q.to_string(), "3.14159e+15");
        assert_eq!(q.milli_value(), None);
    }

    #[test]
    fn unknown_suffix_returns_typed_error() {
        let err = Quantity::from_str("100Xi").unwrap_err();
        assert!(matches!(err, QuantityParseError::UnknownSuffix(_)));
    }

    #[test]
    fn serde_round_trip_via_json() {
        let original = parse("8Gi");
        let json = serde_json::to_string(&original).unwrap();
        // Wire shape: bare string, NOT a structured object.
        assert_eq!(json, "\"8Gi\"");
        let back: Quantity = serde_json::from_str(&json).unwrap();
        assert_eq!(back, original);
    }

    #[test]
    fn ord_compares_by_canonical_milli() {
        // 1Gi > 500Mi (the binary-SI ones).
        assert!(parse("1Gi") > parse("500Mi"));
        // 100m < 1
        assert!(parse("100m") < parse("1"));
        // 1k == 1000
        assert_eq!(parse("1k").milli_value(), parse("1000").milli_value());
    }

    /// Property-based: any sane "integer + recognized suffix"
    /// input parses + serializes back to the SAME string.
    proptest! {
        #[test]
        fn arb_int_plus_suffix_round_trips(
            int_val in 0i64..1_000_000,
            suffix_idx in 0usize..13,
        ) {
            let suffixes = ["", "m", "k", "M", "G", "T", "P", "E", "Ki", "Mi", "Gi", "Ti", "Pi"];
            let input = format!("{int_val}{}", suffixes[suffix_idx]);
            let q = Quantity::from_str(&input).unwrap();
            let displayed = q.to_string();
            prop_assert_eq!(displayed, input);
        }

        /// Numeric ordering matches what consumers expect — if input
        /// A's milli-value < input B's milli-value, the typed PartialOrd
        /// agrees.
        #[test]
        fn arb_milli_value_ordering_matches_partial_ord(
            a_val in 1i64..1_000_000,
            b_val in 1i64..1_000_000,
        ) {
            let a = Quantity::from_str(&format!("{a_val}m")).unwrap();
            let b = Quantity::from_str(&format!("{b_val}m")).unwrap();
            let by_milli = a.milli_value().cmp(&b.milli_value());
            let by_partial_ord = a.cmp(&b);
            prop_assert_eq!(by_milli, by_partial_ord);
        }
    }

    #[test]
    fn from_millis_picks_natural_suffix() {
        // Exact integer → no suffix.
        let q = Quantity::from_millis(4000);
        assert_eq!(q.to_string(), "4");
        // Sub-integer → milli suffix.
        let q = Quantity::from_millis(100);
        assert_eq!(q.to_string(), "100m");
    }
}
