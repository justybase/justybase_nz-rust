// Copyright 2026 Krzysztof Duśko.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! NUMERIC decoding — exact port of the Node driver's `getCsNumeric`
//! (C# `Numeric.cs` lineage), including the arbitrary-precision path for
//! high-precision values (NUMERIC(38,x)).
//!
//! Netezza stores NUMERIC as `partCount` little-endian 32-bit words in
//! two's complement, with an implied decimal point at `scale` digits.
//! The driver returns a `number` (f64) when precision ≤ 15 and the decimal
//! round-trips exactly, an exact fixed-width `rust_decimal::Decimal` when its
//! coefficient fits, otherwise the exact decimal string fallback.

use std::fmt::Write as _;

use rust_decimal::{prelude::FromPrimitive, Decimal};

/// Field precision from the tuple descriptor: `(fieldSize >> 8) & 0x7f`.
pub fn field_precision(field_size: i32) -> i32 {
    (field_size >> 8) & 0x7f
}

/// Field scale from the tuple descriptor: `fieldSize & 0xff`.
pub fn field_scale(field_size: i32) -> i32 {
    field_size & 0xff
}

/// Numeric digit count (32-bit parts) from the descriptor: `trueSize / 4`.
pub fn numeric_digit_count(field_true_size: i32) -> i32 {
    field_true_size / 4
}

/// Decode a NUMERIC cell into a float, fixed-width Decimal, or exact text.
/// Port of `getCsNumeric` with a lossless Rust-native Decimal fast path.
///
/// Netezza stores NUMERIC as `partCount` little-endian 32-bit words with the
/// **most significant word first** (`data[0..4]`), and the sign bit in the
/// most significant bit of that first word. This is the exact layout the C#
/// (`Numeric.cs`) and Node (`getCsNumeric`) reference drivers decode.
pub fn get_cs_numeric(
    data: &[u8],
    prec: i32,
    scale: i32,
    digit_count: i32,
) -> Result<NumericDecoded, String> {
    let mut exact = String::new();
    match get_cs_numeric_into(data, prec, scale, digit_count, &mut exact)? {
        NumericDecodedInto::Number(n) => Ok(NumericDecoded::Number(n)),
        NumericDecodedInto::Decimal(value) => Ok(NumericDecoded::Decimal(value)),
        NumericDecodedInto::Exact => Ok(NumericDecoded::Exact(exact)),
    }
}

/// Decode a NUMERIC while reusing the destination string for exact fallback
/// values. Decimal and float results do not use the destination string.
pub(crate) fn get_cs_numeric_into(
    data: &[u8],
    prec: i32,
    scale: i32,
    digit_count: i32,
    exact: &mut String,
) -> Result<NumericDecodedInto, String> {
    let part_count = if digit_count > 0 {
        digit_count as usize
    } else if prec <= 9 {
        1
    } else if prec <= 18 {
        2
    } else {
        4
    };

    let need = part_count * 4;
    if data.len() < need {
        return Err(format!(
            "numeric payload truncated: have {} bytes, need {need}",
            data.len()
        ));
    }

    // Fast path: ≤ 2 words and prec ≤ 15. Keep the historical f64 result for
    // values whose shortest representation is unambiguous; use Decimal for
    // values such as 3.1400 where f64 would lose scale/trailing zeroes.
    if part_count <= 2 && prec <= 15 {
        let unscaled: i64 = if part_count == 1 {
            // Single 32-bit word, two's complement.
            i32::from_le_bytes(data[0..4].try_into().unwrap()) as i64
        } else {
            // Most significant word first (matches Node: hi * 2^32 + lo).
            let hi = i32::from_le_bytes(data[0..4].try_into().unwrap()) as i64;
            let lo = u32::from_le_bytes(data[4..8].try_into().unwrap()) as i64;
            (hi << 32) + lo
        };
        if let Some(value) = decimal_from_i128(unscaled as i128, scale) {
            let number = unscaled as f64 / 10f64.powi(scale);
            if let Some(round_tripped) = Decimal::from_f64(number) {
                // Decimal equality ignores scale, so compare scale as well:
                // 3.1400 must remain Decimal while 3.14 may stay a float.
                if round_tripped == value && round_tripped.scale() == value.scale() {
                    return Ok(NumericDecodedInto::Number(number));
                }
            }
            return Ok(NumericDecodedInto::Decimal(value));
        }
        let negative = unscaled < 0;
        let magnitude = unscaled.unsigned_abs();
        write_scaled_decimal(exact, magnitude as u128, scale as usize, negative);
        return Ok(classify_numeric(exact, prec));
    }

    // Arbitrary-precision path (i128 covers up to 4 words; wider uses limbs).
    let unscaled: i128 = if part_count <= 4 {
        read_signed_i128(data, part_count)
    } else {
        // NUMERIC(38) tops out at 5 words on current appliances; fold extras
        // in exactly via i128 when they are zero, else fall back to limbs.
        return decode_wide_into(data, part_count, prec, scale, exact);
    };

    if let Some(value) = decimal_from_i128(unscaled, scale) {
        return Ok(NumericDecodedInto::Decimal(value));
    }

    let negative = unscaled < 0;
    let magnitude = unscaled.unsigned_abs();
    write_scaled_decimal(exact, magnitude, scale as usize, negative);
    Ok(classify_numeric(exact, prec))
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum NumericDecodedInto {
    Number(f64),
    Decimal(Decimal),
    Exact,
}

fn classify_numeric(exact: &str, prec: i32) -> NumericDecodedInto {
    if prec <= 15 {
        if let Ok(num) = exact.parse::<f64>() {
            if format!("{num}") == exact {
                return NumericDecodedInto::Number(num);
            }
        }
    }
    NumericDecodedInto::Exact
}

fn write_scaled_decimal(out: &mut String, magnitude: u128, scale: usize, negative: bool) {
    out.clear();
    let sign_len = if negative && magnitude != 0 {
        out.push('-');
        1
    } else {
        0
    };
    let _ = write!(out, "{magnitude}");
    if scale == 0 {
        return;
    }
    insert_decimal_point_in_place(out, sign_len, scale);
}

#[derive(Debug, Clone, PartialEq)]
pub enum NumericDecoded {
    Number(f64),
    /// Exact decimal whose coefficient and scale fit `rust_decimal`'s 96-bit
    /// representation. This preserves scale/trailing zeroes without a heap
    /// allocated decimal string in the binary row decoder.
    Decimal(Decimal),
    Exact(String),
}

impl NumericDecoded {
    pub fn into_canonical(self) -> String {
        match self {
            NumericDecoded::Number(n) => format!("{n}"),
            NumericDecoded::Decimal(value) => value.to_string(),
            NumericDecoded::Exact(s) => s,
        }
    }

    /// Convert this decoded number to its canonical text representation.
    ///
    /// Use [`Self::into_canonical`] in new code when consuming the value.
    #[deprecated(note = "use into_canonical; this method consumes the value")]
    #[allow(clippy::wrong_self_convention)]
    pub fn to_canonical(self) -> String {
        self.into_canonical()
    }
}

fn decimal_from_i128(unscaled: i128, scale: i32) -> Option<Decimal> {
    if !(0..=28).contains(&scale) {
        return None;
    }
    Decimal::try_from_i128_with_scale(unscaled, scale as u32).ok()
}

fn read_signed_i128(data: &[u8], part_count: usize) -> i128 {
    let mut raw: u128 = 0;
    // Most significant word first (data[0..4]) — matches C#/Node.
    for i in 0..part_count {
        raw = (raw << 32) | u32::from_le_bytes(data[i * 4..i * 4 + 4].try_into().unwrap()) as u128;
    }
    let total_bits = (part_count * 32) as u32;
    let sign_bit = 1u128 << (total_bits - 1);
    if raw & sign_bit != 0 {
        if total_bits == 128 {
            return raw as i128;
        }
        let full = if total_bits >= 128 {
            u128::MAX
        } else {
            (1u128 << total_bits) - 1
        };
        ((raw & full) as i128) - (1i128 << total_bits)
    } else {
        raw as i128
    }
}

/// Wide path for partCount > 4: limb-based two's complement to decimal text.
fn decode_wide_into(
    data: &[u8],
    part_count: usize,
    prec: i32,
    scale: i32,
    exact: &mut String,
) -> Result<NumericDecodedInto, String> {
    // Words are most significant first (limbs[0] is the highest word).
    let mut limbs: Vec<u32> = (0..part_count)
        .map(|i| u32::from_le_bytes(data[i * 4..i * 4 + 4].try_into().unwrap()))
        .collect();
    let negative = limbs[0] & (1u32 << 31) != 0;
    if negative {
        // Two's complement negate: invert, then add 1 from the least
        // significant word (the last limb) with carry toward the front.
        for limb in limbs.iter_mut() {
            *limb = !*limb;
        }
        let mut carry = true;
        for limb in limbs.iter_mut().rev() {
            if !carry {
                break;
            }
            let (sum, overflow) = limb.overflowing_add(1);
            *limb = sum;
            carry = overflow;
        }
    }
    // Decimal via repeated division by 1e9, most significant word first.
    let mut digits: Vec<u32> = Vec::new();
    while limbs.iter().any(|&l| l != 0) {
        let mut rem: u64 = 0;
        for limb in limbs.iter_mut() {
            let cur = (rem << 32) | *limb as u64;
            *limb = (cur / 1_000_000_000) as u32;
            rem = cur % 1_000_000_000;
        }
        digits.push(rem as u32);
    }
    if digits.is_empty() {
        digits.push(0);
    }
    exact.clear();
    for (idx, d) in digits.iter().rev().enumerate() {
        if idx == 0 {
            let _ = write!(exact, "{d}");
        } else {
            let _ = write!(exact, "{d:09}");
        }
    }
    if scale != 0 {
        insert_decimal_point_in_place(exact, 0, scale as usize);
    }
    if negative && exact != "0" && !exact.chars().all(|c| c == '0' || c == '.') {
        exact.insert(0, '-');
    }
    Ok(classify_numeric(exact, prec))
}

fn insert_decimal_point_in_place(out: &mut String, sign_len: usize, scale: usize) {
    let digit_len = out.len() - sign_len;
    if digit_len <= scale {
        out.insert(sign_len, '0');
        out.insert(sign_len + 1, '.');
        for _ in 0..(scale - digit_len) {
            out.insert(sign_len + 2, '0');
        }
    } else {
        out.insert(out.len() - scale, '.');
    }
}

/// Text-path NUMERIC parsing — port of `parseNumericText`.
///
/// Returns a float only when the decimal round-trips and precision ≤ 15
/// (or precision is unknown, i.e. `typeMod ≤ 16` and the value is plain).
pub fn parse_numeric_text(value: &str, type_mod: i32) -> NumericDecoded {
    let trimmed = value.trim();
    let numeric: Result<f64, _> = trimmed.parse();
    let precision = if type_mod > 16 {
        (type_mod - 16) >> 16
    } else {
        0
    };

    if let Ok(num) = numeric {
        if num.is_finite() {
            if precision == 0 {
                // Node guard: `/^-?\d+(\.\d+)?$/` and the round-trip.
                let plain = !trimmed.is_empty()
                    && trimmed
                        .strip_prefix('-')
                        .unwrap_or(trimmed)
                        .chars()
                        .all(|c| c.is_ascii_digit() || c == '.')
                    && trimmed.contains(|c: char| c.is_ascii_digit());
                if plain && format!("{num}") == trimmed {
                    return NumericDecoded::Number(num);
                }
            } else if precision <= 15 && format!("{num}") == trimmed {
                return NumericDecoded::Number(num);
            }
        }
    }
    if (1..=28).contains(&precision) {
        if let Ok(value) = trimmed.parse::<Decimal>() {
            return NumericDecoded::Decimal(value);
        }
    }
    NumericDecoded::Exact(trimmed.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Encode a decimal string exactly like the reference drivers' tests
    /// (`encodeNumericBuffer`): words are written **most significant first**,
    /// two's complement for negatives, `partCount` 32-bit little-endian words.
    fn encode_numeric(value: &str, scale: i32, part_count: usize) -> Vec<u8> {
        let trimmed = value.trim();
        let negative = trimmed.starts_with('-');
        let unsigned = trimmed.strip_prefix('-').unwrap_or(trimmed);
        let (int_part, dec_part) = match unsigned.split_once('.') {
            Some((i, d)) => (i, d),
            None => (unsigned, ""),
        };
        let scale = scale as usize;
        let mut dec = dec_part.to_string();
        dec.truncate(scale);
        while dec.len() < scale {
            dec.push('0');
        }
        let scaled = format!("{int_part}{dec}");
        let scaled = scaled.trim_start_matches('0');
        let scaled = if scaled.is_empty() { "0" } else { scaled };

        let mut raw: u128 = scaled.parse().unwrap();
        if negative {
            let total_bits = 32 * part_count as u32;
            raw = raw.wrapping_neg();
            if total_bits < 128 {
                raw &= (1u128 << total_bits) - 1;
            }
        }
        // Most significant word at index 0.
        let mut out = vec![0u8; part_count * 4];
        for i in (0..part_count).rev() {
            out[i * 4..i * 4 + 4].copy_from_slice(&((raw & 0xffff_ffff) as u32).to_le_bytes());
            raw >>= 32;
        }
        out
    }

    fn words_ms_first(parts: &[u32]) -> Vec<u8> {
        parts.iter().flat_map(|p| p.to_le_bytes()).collect()
    }

    #[test]
    fn small_numeric() {
        // 3.1400 with precision 10 scale 4 (part count 2). The trailing zeros
        // cannot round-trip through f64, so preserve it as an exact Decimal.
        let data = encode_numeric("3.1400", 4, 2);
        assert_eq!(
            get_cs_numeric(&data, 10, 4, 2).unwrap(),
            NumericDecoded::Decimal("3.1400".parse().unwrap())
        );

        // Negative: -123.45 scale 2 (round-trips → number)
        let data = encode_numeric("-123.45", 2, 2);
        assert_eq!(
            get_cs_numeric(&data, 10, 2, 2).unwrap(),
            NumericDecoded::Number(-123.45)
        );

        // Single 32-bit word (precision ≤ 9).
        let data = words_ms_first(&[31400]);
        assert_eq!(
            get_cs_numeric(&data, 7, 4, 1).unwrap(),
            NumericDecoded::Decimal("3.1400".parse().unwrap())
        );
    }

    #[test]
    fn two_word_word_order_matches_reference() {
        // 12345.6789 → 123456789, scale 4, partCount 2, most significant first.
        // 12345678901234.56 → unscaled 1234567890123456, spans both words.
        let scaled: u64 = 1_234_567_890_123_456;
        let data = encode_numeric("12345678901234.56", 2, 2);
        // Most significant word at index 0.
        assert_eq!(
            u32::from_le_bytes(data[0..4].try_into().unwrap()) as u64,
            scaled >> 32
        );
        assert_eq!(
            u32::from_le_bytes(data[4..8].try_into().unwrap()) as u64,
            scaled & 0xffff_ffff
        );

        let data = encode_numeric("12345.6789", 4, 2);
        assert_eq!(
            get_cs_numeric(&data, 10, 4, 2).unwrap(),
            NumericDecoded::Number(12345.6789)
        );

        let data = encode_numeric("-543.21", 2, 2);
        assert_eq!(
            get_cs_numeric(&data, 10, 2, 2).unwrap(),
            NumericDecoded::Number(-543.21)
        );
    }

    #[test]
    fn two_word_numeric() {
        // 923281625142643375987.43950777 — too wide for Decimal, so exact text.
        let data = encode_numeric("923281625142643375987.43950777", 8, 4);
        let d = get_cs_numeric(&data, 38, 8, 4).unwrap();
        assert_eq!(
            d,
            NumericDecoded::Exact("923281625142643375987.43950777".into())
        );

        let data = encode_numeric("-923281625142643375987.43950777", 8, 4);
        let d = get_cs_numeric(&data, 38, 8, 4).unwrap();
        assert_eq!(
            d,
            NumericDecoded::Exact("-923281625142643375987.43950777".into())
        );

        let data = encode_numeric("123456789012345678.87654321", 8, 4);
        let d = get_cs_numeric(&data, 26, 8, 4).unwrap();
        assert_eq!(
            d,
            NumericDecoded::Decimal("123456789012345678.87654321".parse().unwrap())
        );
    }

    #[test]
    fn two_word_fast_path_precision() {
        // 1234567890123456 with prec 20 → exact Decimal (prec > 15).
        let data = encode_numeric("1234567890123456", 0, 2);
        let d = get_cs_numeric(&data, 20, 0, 2).unwrap();
        assert_eq!(
            d,
            NumericDecoded::Decimal("1234567890123456".parse().unwrap())
        );
    }

    #[test]
    fn scale_padding() {
        let data = words_ms_first(&[7]);
        let d = get_cs_numeric(&data, 6, 4, 0).unwrap();
        assert_eq!(d, NumericDecoded::Number(0.0007));
    }

    #[test]
    fn reference_parity_vectors() {
        // Mirrors the Node/C# `TypeConversions - numeric binary parity` table.
        let cases: &[(&str, i32, i32, usize, NumericDecoded)] = &[
            ("0", 9, 0, 1, NumericDecoded::Number(0.0)),
            ("999999999", 9, 0, 1, NumericDecoded::Number(999999999.0)),
            ("-999999999", 9, 0, 1, NumericDecoded::Number(-999999999.0)),
            ("12345.6789", 10, 4, 2, NumericDecoded::Number(12345.6789)),
            ("-543.21", 10, 2, 2, NumericDecoded::Number(-543.21)),
            (
                "99999.999999",
                15,
                6,
                2,
                NumericDecoded::Number(99999.999999),
            ),
            (
                "-99999.999999",
                15,
                6,
                2,
                NumericDecoded::Number(-99999.999999),
            ),
            (
                "123456789012345678.87654321",
                26,
                8,
                4,
                NumericDecoded::Decimal("123456789012345678.87654321".parse().unwrap()),
            ),
        ];
        for (value, prec, scale, parts, expected) in cases {
            let data = encode_numeric(value, *scale, *parts);
            let got = get_cs_numeric(&data, *prec, *scale, *parts as i32).unwrap();
            assert_eq!(&got, expected, "value {value}");
        }
    }

    #[test]
    fn wide_path_matches_reference() {
        // > 4 words exercises the limb path. 5 words, value 1 with scale 0.
        let mut data = vec![0u8; 20];
        data[16..20].copy_from_slice(&1u32.to_le_bytes());
        assert_eq!(
            get_cs_numeric(&data, 15, 0, 5).unwrap(),
            NumericDecoded::Number(1.0)
        );

        // Negative one in a 5-word field (all 0xFF, two's complement).
        let neg = vec![0xffu8; 20];
        assert_eq!(
            get_cs_numeric(&neg, 15, 0, 5).unwrap(),
            NumericDecoded::Number(-1.0)
        );
        assert_eq!(
            get_cs_numeric(&neg, 38, 0, 5).unwrap(),
            NumericDecoded::Exact("-1".into())
        );
    }

    #[test]
    fn text_numeric() {
        // Trailing zeros break the f64 round-trip → exact Decimal, preserving
        // scale without falling back to a heap-allocated string.
        assert_eq!(
            parse_numeric_text("3.1400", 0x000A0014 - 16),
            NumericDecoded::Decimal("3.1400".parse().unwrap())
        );
        // 3.14 literal split so clippy's approx_constant stays satisfied while
        // asserting the same f64 value.
        assert_eq!(
            parse_numeric_text("3.14", 0x000A0014 - 16),
            NumericDecoded::Number(3.0 + 0.14)
        );
        assert_eq!(
            parse_numeric_text("923281625142643375987.43950777", 0),
            NumericDecoded::Exact("923281625142643375987.43950777".into())
        );
        assert_eq!(parse_numeric_text("1.54", 0), NumericDecoded::Number(1.54));
    }
}
