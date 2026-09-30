//! Exact NUMERIC(38) values with an allocation-free binary decoding path.
use crate::{FromSql, FromSqlRaw, NzError, NzResult, NzValue, RawValue, ToSql};
use std::{fmt, str::FromStr};

/// A signed decimal coefficient and scale, retaining trailing zeroes.
/// The range matches Netezza NUMERIC: at most 38 digits and scale at most 38.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NzNumeric {
    coefficient: i128,
    scale: u32,
}
impl NzNumeric {
    pub fn new(coefficient: i128, scale: u32) -> NzResult<Self> {
        if coefficient.unsigned_abs() >= 10u128.pow(38) || scale > 38 {
            return Err(NzError::Config(
                "NUMERIC exceeds 38 digits or scale 38".into(),
            ));
        }
        Ok(Self { coefficient, scale })
    }
    pub fn coefficient(self) -> i128 {
        self.coefficient
    }
    pub fn scale(self) -> u32 {
        self.scale
    }
    /// Checked conversion to the narrower 96-bit rust_decimal representation.
    pub fn to_decimal(self) -> NzResult<rust_decimal::Decimal> {
        rust_decimal::Decimal::try_from_i128_with_scale(self.coefficient, self.scale)
            .map_err(|_| NzError::Config("NUMERIC does not fit Decimal".into()))
    }
}
impl FromStr for NzNumeric {
    type Err = NzError;
    fn from_str(input: &str) -> NzResult<Self> {
        let (negative, digits) = if let Some(rest) = input.strip_prefix('-') {
            (true, rest)
        } else {
            (false, input.strip_prefix('+').unwrap_or(input))
        };
        let mut coefficient = 0i128;
        let mut scale = 0;
        let mut point = false;
        let mut count = 0;
        for byte in digits.bytes() {
            if byte == b'.' && !point {
                point = true;
                continue;
            }
            if !byte.is_ascii_digit() {
                return Err(NzError::Config("invalid NUMERIC decimal".into()));
            }
            coefficient = coefficient
                .checked_mul(10)
                .and_then(|v| v.checked_add(i128::from(byte - b'0')))
                .ok_or_else(|| NzError::Config("NUMERIC coefficient overflow".into()))?;
            count += 1;
            if point {
                scale += 1;
            }
        }
        if count == 0 {
            return Err(NzError::Config("empty NUMERIC decimal".into()));
        }
        Self::new(if negative { -coefficient } else { coefficient }, scale)
    }
}
impl fmt::Display for NzNumeric {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.coefficient < 0 {
            f.write_str("-")?;
        }
        let magnitude = self.coefficient.unsigned_abs();
        if self.scale == 0 {
            return write!(f, "{magnitude}");
        }
        let divisor = 10u128.pow(self.scale);
        write!(
            f,
            "{}.{:0width$}",
            magnitude / divisor,
            magnitude % divisor,
            width = self.scale as usize
        )
    }
}
impl FromSql for NzNumeric {
    fn from_raw(value: RawValue<'_>) -> NzResult<Self> {
        if value.is_null() {
            return Err(NzError::Config("cannot decode NULL as NUMERIC".into()));
        }
        if let Some((desc, row, offset, index)) = value.dbos {
            if desc.field_type[index] != crate::messages::nz_type::NZ_TYPE_NUMERIC {
                return Err(NzError::Config("field is not NUMERIC".into()));
            }
            let length = usize::try_from(desc.field_true_size[index])
                .map_err(|_| NzError::Protocol("invalid NUMERIC size".into()))?;
            let bytes = offset
                .checked_add(length)
                .and_then(|end| row.get(offset..end))
                .ok_or_else(|| NzError::Protocol("truncated NUMERIC field".into()))?;
            if length == 0 || length % 4 != 0 || length > 20 {
                return Err(NzError::Protocol("invalid NUMERIC word count".into()));
            }
            let mut coefficient = i128::from(i32::from_le_bytes(bytes[..4].try_into().unwrap()));
            for word in bytes[4..].chunks_exact(4) {
                coefficient = coefficient
                    .checked_mul(1i128 << 32)
                    .and_then(|v| {
                        v.checked_add(i128::from(u32::from_le_bytes(word.try_into().unwrap())))
                    })
                    .ok_or_else(|| NzError::Protocol("NUMERIC coefficient overflow".into()))?;
            }
            let scale = u32::try_from(super::numeric::field_scale(desc.field_size[index]))
                .map_err(|_| NzError::Protocol("invalid NUMERIC scale".into()))?;
            return Self::new(coefficient, scale);
        }
        if value.format() == 0 && value.type_oid() == 1700 {
            if let Some(bytes) = value.as_bytes() {
                return std::str::from_utf8(bytes)
                    .map_err(|_| NzError::Protocol("invalid NUMERIC text".into()))?
                    .parse();
            }
        }
        Self::from_sql(&value.to_nz_value()?)
    }
    fn from_sql(value: &NzValue) -> NzResult<Self> {
        match value {
            NzValue::Numeric(text) => text.parse(),
            NzValue::Decimal(value) => Self::new(value.mantissa(), value.scale()),
            _ => Err(NzError::Config("value is not exact NUMERIC".into())),
        }
    }
}
impl<'a> FromSqlRaw<'a> for NzNumeric {
    fn from_sql_raw(value: RawValue<'a>) -> NzResult<Self> {
        Self::from_raw(value)
    }
}
impl ToSql for NzNumeric {
    fn to_nz_value(&self) -> NzValue {
        NzValue::Numeric(self.to_string())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn preserves_full_precision_and_scale() {
        for text in [
            "99999999999999999999999999999999999999",
            "-0.00000000000000000000000000000000000001",
            "3.1400",
            "-99999999999999999999999999999999999999",
        ] {
            assert_eq!(text.parse::<NzNumeric>().unwrap().to_string(), text);
        }
        for text in [
            "",
            ".",
            "1e2",
            "1;SELECT",
            "100000000000000000000000000000000000000",
            "0.000000000000000000000000000000000000001",
        ] {
            assert!(text.parse::<NzNumeric>().is_err());
        }
    }
}
