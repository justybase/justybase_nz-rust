//! Deterministic property tests (fixed seeds, no extra dependencies):
//!
//! * a rendered SQL text literal evaluates back to the original value and can
//!   never end the literal or the statement early;
//! * parameter values are never re-scanned for placeholders;
//! * numeric values survive render → parse;
//! * (frame splitting at any offset and hostile lengths are covered by
//!   `wire_fragmentation.rs` and `allocation_bounds.rs`).

mod support;

use nz_rust::messages::split_statements;
use nz_rust::params::{escape_literal, substitute_parameters};
use nz_rust::types::text::try_parse_text_value;
use nz_rust::{Decimal, NzValue};
use support::next_random;

const ALPHABET: &[&str] = &[
    "a", "Z", "0", " ", "'", "''", "\\", "\\\\", ";", "--", "/*", "*/", "$", "$1", "$$", "\"",
    "\n", "\t", "ż", "日", "😀", ":x", "@y", "?", "chr(92)", "||", "E'", "U&'",
];

fn random_text(seed: &mut u64) -> String {
    let length = (next_random(seed) % 24) as usize;
    (0..length)
        .map(|_| ALPHABET[(next_random(seed) % ALPHABET.len() as u64) as usize])
        .collect()
}

/// Evaluate a rendered text literal: `'…'` with doubled quotes, optionally
/// parenthesised pieces joined by ` || chr(92) || ` (a backslash). Returns the
/// value and the number of bytes consumed.
fn eval_literal(rendered: &str) -> Option<(String, usize)> {
    fn quoted(input: &str) -> Option<(String, usize)> {
        let bytes = input.as_bytes();
        if bytes.first() != Some(&b'\'') {
            return None;
        }
        let mut out = String::new();
        let mut index = 1;
        loop {
            let ch = input[index..].chars().next()?;
            index += ch.len_utf8();
            if ch == '\'' {
                if input[index..].starts_with('\'') {
                    out.push('\'');
                    index += 1;
                } else {
                    return Some((out, index));
                }
            } else {
                out.push(ch);
            }
        }
    }
    if let Some(rest) = rendered.strip_prefix('(') {
        let mut value = String::new();
        let mut offset = 1;
        let mut remaining = rest;
        loop {
            let (part, used) = quoted(remaining)?;
            value.push_str(&part);
            offset += used;
            remaining = &remaining[used..];
            if let Some(next) = remaining.strip_prefix(" || chr(92) || ") {
                value.push('\\');
                offset += " || chr(92) || ".len();
                remaining = next;
            } else if remaining.starts_with(')') {
                return Some((value, offset + 1));
            } else {
                return None;
            }
        }
    }
    quoted(rendered)
}

#[test]
fn text_literals_round_trip_and_never_escape_the_literal() {
    let mut seed = 0x5eed_1234_5678;
    for case in 0..4_000 {
        let value = random_text(&mut seed);
        let rendered = escape_literal(&NzValue::Text(value.clone())).unwrap();
        let (evaluated, used) = eval_literal(&rendered)
            .unwrap_or_else(|| panic!("case {case}: {value:?} -> {rendered}"));
        assert_eq!(evaluated, value, "case {case}: {rendered}");
        assert_eq!(
            used,
            rendered.len(),
            "case {case}: trailing bytes after literal"
        );

        // Embedded in statements, the literal never changes their shape.
        let sql = format!("SELECT {rendered} AS A; SELECT 2");
        assert_eq!(split_statements(&sql).len(), 2, "case {case}: {sql}");
    }
}

#[test]
fn parameter_values_are_never_rescanned_for_placeholders() {
    let mut seed = 0xfeed_beef;
    for case in 0..2_000 {
        let (a, b) = (random_text(&mut seed), random_text(&mut seed));
        let sql = substitute_parameters(
            "SELECT $1, $2 /* $1 */ -- $2\n, '$1'",
            &[NzValue::Text(a.clone()), NzValue::Text(b.clone())],
        )
        .unwrap();
        let rest = sql.strip_prefix("SELECT ").unwrap();
        let (first, used) = eval_literal(rest).unwrap_or_else(|| panic!("case {case}: {sql}"));
        let rest = rest[used..].strip_prefix(", ").unwrap();
        let (second, used) = eval_literal(rest).unwrap_or_else(|| panic!("case {case}: {sql}"));
        assert_eq!((first, second), (a, b), "case {case}: {sql}");
        assert_eq!(
            &rest[used..],
            " /* $1 */ -- $2\n, '$1'",
            "case {case}: comments and literals must be untouched"
        );
    }
}

#[test]
fn numeric_values_survive_render_then_parse() {
    let mut seed = 0x1234_abcd_9876;
    for case in 0..4_000 {
        let word = next_random(&mut seed);
        let int = word as i64;
        let rendered = escape_literal(&NzValue::Int8(int)).unwrap();
        assert_eq!(rendered.parse::<i64>().unwrap(), int);
        assert_eq!(
            try_parse_text_value(&rendered, 20, -1).unwrap(),
            NzValue::Int8(int)
        );
        let small = word as i32;
        assert_eq!(
            try_parse_text_value(&escape_literal(&NzValue::Int4(small)).unwrap(), 23, -1).unwrap(),
            NzValue::Int4(small)
        );

        let float = f64::from_bits(next_random(&mut seed));
        if float.is_finite() {
            let rendered = escape_literal(&NzValue::Float8(float)).unwrap();
            assert_eq!(
                rendered.parse::<f64>().unwrap().to_bits(),
                float.to_bits(),
                "case {case}: {rendered}"
            );
        } else {
            assert!(escape_literal(&NzValue::Float8(float)).is_err());
        }

        let mantissa = (next_random(&mut seed) as i128) << 32 | next_random(&mut seed) as i128;
        let scale = (next_random(&mut seed) % 29) as u32;
        if let Ok(decimal) = Decimal::try_from_i128_with_scale(mantissa >> 40, scale) {
            let rendered = escape_literal(&NzValue::Decimal(decimal)).unwrap();
            let parsed = Decimal::from_str_exact(&rendered).unwrap();
            assert_eq!(parsed, decimal, "case {case}");
            assert_eq!(parsed.scale(), decimal.scale(), "case {case}: scale kept");
        }
    }
}

#[test]
fn hostile_values_are_rejected_not_rendered() {
    assert!(escape_literal(&NzValue::Text("a\0b".into())).is_err());
    assert!(escape_literal(&NzValue::Numeric("1; DROP TABLE x".into())).is_err());
    assert!(escape_literal(&NzValue::Numeric("1e".into())).is_err());
    assert!(escape_literal(&NzValue::Float8(f64::NAN)).is_err());
    assert!(escape_literal(&NzValue::Bytea(vec![1])).is_err());
    assert!(substitute_parameters("SELECT $2", &[NzValue::Null]).is_err());
    assert!(substitute_parameters("SELECT 1", &[NzValue::Null]).is_err());
}
