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

//! Client-side SQL parameter substitution for Netezza.
//!
//! IMPORTANT: Netezza's simple-query path does not expose server-side
//! bind/prepared parameters. Values are escaped and interpolated into the SQL
//! text before send. Port of the Node driver `protocol/sqlParameters.ts`.

use crate::types::value::NzValue;
use std::ops::Range;
use std::sync::Arc;

/// A named or positional client-side parameter.
///
/// Netezza's simple-query protocol has no bind message. This type therefore
/// describes how a value is rendered into SQL, while keeping the Rust API
/// explicit about the parameter mode instead of relying on a mutable
/// collection with ADO.NET semantics.
#[derive(Debug, Clone, PartialEq)]
pub struct NzParameter {
    pub name: Option<String>,
    pub value: NzValue,
    pub positional: bool,
}

impl NzParameter {
    pub fn named(name: &str, value: NzValue) -> Self {
        Self {
            name: Some(name.trim_start_matches([':', '@']).to_string()),
            value,
            positional: false,
        }
    }

    pub fn positional(value: NzValue) -> Self {
        Self {
            name: None,
            value,
            positional: true,
        }
    }
}

/// Escape a value as a SQL literal.
pub fn escape_literal(value: &NzValue) -> Result<String, String> {
    let mut result = String::new();
    write_nz_value_sql(value, &mut result)?;
    Ok(result)
}

/// Append one Netezza SQL literal without allocating a temporary string.
pub(crate) fn write_nz_value_sql(value: &NzValue, output: &mut String) -> Result<(), String> {
    use std::fmt::Write as _;

    match value {
        NzValue::Null => output.push_str("NULL"),
        NzValue::Bool(value) => output.push_str(if *value { "'t'" } else { "'f'" }),
        NzValue::Int2(value) => write!(output, "{value}").expect("writing to String cannot fail"),
        NzValue::Int4(value) => write!(output, "{value}").expect("writing to String cannot fail"),
        NzValue::Int8(value) => write!(output, "{value}").expect("writing to String cannot fail"),
        NzValue::Float4(value) => {
            let number = *value as f64;
            if !number.is_finite() {
                return Err(format!("Cannot bind non-finite number: {number}"));
            }
            write!(output, "{number}").expect("writing to String cannot fail");
        }
        NzValue::Float8(number) => {
            if !number.is_finite() {
                return Err(format!("Cannot bind non-finite number: {number}"));
            }
            write!(output, "{number}").expect("writing to String cannot fail");
        }
        NzValue::Numeric(value) => {
            if !valid_numeric_literal(value) {
                return Err("Invalid numeric parameter".into());
            }
            output.push_str(value);
        }
        NzValue::Decimal(value) => {
            write!(output, "{value}").expect("writing to String cannot fail")
        }
        NzValue::Text(value)
        | NzValue::Date(value)
        | NzValue::Time(value)
        | NzValue::Timetz(value)
        | NzValue::Timestamp(value)
        | NzValue::Interval(value) => write_text_literal(value, output)?,
        NzValue::Bytea(_) => {
            return Err(
                "Binary SQL parameters are unsupported; use an external-table reader".into(),
            )
        }
    }
    Ok(())
}

pub(crate) fn write_text_literal(value: &str, output: &mut String) -> Result<(), String> {
    if value.contains('\0') {
        return Err("SQL parameters cannot contain NUL".into());
    }
    if value.contains('\\') {
        output.push('(');
        let mut parts = value.split('\\').peekable();
        while let Some(part) = parts.next() {
            append_quoted_text(part, output);
            if parts.peek().is_some() {
                output.push_str(" || chr(92) || ");
            }
        }
        output.push(')');
    } else {
        append_quoted_text(value, output);
    }
    Ok(())
}

fn append_quoted_text(value: &str, output: &mut String) {
    output.push('\'');
    for ch in value.chars() {
        output.push(ch);
        if ch == '\'' {
            output.push('\'');
        }
    }
    output.push('\'');
}

fn valid_numeric_literal(s: &str) -> bool {
    let bytes = s.as_bytes();
    let mut i = usize::from(matches!(bytes.first(), Some(b'+' | b'-')));
    let mut digits = 0;
    while bytes.get(i).is_some_and(u8::is_ascii_digit) {
        i += 1;
        digits += 1;
    }
    if bytes.get(i) == Some(&b'.') {
        i += 1;
        while bytes.get(i).is_some_and(u8::is_ascii_digit) {
            i += 1;
            digits += 1;
        }
    }
    if digits == 0 {
        return false;
    }
    if matches!(bytes.get(i), Some(b'e' | b'E')) {
        i += 1;
        if matches!(bytes.get(i), Some(b'+' | b'-')) {
            i += 1;
        }
        let start = i;
        while bytes.get(i).is_some_and(u8::is_ascii_digit) {
            i += 1;
        }
        if i == start {
            return false;
        }
    }
    i == bytes.len()
}

fn block_comment_end(bytes: &[u8], start: usize) -> usize {
    let mut depth = 1usize;
    let mut index = start + 2;
    while index + 1 < bytes.len() {
        match &bytes[index..index + 2] {
            b"/*" => {
                depth += 1;
                index += 2;
            }
            b"*/" => {
                depth -= 1;
                index += 2;
                if depth == 0 {
                    return index;
                }
            }
            _ => index += 1,
        }
    }
    bytes.len()
}

/// Replace `$1`, `$2`, … placeholders with escaped literals.
/// Missing and unused parameter values are rejected before sending SQL.
///
/// The scan is a lexer: string literals, quoted identifiers, dollar-quoted
/// bodies and comments are preserved byte-for-byte.
pub fn substitute_parameters(sql: &str, params: &[NzValue]) -> Result<String, String> {
    SqlTemplate::parse(sql)?.render(params.len(), |index, output| {
        write_nz_value_sql(&params[index], output)
    })
}

#[derive(Debug)]
pub(crate) struct SqlTemplate {
    source: Arc<str>,
    placeholders: Box<[SqlPlaceholder]>,
}

#[derive(Debug, Clone)]
struct SqlPlaceholder {
    range: Range<usize>,
    index: usize,
}

impl SqlTemplate {
    pub(crate) fn parse(sql: &str) -> Result<Self, String> {
        if sql.contains('\0') {
            return Err("SQL cannot contain NUL".into());
        }
        let bytes = sql.as_bytes();
        let mut placeholders = Vec::new();
        let mut i = 0usize;
        let mut dollar_quote: Option<String> = None;
        while i < bytes.len() {
            if let Some(tag) = &dollar_quote {
                if sql[i..].starts_with(tag.as_str()) {
                    i += tag.len();
                    dollar_quote = None;
                } else {
                    i += sql[i..].chars().next().map(char::len_utf8).unwrap_or(1);
                }
                continue;
            }
            match bytes[i] {
                b'-' if i + 1 < bytes.len() && bytes[i + 1] == b'-' => {
                    i = sql[i..].find('\n').map_or(sql.len(), |end| i + end + 1);
                }
                b'/' if i + 1 < bytes.len() && bytes[i + 1] == b'*' => {
                    i = block_comment_end(bytes, i);
                }
                b'\'' | b'"' => {
                    let quote = bytes[i];
                    i += 1;
                    while i < bytes.len() {
                        if bytes[i] == quote {
                            if i + 1 < bytes.len() && bytes[i + 1] == quote {
                                i += 2;
                            } else {
                                i += 1;
                                break;
                            }
                        } else {
                            i += 1;
                        }
                    }
                }
                b'$' => {
                    let rest = &sql[i..];
                    if let Some(tag) = dollar_tag_str(rest) {
                        i += tag.len();
                        dollar_quote = Some(tag);
                    } else if let Some(number_len) = dollar_number(rest) {
                        let end = i + 1 + number_len;
                        let index = sql[i + 1..end].parse().unwrap_or(0);
                        placeholders.push(SqlPlaceholder {
                            range: i..end,
                            index,
                        });
                        i = end;
                    } else {
                        i += 1;
                    }
                }
                _ => i += sql[i..].chars().next().map(char::len_utf8).unwrap_or(1),
            }
        }
        Ok(Self {
            source: Arc::from(sql),
            placeholders: placeholders.into_boxed_slice(),
        })
    }

    pub(crate) fn retained_bytes(&self) -> usize {
        self.source.len().saturating_add(
            self.placeholders
                .len()
                .saturating_mul(std::mem::size_of::<SqlPlaceholder>()),
        )
    }

    pub(crate) fn source(&self) -> Arc<str> {
        self.source.clone()
    }

    pub(crate) fn render(
        &self,
        parameter_count: usize,
        mut write_parameter: impl FnMut(usize, &mut String) -> Result<(), String>,
    ) -> Result<String, String> {
        let mut result = String::with_capacity(self.source.len() + 16);
        let mut used_parameters = vec![false; parameter_count];
        let mut previous = 0;
        for placeholder in self.placeholders.iter() {
            result.push_str(&self.source[previous..placeholder.range.start]);
            if placeholder.index == 0 || placeholder.index > parameter_count {
                let token = &self.source[placeholder.range.clone()];
                return Err(format!("Missing value for SQL parameter '{token}'"));
            }
            let parameter_index = placeholder.index - 1;
            used_parameters[parameter_index] = true;
            write_parameter(parameter_index, &mut result)?;
            previous = placeholder.range.end;
        }
        result.push_str(&self.source[previous..]);
        if used_parameters.iter().any(|used| !used) {
            return Err("Unused SQL parameter value".into());
        }
        Ok(result)
    }
}

/// Substitute C#/ADO.NET-style named (`:name`, `@name`) or question-mark
/// positional parameters. The same lexer rules as [`substitute_parameters`]
/// apply, so placeholders inside literals, comments, quoted identifiers and
/// dollar-quoted bodies are not touched.
pub fn substitute_bound_parameters(sql: &str, params: &[NzParameter]) -> Result<String, String> {
    if sql.contains('\0') {
        return Err("SQL contains NUL".into());
    }
    if params.is_empty() {
        if let Some(placeholder) = first_placeholder(sql) {
            return Err(format!("Missing value for SQL parameter '{placeholder}'."));
        }
        return Ok(sql.to_string());
    }

    let has_named = params.iter().any(|p| !p.positional);
    let has_positional = params.iter().any(|p| p.positional);
    if has_named && has_positional {
        return Err("Named and positional parameters cannot be mixed in the same command.".into());
    }

    let mut result = String::with_capacity(sql.len() + 16);
    let mut used_names = std::collections::HashSet::<String>::new();
    let mut used_positional = std::collections::HashSet::<usize>::new();
    let mut positional_index = 0usize;
    let mut i = 0usize;
    let bytes = sql.as_bytes();
    let mut dollar_quote: Option<String> = None;

    while i < bytes.len() {
        if let Some(tag) = &dollar_quote {
            if sql[i..].starts_with(tag.as_str()) {
                result.push_str(tag);
                i += tag.len();
                dollar_quote = None;
            } else {
                let n = sql[i..].chars().next().map(char::len_utf8).unwrap_or(1);
                result.push_str(&sql[i..i + n]);
                i += n;
            }
            continue;
        }

        let ch = bytes[i] as char;
        if ch == '-' && i + 1 < bytes.len() && bytes[i + 1] == b'-' {
            let end = sql[i..].find('\n').map(|n| i + n + 1).unwrap_or(sql.len());
            result.push_str(&sql[i..end]);
            i = end;
            continue;
        }
        if ch == '/' && i + 1 < bytes.len() && bytes[i + 1] == b'*' {
            let end = block_comment_end(bytes, i);
            result.push_str(&sql[i..end]);
            i = end;
            continue;
        }
        if ch == '\'' || ch == '"' {
            let start = i;
            let quote = ch as u8;
            i += 1;
            while i < bytes.len() {
                if bytes[i] == quote {
                    if i + 1 < bytes.len() && bytes[i + 1] == quote {
                        i += 2;
                    } else {
                        i += 1;
                        break;
                    }
                } else {
                    i += 1;
                }
            }
            result.push_str(&sql[start..i.min(sql.len())]);
            continue;
        }
        if ch == '$' {
            if let Some(tag) = dollar_tag_str(&sql[i..]) {
                dollar_quote = Some(tag.clone());
                result.push_str(&tag);
                i += tag.len();
                continue;
            }
            // Also accept the Node/Rust `$1` form when bindings are positional.
            if has_positional {
                if let Some(n) = dollar_number(&sql[i..]) {
                    let token = &sql[i..i + n + 1];
                    let index = sql[i + 1..i + n + 1]
                        .parse::<usize>()
                        .map_err(|_| "Invalid positional parameter".to_string())?;
                    if index == 0 || index > params.len() {
                        return Err(format!("Missing value for SQL parameter '{token}'."));
                    }
                    result.push_str(&escape_literal(&params[index - 1].value)?);
                    used_positional.insert(index);
                    i += token.len();
                    continue;
                }
            }
        }
        if has_named && (ch == ':' || ch == '@') {
            if ch == ':' && i + 1 < bytes.len() && bytes[i + 1] == b':' {
                result.push_str("::");
                i += 2;
                continue;
            }
            if i + 1 < bytes.len() && is_identifier_start(bytes[i + 1]) {
                let start = i;
                i += 1;
                let name_start = i;
                while i < bytes.len() && is_identifier_part(bytes[i]) {
                    i += 1;
                }
                let lookup = &sql[name_start..i];
                let param = params.iter().find(|p| {
                    !p.positional
                        && p.name
                            .as_deref()
                            .is_some_and(|name| name.eq_ignore_ascii_case(lookup))
                });
                let Some(param) = param else {
                    return Err(format!(
                        "Missing value for SQL parameter '{}'.",
                        &sql[start..i]
                    ));
                };
                result.push_str(&escape_literal(&param.value)?);
                used_names.insert(lookup.to_ascii_lowercase());
                continue;
            }
        }
        if has_positional && ch == '?' {
            if positional_index >= params.len() {
                return Err("Missing value for SQL parameter '?'.".into());
            }
            result.push_str(&escape_literal(&params[positional_index].value)?);
            used_positional.insert(positional_index + 1);
            positional_index += 1;
            i += 1;
            continue;
        }
        let n = sql[i..].chars().next().map(char::len_utf8).unwrap_or(1);
        result.push_str(&sql[i..i + n]);
        i += n;
    }

    if has_named {
        for param in params {
            let Some(name) = param.name.as_deref() else {
                continue;
            };
            if !used_names.contains(&name.to_ascii_lowercase()) {
                return Err(format!("SQL parameter '{name}' was provided but not used."));
            }
        }
    } else if (1..=params.len()).any(|index| !used_positional.contains(&index)) {
        return Err("More positional parameter values were supplied than placeholders.".into());
    }
    Ok(result)
}

fn is_identifier_start(byte: u8) -> bool {
    byte.is_ascii_alphabetic() || byte == b'_'
}

fn is_identifier_part(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'$'
}

fn first_placeholder(sql: &str) -> Option<String> {
    let bytes = sql.as_bytes();
    let mut i = 0usize;
    let mut dollar_quote: Option<String> = None;

    while i < bytes.len() {
        if let Some(tag) = &dollar_quote {
            if sql[i..].starts_with(tag.as_str()) {
                i += tag.len();
                dollar_quote = None;
            } else {
                i += sql[i..].chars().next().map(char::len_utf8).unwrap_or(1);
            }
            continue;
        }

        let ch = bytes[i] as char;
        if ch == '-' && i + 1 < bytes.len() && bytes[i + 1] == b'-' {
            i = sql[i..].find('\n').map(|n| i + n + 1).unwrap_or(sql.len());
            continue;
        }
        if ch == '/' && i + 1 < bytes.len() && bytes[i + 1] == b'*' {
            i = block_comment_end(bytes, i);
            continue;
        }
        if ch == '\'' || ch == '"' {
            let quote = ch as u8;
            i += 1;
            while i < bytes.len() {
                if bytes[i] == quote {
                    if i + 1 < bytes.len() && bytes[i + 1] == quote {
                        i += 2;
                    } else {
                        i += 1;
                        break;
                    }
                } else {
                    i += 1;
                }
            }
            continue;
        }
        if ch == '$' {
            if let Some(tag) = dollar_tag_str(&sql[i..]) {
                i += tag.len();
                dollar_quote = Some(tag);
                continue;
            }
            if let Some(number_len) = dollar_number(&sql[i..]) {
                return Some(sql[i..i + number_len + 1].to_string());
            }
        }
        if ((ch == ':' && !(i + 1 < bytes.len() && bytes[i + 1] == b':')) || ch == '@')
            && i + 1 < bytes.len()
            && is_identifier_start(bytes[i + 1])
        {
            let mut end = i + 2;
            while end < bytes.len() && is_identifier_part(bytes[end]) {
                end += 1;
            }
            return Some(sql[i..end].to_string());
        }
        if ch == '?' {
            return Some("?".into());
        }
        i += sql[i..].chars().next().map(char::len_utf8).unwrap_or(1);
    }
    None
}

fn dollar_tag_str(rest: &str) -> Option<String> {
    let bytes = rest.as_bytes();
    if bytes.first() != Some(&b'$') {
        return None;
    }
    let mut i = 1;
    while i < bytes.len() {
        match bytes[i] {
            b'$' => return Some(rest[..=i].to_string()),
            b'A'..=b'Z' | b'a'..=b'z' | b'_' => i += 1,
            // A numeric suffix is a parameter, not a tag.
            b'0'..=b'9' => return None,
            _ => return None,
        }
    }
    None
}

fn dollar_number(rest: &str) -> Option<usize> {
    let bytes = rest.as_bytes();
    if bytes.first() != Some(&b'$') {
        return None;
    }
    let mut i = 1;
    while i < bytes.len() && bytes[i].is_ascii_digit() {
        i += 1;
    }
    if i == 1 {
        None
    } else {
        Some(i - 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn escapes_literals() {
        assert_eq!(escape_literal(&NzValue::Null).unwrap(), "NULL");
        assert_eq!(escape_literal(&NzValue::Bool(true)).unwrap(), "'t'");
        assert_eq!(
            escape_literal(&NzValue::Text("O'Brien".into())).unwrap(),
            "'O''Brien'"
        );
        assert_eq!(escape_literal(&NzValue::Int4(42)).unwrap(), "42");
        assert!(escape_literal(&NzValue::Bytea(vec![0xde, 0xad])).is_err());
    }

    #[test]
    fn substitutes_positional_params() {
        let sql = "SELECT * FROM t WHERE a = $1 AND b = $2 AND c = $1";
        let out =
            substitute_parameters(sql, &[NzValue::Int4(1), NzValue::Text("x;y".into())]).unwrap();
        assert_eq!(out, "SELECT * FROM t WHERE a = 1 AND b = 'x;y' AND c = 1");
    }

    #[test]
    fn preserves_strings_comments_and_dollar_quotes() {
        let sql = "SELECT '$1', \"col$2\", /* $3 */ $$body $4$$, $1::int";
        let out = substitute_parameters(sql, &[NzValue::Int4(9)]).unwrap();
        assert_eq!(out, "SELECT '$1', \"col$2\", /* $3 */ $$body $4$$, 9::int");
    }

    #[test]
    fn rejects_missing_and_unused_numbered_parameters() {
        assert!(substitute_parameters("SELECT $1, $2", &[NzValue::Int4(1)]).is_err());
        assert!(substitute_parameters("SELECT $1", &[]).is_err());
        assert!(substitute_parameters("SELECT 1", &[NzValue::Int4(1)]).is_err());
        assert!(substitute_parameters("SELECT $0", &[NzValue::Int4(1)]).is_err());
        assert!(substitute_parameters("SELECT 1\0", &[]).is_err());
    }

    #[test]
    fn validates_unused_large_parameter_sets_without_rescanning_placeholders() {
        use std::fmt::Write as _;

        const PARAMETER_COUNT: usize = 2_048;
        let mut sql = String::new();
        for index in 1..PARAMETER_COUNT {
            if index > 1 {
                sql.push(',');
            }
            write!(sql, "${index}").unwrap();
        }
        let template = SqlTemplate::parse(&sql).unwrap();

        let rendered = template.render(PARAMETER_COUNT, |_, output| {
            output.push('1');
            Ok(())
        });
        assert_eq!(rendered.unwrap_err(), "Unused SQL parameter value");
    }

    #[test]
    fn rejects_numeric_injection_and_escapes_temporal_values() {
        for value in ["0); SELECT 42; --", "NaN", "Infinity", "1e", "", "1 2"] {
            assert!(escape_literal(&NzValue::Numeric(value.into())).is_err());
        }
        for value in ["-1.234", "+.125", "1e-5", "0"] {
            assert_eq!(
                escape_literal(&NzValue::Numeric(value.into())).unwrap(),
                value
            );
        }
        let sql = escape_literal(&NzValue::Timestamp("2026-01-01'; SELECT 42; --".into())).unwrap();
        assert_eq!(sql, "'2026-01-01''; SELECT 42; --'");
        assert!(escape_literal(&NzValue::Text("x\0y".into())).is_err());
    }

    #[test]
    fn substitutes_named_and_question_parameters() {
        let out = substitute_bound_parameters(
            "SELECT :name, @name, :age::INTEGER",
            &[
                NzParameter::named(":name", NzValue::Text("O'Brien".into())),
                NzParameter::named("age", NzValue::Int4(42)),
            ],
        )
        .unwrap();
        assert_eq!(out, "SELECT 'O''Brien', 'O''Brien', 42::INTEGER");

        let out = substitute_bound_parameters(
            "SELECT ?, ?",
            &[
                NzParameter::positional(NzValue::Int4(1)),
                NzParameter::positional(NzValue::Bool(true)),
            ],
        )
        .unwrap();
        assert_eq!(out, "SELECT 1, 't'");
    }

    #[test]
    fn bound_parameter_lexer_preserves_non_sql_regions() {
        let out = substitute_bound_parameters(
            "SELECT ':x', \"@x\", /* ? */ $$:x ?$$, :x",
            &[NzParameter::named("x", NzValue::Int4(7))],
        )
        .unwrap();
        assert_eq!(out, "SELECT ':x', \"@x\", /* ? */ $$:x ?$$, 7");
    }

    #[test]
    fn bound_parameters_reject_mixed_missing_and_unused() {
        assert!(substitute_bound_parameters(
            "SELECT :x",
            &[NzParameter::positional(NzValue::Int4(1))]
        )
        .is_err());
        assert!(substitute_bound_parameters(
            "SELECT :x",
            &[
                NzParameter::named("x", NzValue::Int4(1)),
                NzParameter::named("y", NzValue::Int4(2))
            ]
        )
        .is_err());
        assert!(substitute_bound_parameters(
            "SELECT :missing",
            &[NzParameter::named("x", NzValue::Int4(1))]
        )
        .is_err());

        assert!(substitute_bound_parameters(
            "SELECT $1",
            &[
                NzParameter::positional(NzValue::Int4(1)),
                NzParameter::positional(NzValue::Int4(2)),
            ]
        )
        .is_err());
    }

    #[test]
    fn empty_bindings_only_reject_real_placeholders() {
        assert_eq!(
            substitute_bound_parameters("SELECT ':x', /* ? */ $$@y$$", &[]).unwrap(),
            "SELECT ':x', /* ? */ $$@y$$"
        );
        assert!(substitute_bound_parameters("SELECT ':x', :x", &[]).is_err());
        assert!(substitute_bound_parameters("SELECT $1", &[]).is_err());
    }
    #[test]
    fn placeholders_follow_netezza_quotes_and_nested_comments() {
        assert_eq!(
            substitute_parameters(
                "SELECT '\\', $1 /* outer /* inner */ $2 */",
                &[NzValue::Int4(7)]
            )
            .unwrap(),
            "SELECT '\\', 7 /* outer /* inner */ $2 */"
        );
        assert_eq!(
            substitute_bound_parameters(
                "SELECT '\\', ? /* outer /* ? */ ? */",
                &[NzParameter::positional(NzValue::Int4(7))]
            )
            .unwrap(),
            "SELECT '\\', 7 /* outer /* ? */ ? */"
        );
    }
}
