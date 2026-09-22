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
    Ok(match value {
        NzValue::Null => "NULL".into(),
        NzValue::Bool(b) => {
            if *b {
                "'t'".into()
            } else {
                "'f'".into()
            }
        }
        NzValue::Int2(v) => v.to_string(),
        NzValue::Int4(v) => v.to_string(),
        NzValue::Int8(v) => v.to_string(),
        NzValue::Float4(_) | NzValue::Float8(_) => {
            let s = value.to_display_string();
            let n: f64 = s
                .parse()
                .map_err(|_| format!("Cannot bind non-finite number: {s}"))?;
            if !n.is_finite() {
                return Err(format!("Cannot bind non-finite number: {n}"));
            }
            s
        }
        NzValue::Numeric(s) => s.clone(),
        NzValue::Decimal(value) => value.to_string(),
        NzValue::Text(s) => format!("'{}'", s.replace('\'', "''")),
        NzValue::Date(s) => format!("'{s}'"),
        NzValue::Time(s) => format!("'{s}'"),
        NzValue::Timetz(s) => format!("'{s}'"),
        NzValue::Timestamp(s) => format!("'{s}'"),
        NzValue::Interval(s) => format!("'{s}'"),
        NzValue::Bytea(b) => format!("E'\\\\x{}'", hex_encode(b)),
    })
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push_str(&format!("{b:02x}"));
    }
    out
}

/// Replace `$1`, `$2`, … placeholders with escaped literals.
/// Unmatched placeholders are left unchanged.
///
/// The scan is a lexer: string literals, quoted identifiers, dollar-quoted
/// bodies and comments are preserved byte-for-byte.
pub fn substitute_parameters(sql: &str, params: &[NzValue]) -> Result<String, String> {
    if params.is_empty() {
        return Ok(sql.to_string());
    }

    let bytes = sql.as_bytes();
    let mut result = String::with_capacity(sql.len() + 16);
    let mut i = 0usize;
    let mut dollar_quote: Option<String> = None;

    while i < bytes.len() {
        if let Some(tag) = &dollar_quote {
            if sql[i..].starts_with(tag.as_str()) {
                result.push_str(tag);
                i += tag.len();
                dollar_quote = None;
            } else {
                let ch_len = sql[i..].chars().next().map(|c| c.len_utf8()).unwrap_or(1);
                result.push_str(&sql[i..i + ch_len]);
                i += ch_len;
            }
            continue;
        }

        let ch = bytes[i] as char;

        // SQL line comments do not contain parameters.
        if ch == '-' && i + 1 < bytes.len() && bytes[i + 1] == b'-' {
            match sql[i..].find('\n') {
                Some(line_end) => {
                    result.push_str(&sql[i..=i + line_end]);
                    i += line_end + 1;
                }
                None => {
                    result.push_str(&sql[i..]);
                    break;
                }
            }
            continue;
        }

        // SQL block comments may contain arbitrary '$1'-like text.
        if ch == '/' && i + 1 < bytes.len() && bytes[i + 1] == b'*' {
            match sql[i..].find("*/") {
                Some(end) => {
                    result.push_str(&sql[i..i + end + 2]);
                    i += end + 2;
                }
                None => {
                    result.push_str(&sql[i..]);
                    break;
                }
            }
            continue;
        }

        if ch == '\'' {
            let start = i;
            i += 1;
            while i < bytes.len() {
                if bytes[i] == b'\\' && i + 1 < bytes.len() {
                    i += 2;
                    continue;
                }
                if bytes[i] != b'\'' {
                    i += 1;
                    continue;
                }
                if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                    i += 2;
                    continue;
                }
                i += 1;
                break;
            }
            result.push_str(&sql[start..i.min(sql.len())]);
            continue;
        }

        if ch == '"' {
            let start = i;
            i += 1;
            while i < bytes.len() {
                if bytes[i] == b'"' && i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                    i += 2;
                    continue;
                }
                if bytes[i] == b'"' {
                    i += 1;
                    break;
                }
                i += 1;
            }
            result.push_str(&sql[start..i.min(sql.len())]);
            continue;
        }

        // Preserve dollar-quoted bodies; a numeric suffix is a parameter, not a tag.
        if ch == '$' {
            let rest = &sql[i..];
            if let Some(tag) = dollar_tag_str(rest) {
                dollar_quote = Some(tag.clone());
                result.push_str(&tag);
                i += tag.len();
                continue;
            }
            if let Some(num_len) = dollar_number(rest) {
                let match_str = &rest[..1 + num_len];
                let idx: usize = match_str[1..].parse().unwrap_or(0);
                if idx >= 1 && idx <= params.len() {
                    result.push_str(&escape_literal(&params[idx - 1])?);
                } else {
                    result.push_str(match_str);
                }
                i += match_str.len();
                continue;
            }
        }

        let ch_len = sql[i..].chars().next().map(|c| c.len_utf8()).unwrap_or(1);
        result.push_str(&sql[i..i + ch_len]);
        i += ch_len;
    }

    Ok(result)
}

/// Substitute C#/ADO.NET-style named (`:name`, `@name`) or question-mark
/// positional parameters. The same lexer rules as [`substitute_parameters`]
/// apply, so placeholders inside literals, comments, quoted identifiers and
/// dollar-quoted bodies are not touched.
pub fn substitute_bound_parameters(sql: &str, params: &[NzParameter]) -> Result<String, String> {
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
            let end = sql[i..].find("*/").map(|n| i + n + 2).unwrap_or(sql.len());
            result.push_str(&sql[i..end]);
            i = end;
            continue;
        }
        if ch == '\'' || ch == '"' {
            let start = i;
            let quote = ch as u8;
            i += 1;
            while i < bytes.len() {
                if quote == b'\'' && bytes[i] == b'\\' && i + 1 < bytes.len() {
                    i += 2;
                } else if bytes[i] == quote {
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
            i = sql[i..].find("*/").map(|n| i + n + 2).unwrap_or(sql.len());
            continue;
        }
        if ch == '\'' || ch == '"' {
            let quote = ch as u8;
            i += 1;
            while i < bytes.len() {
                if quote == b'\'' && bytes[i] == b'\\' && i + 1 < bytes.len() {
                    i += 2;
                } else if bytes[i] == quote {
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
        assert_eq!(
            escape_literal(&NzValue::Bytea(vec![0xde, 0xad])).unwrap(),
            "E'\\\\xdead'"
        );
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
    fn unmatched_placeholders_unchanged() {
        let out = substitute_parameters("SELECT $1, $2", &[NzValue::Int4(1)]).unwrap();
        assert_eq!(out, "SELECT 1, $2");
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
}
