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

//! Backend message codes and small protocol helpers.
//!
//! Port of the Node driver `protocol/constants.ts` plus the statement /
//! CommandComplete helpers from `NzConnection.ts`.

/// Backend message type bytes. Netezza-specific codes documented inline.
pub mod code {
    pub const AUTHENTICATION_REQUEST: u8 = b'R'; // 82
    pub const ERROR_RESPONSE: u8 = b'E'; // 69
    pub const NOTICE_RESPONSE: u8 = b'N'; // 78
    pub const BACKEND_KEY_DATA: u8 = b'K'; // 75
    pub const READY_FOR_QUERY: u8 = b'Z'; // 90
    pub const ROW_DESCRIPTION: u8 = b'T'; // 84
    /// Binary row description for standard (table) queries.
    pub const ROW_DESCRIPTION_STANDARD: u8 = b'X'; // 88
    pub const DATA_ROW: u8 = b'D'; // 68
    pub const COMMAND_COMPLETE: u8 = b'C'; // 67
    pub const EMPTY_QUERY_RESPONSE: u8 = b'I'; // 73
    /// Binary row data for standard tables.
    pub const ROW_STANDARD: u8 = b'Y'; // 89
    pub const COPY_IN_RESPONSE: u8 = b'G'; // 71
    pub const COPY_OUT_RESPONSE: u8 = b'H'; // 72
    pub const COPY_DONE: u8 = b'c'; // 99
    pub const COPY_DATA: u8 = b'd'; // 100
    /// Netezza-specific: alternate ready/idle marker (ASCII 'L').
    pub const READY_FOR_QUERY_ALT: u8 = 0x4c;
    /// Netezza-specific skippable control byte (ASCII '0').
    pub const CONTROL_ZERO: u8 = 0x30;
    /// Netezza-specific skippable control byte (ASCII 'A').
    pub const CONTROL_A: u8 = 0x41;
    /// Netezza-specific payload prefix (ASCII 'P') with a following length+body.
    pub const BACKEND_PAYLOAD_P: u8 = 0x50;
    /// External-table export start / data / import / cancel / log messages.
    pub const EXT_EXPORT_START: u8 = b'u';
    pub const EXT_EXPORT_DATA: u8 = b'U';
    pub const EXT_IMPORT: u8 = b'l';
    pub const EXT_CANCEL: u8 = b'x';
    pub const EXT_LOG: u8 = b'e';
}

/// Netezza column type codes from the binary tuple descriptor.
/// Based on C# `NzConnection.cs`.
pub mod nz_type {
    pub const NZ_TYPE_REC_ADDR: i32 = 1;
    pub const NZ_TYPE_DOUBLE: i32 = 2;
    pub const NZ_TYPE_INT: i32 = 3;
    pub const NZ_TYPE_FLOAT: i32 = 4;
    pub const NZ_TYPE_MONEY: i32 = 5;
    pub const NZ_TYPE_DATE: i32 = 6;
    pub const NZ_TYPE_NUMERIC: i32 = 7;
    pub const NZ_TYPE_TIME: i32 = 8;
    pub const NZ_TYPE_TIMESTAMP: i32 = 9;
    pub const NZ_TYPE_INTERVAL: i32 = 10;
    pub const NZ_TYPE_TIME_TZ: i32 = 11;
    pub const NZ_TYPE_BOOL: i32 = 12;
    pub const NZ_TYPE_INT1: i32 = 13;
    pub const NZ_TYPE_BINARY: i32 = 14;
    pub const NZ_TYPE_CHAR: i32 = 15;
    pub const NZ_TYPE_VARCHAR: i32 = 16;
    pub const NZ_DEPR_TEXT: i32 = 17;
    pub const NZ_TYPE_UNKNOWN: i32 = 18;
    pub const NZ_TYPE_INT2: i32 = 19;
    pub const NZ_TYPE_INT8: i32 = 20;
    pub const NZ_TYPE_VAR_FIXED_CHAR: i32 = 21;
    pub const NZ_TYPE_GEOMETRY: i32 = 22;
    pub const NZ_TYPE_VAR_BINARY: i32 = 23;
    pub const NZ_DEPR_BLOB: i32 = 24;
    pub const NZ_TYPE_NCHAR: i32 = 25;
    pub const NZ_TYPE_NVARCHAR: i32 = 26;
    pub const NZ_DEPR_NTEXT: i32 = 27;
    pub const NZ_TYPE_JSON: i32 = 30;
    pub const NZ_TYPE_JSONB: i32 = 31;
    pub const NZ_TYPE_JSONPATH: i32 = 32;
    pub const NZ_TYPE_LAST_ENTRY: i32 = 33;
    /// Abstime compatibility fix (nzpy issue #61).
    pub const NZ_TYPE_INTVS_ABS_TIME_FIX: i32 = 39;
}

/// PostgreSQL type OIDs as observed in text RowDescription frames.
pub mod oid {
    pub const BOOL: i32 = 16;
    pub const INT8: i32 = 20;
    pub const INT2: i32 = 21;
    pub const INT4: i32 = 23;
    pub const TEXT: i32 = 25;
    pub const OID: i32 = 26;
    pub const FLOAT4: i32 = 700;
    pub const FLOAT8: i32 = 701;
    pub const ABSTIME: i32 = 702;
    pub const VARCHAR: i32 = 1043;
    pub const DATE: i32 = 1082;
    pub const TIME: i32 = 1083;
    pub const TIMESTAMP: i32 = 1114;
    pub const TIMESTAMPTZ: i32 = 1184;
    pub const INTERVAL: i32 = 1186;
    pub const TIMETZ: i32 = 1266;
    pub const NUMERIC: i32 = 1700;
    pub const BYTEINT_ALIAS: i32 = 2500;
}

/// Parse rows affected from a Netezza CommandComplete message.
///
/// Patterns: "INSERT 0 1", "UPDATE 5", "DELETE 3", "SELECT 10",
/// "CREATE TABLE" (no count → -1). Same logic as the C# driver: split by
/// whitespace and take the last value for counting commands.
pub fn parse_command_complete_rows(command_text: &str) -> i64 {
    let binding = command_text.replace('\0', "");
    let clean = binding.trim();
    let mut values = clean.split_whitespace();
    let command = values.next().unwrap_or("");
    if matches!(
        command.to_ascii_uppercase().as_str(),
        "INSERT" | "UPDATE" | "DELETE" | "SELECT"
    ) {
        if let Some(last) = values.last() {
            if let Ok(parsed) = last.parse::<i64>() {
                return parsed;
            }
        }
    }
    -1
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionState {
    Unchanged,
    Opened,
    Closed,
}

/// Parse `sql` for an explicit-transaction boundary.
///
/// Only a transaction-control keyword at the start of a statement counts, so
/// `CASE ... END` (or `END` inside a procedure body) cannot be mistaken for a
/// transaction boundary. When a text holds several statements the last one
/// wins. Port of `_parseTransactionState` / `splitStatements`.
pub fn parse_transaction_state(sql: &str) -> (TransactionState, bool) {
    let mut state: Option<bool> = None;
    let mut had_start = false;
    for statement in split_statements(sql) {
        let text = statement.trim();
        if text.is_empty() {
            continue;
        }
        if is_transaction_start(text) {
            state = Some(true);
            had_start = true;
        } else if is_transaction_end(text) {
            state = Some(false);
        }
    }
    match state {
        Some(true) => (TransactionState::Opened, had_start),
        Some(false) => (TransactionState::Closed, had_start),
        None => (TransactionState::Unchanged, had_start),
    }
}

fn first_word_upper(text: &str) -> &str {
    let end = text
        .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
        .unwrap_or(text.len());
    &text[..end]
}

fn is_transaction_start(text: &str) -> bool {
    let w = first_word_upper(text);
    w.eq_ignore_ascii_case("BEGIN") || w.eq_ignore_ascii_case("START")
}

fn is_transaction_end(text: &str) -> bool {
    let w = first_word_upper(text);
    w.eq_ignore_ascii_case("COMMIT")
        || w.eq_ignore_ascii_case("ROLLBACK")
        || w.eq_ignore_ascii_case("END")
        || w.eq_ignore_ascii_case("ABORT")
}

/// Split SQL text on statement boundaries, blanking out string literals,
/// quoted identifiers, dollar-quoted bodies, `AS BEGIN_PROC ... END_PROC`
/// bodies and comments. Port of `splitStatements`.
pub fn split_statements(sql: &str) -> Vec<String> {
    let mut statements = Vec::new();
    let mut current = String::new();
    let bytes = sql.as_bytes();
    let mut i = 0usize;

    while i < bytes.len() {
        let ch = bytes[i] as char;
        let rest = &sql[i..];

        if ch == '\'' || ch == '"' {
            let start = i;
            let quote = ch;
            i += 1;
            while i < bytes.len() {
                if quote == '\'' && bytes[i] == b'\\' && i + 1 < bytes.len() {
                    i += 2;
                    continue;
                }
                if bytes[i] as char != quote {
                    i += 1;
                    continue;
                }
                if i + 1 < bytes.len() && bytes[i + 1] as char == quote {
                    i += 2; // doubled quote ('' or "")
                    continue;
                }
                i += 1;
                break;
            }
            current.push_str(&sql[start..i.min(sql.len())]);
            continue;
        }

        // Netezza procedural body without dollar quoting: AS BEGIN_PROC ... END_PROC
        if (ch == 'b' || ch == 'B')
            && (i == 0
                || !(bytes[i - 1].is_ascii_alphanumeric()
                    || bytes[i - 1] == b'_'
                    || bytes[i - 1] == b'$'))
            && rest.len() >= 10
            && rest[..10].eq_ignore_ascii_case("begin_proc")
        {
            let after = &rest[10..];
            let end_off = find_word_ci(after, "end_proc");
            match end_off {
                Some(off) => i += 10 + off + 8,
                None => i = bytes.len(),
            }
            current.push(' ');
            continue;
        }

        // Dollar-quoted body ($$...$$ / $tag$...$tag$); numeric suffix is a parameter.
        if ch == '$' {
            if let Some(tag) = dollar_tag(rest) {
                let end = rest[tag.len()..]
                    .find(&tag)
                    .map(|p| p + tag.len() + tag.len());
                i = match end {
                    Some(e) => i + e,
                    None => bytes.len(),
                };
                current.push(' ');
                continue;
            }
        }

        if ch == '-' && i + 1 < bytes.len() && bytes[i + 1] == b'-' {
            match sql[i..].find('\n') {
                Some(line_end) => i += line_end,
                None => i = bytes.len(),
            }
            current.push(' ');
            continue;
        }

        if ch == '/' && i + 1 < bytes.len() && bytes[i + 1] == b'*' {
            match rest[2..].find("*/") {
                Some(comment_end) => i += comment_end + 4,
                None => i = bytes.len(),
            }
            current.push(' ');
            continue;
        }

        if ch == ';' {
            statements.push(std::mem::take(&mut current));
            i += 1;
            continue;
        }

        // Push the full UTF-8 character (multi-byte safe).
        let ch_len = sql[i..].chars().next().map(|c| c.len_utf8()).unwrap_or(1);
        current.push_str(&sql[i..i + ch_len]);
        i += ch_len;
    }

    statements.push(current);
    statements
}

fn dollar_tag(rest: &str) -> Option<String> {
    let bytes = rest.as_bytes();
    if bytes.first() != Some(&b'$') {
        return None;
    }
    let mut i = 1;
    while i < bytes.len() {
        match bytes[i] {
            b'$' => return Some(rest[..=i].to_string()),
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'_' => i += 1,
            _ => return None,
        }
    }
    None
}

fn find_word_ci(haystack: &str, word: &str) -> Option<usize> {
    let hay = haystack.as_bytes();
    let w = word.as_bytes();
    if w.is_empty() || hay.len() < w.len() {
        return None;
    }
    for start in 0..=(hay.len() - w.len()) {
        let before_ok = start == 0
            || !(hay[start - 1].is_ascii_alphanumeric()
                || hay[start - 1] == b'_'
                || hay[start - 1] == b'$');
        let after_ok = start + w.len() == hay.len()
            || !(hay[start + w.len()].is_ascii_alphanumeric()
                || hay[start + w.len()] == b'_'
                || hay[start + w.len()] == b'$');
        if before_ok && after_ok && hay[start..start + w.len()].eq_ignore_ascii_case(w) {
            return Some(start);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_command_complete_rows() {
        assert_eq!(parse_command_complete_rows("INSERT 0 1"), 1);
        assert_eq!(parse_command_complete_rows("UPDATE 5"), 5);
        assert_eq!(parse_command_complete_rows("DELETE 3"), 3);
        assert_eq!(parse_command_complete_rows("SELECT 10"), 10);
        assert_eq!(parse_command_complete_rows("CREATE TABLE"), -1);
        assert_eq!(parse_command_complete_rows("SELECT\0"), -1);
    }

    #[test]
    fn splits_statements_respecting_literals() {
        let sql = "SELECT 'a;b'; INSERT INTO t VALUES ('a;COMMIT'); END_PROC; SELECT 1";
        let parts = split_statements(sql);
        assert_eq!(parts.len(), 4);
        assert!(parts[1].contains("a;COMMIT"));
    }

    #[test]
    fn splits_on_dollar_quoted_bodies() {
        let sql =
            "CREATE FUNCTION f() RETURNS void AS $$ BEGIN; END; $$ LANGUAGE nzplsql; SELECT 2";
        let parts = split_statements(sql);
        assert_eq!(parts.len(), 2);
    }

    #[test]
    fn tracks_transaction_state() {
        assert_eq!(
            parse_transaction_state("BEGIN"),
            (TransactionState::Opened, true)
        );
        assert_eq!(
            parse_transaction_state("COMMIT"),
            (TransactionState::Closed, false)
        );
        assert_eq!(
            parse_transaction_state("SELECT CASE WHEN 1=1 THEN 2 END"),
            (TransactionState::Unchanged, false)
        );
        assert_eq!(
            parse_transaction_state("start transaction"),
            (TransactionState::Opened, true)
        );
        assert_eq!(
            parse_transaction_state("-- BEGIN\nSELECT 1"),
            (TransactionState::Unchanged, false)
        );
        assert_eq!(
            parse_transaction_state("BEGIN; COMMIT;"),
            (TransactionState::Closed, true)
        );
    }
}
