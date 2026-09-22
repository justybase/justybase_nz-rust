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

//! Driver error types.
//!
//! Port of the Node driver `errors/NzDatabaseError.ts` and
//! `protocol/ProtocolLength.ts` (`NzProtocolError`).

use std::fmt;

/// Structured database error thrown for backend ErrorResponse payloads.
/// Fields follow PostgreSQL-style ErrorResponse encoding when present.
#[derive(Debug, Clone, Default)]
pub struct NzDatabaseError {
    /// Severity (e.g. ERROR, FATAL, PANIC) when provided by the backend.
    pub severity: Option<String>,
    /// SQLSTATE / error code when provided by the backend.
    pub code: Option<String>,
    /// Primary human-readable message.
    pub message: String,
    /// Optional detail.
    pub detail: Option<String>,
    /// Optional hint.
    pub hint: Option<String>,
    /// All backend diagnostic fields keyed by their protocol field code.
    pub diagnostics: Vec<(char, String)>,
    /// Raw payload as received from the backend.
    pub raw: String,
}

impl NzDatabaseError {
    pub fn new(raw: impl Into<String>) -> Self {
        let raw = raw.into();
        Self {
            message: raw.trim().to_string(),
            raw,
            ..Default::default()
        }
    }
}

impl fmt::Display for NzDatabaseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)?;
        if let Some(code) = &self.code {
            write!(f, " (SQLSTATE {code})")?;
        }
        Ok(())
    }
}

impl std::error::Error for NzDatabaseError {}

/// Parse a PostgreSQL/Netezza ErrorResponse or NoticeResponse body.
///
/// Body is a sequence of `typeByte + null-terminated C string`, ending with a
/// final NUL. Legacy payloads (plain text without field structure) fall back
/// to treating the whole payload as the message — identical logic to the Node
/// driver (`parseBackendErrorFields`).
pub fn parse_backend_error_fields(data: &[u8]) -> NzDatabaseError {
    let raw = String::from_utf8_lossy(data).to_string();
    let raw_trimmed = raw.trim_end_matches('\0').to_string();

    // A structured body has a NUL after each field and one final terminator,
    // while legacy text has at most the single terminator at its end.
    let nul_count = data.iter().filter(|&&b| b == 0).count();

    let mut err = NzDatabaseError {
        raw: raw_trimmed,
        ..Default::default()
    };

    if nul_count >= 2 {
        let mut i = 0usize;
        while i < data.len() {
            let field_type = data[i];
            i += 1;
            if field_type == 0 {
                break;
            }
            let mut end = i;
            while end < data.len() && data[end] != 0 {
                end += 1;
            }
            let value = String::from_utf8_lossy(&data[i..end]).to_string();
            i = if end < data.len() { end + 1 } else { end };

            let field = field_type as char;
            err.diagnostics.push((field, value.clone()));
            match field {
                'S' if err.severity.is_none() => err.severity = Some(value),
                'V' => err.severity = Some(value),
                'C' => err.code = Some(value),
                'M' => err.message = value,
                'D' => err.detail = Some(value),
                'H' => err.hint = Some(value),
                _ => {}
            }
        }
    }

    if err.message.is_empty() {
        err.message = raw.replace('\0', "").trim().to_string();
        if err.message.is_empty() {
            err.message = "Netezza backend returned an empty error response".into();
        }
    }
    err
}

/// Top-level driver error.
///
/// `Database` is boxed: `NzDatabaseError` carries several `String`s plus a
/// diagnostics vector, and without the box every `NzResult<T>` must reserve
/// that whole payload on the stack — clippy flags 145 call sites
/// (`result_large_err`) and the hot path (row decoding) never needs the
/// payload inline.
#[derive(Debug)]
pub enum NzError {
    /// Transport / I/O failure.
    Io(std::io::Error),
    /// Protocol framing violation — the connection is no longer safe to reuse.
    Protocol(String),
    /// Backend returned an ErrorResponse.
    Database(Box<NzDatabaseError>),
    /// Invalid configuration or connection string.
    Config(String),
    /// Feature not supported by this driver build.
    Unsupported(String),
    /// Connection is closed or was closed during an operation.
    Closed(String),
    /// Operation timed out.
    Timeout(String),
}

impl NzError {
    /// True for protocol-fault errors after which the socket must be dropped.
    pub fn is_protocol_fault(&self) -> bool {
        matches!(self, NzError::Protocol(_))
    }
}

impl fmt::Display for NzError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NzError::Io(e) => write!(f, "I/O error: {e}"),
            NzError::Protocol(m) => write!(f, "protocol error: {m}"),
            NzError::Database(e) => write!(f, "database error: {e}"),
            NzError::Config(m) => write!(f, "config error: {m}"),
            NzError::Unsupported(m) => write!(f, "unsupported: {m}"),
            NzError::Closed(m) => write!(f, "connection closed: {m}"),
            NzError::Timeout(m) => write!(f, "timeout: {m}"),
        }
    }
}

impl std::error::Error for NzError {}

impl From<std::io::Error> for NzError {
    fn from(e: std::io::Error) -> Self {
        NzError::Io(e)
    }
}

impl From<NzDatabaseError> for NzError {
    fn from(e: NzDatabaseError) -> Self {
        NzError::Database(Box::new(e))
    }
}

pub type NzResult<T> = Result<T, NzError>;

/// Upper bound for length-prefixed protocol fields (port of `MAX_PROTOCOL_PAYLOAD`).
pub const MAX_PROTOCOL_PAYLOAD: i32 = 10_000_000;

/// Validate a backend-supplied length before allocating or skipping.
/// Port of `validateProtocolLength`.
pub fn validate_protocol_length(length: i32, field: &str, allow_zero: bool) -> NzResult<i32> {
    if length < 0 {
        return Err(NzError::Protocol(format!(
            "Invalid backend protocol length for '{field}': {length}; negative lengths are not valid. \
             The connection is no longer safe to reuse; reconnect is required."
        )));
    }
    if !allow_zero && length == 0 {
        return Err(NzError::Protocol(format!(
            "Invalid backend protocol length for '{field}': 0; zero is not valid for this field. \
             The connection is no longer safe to reuse; reconnect is required."
        )));
    }
    if length > MAX_PROTOCOL_PAYLOAD {
        return Err(NzError::Protocol(format!(
            "Invalid backend protocol length for '{field}': {length}; maximum supported payload is \
             {MAX_PROTOCOL_PAYLOAD} bytes. The connection is no longer safe to reuse; reconnect is required."
        )));
    }
    Ok(length)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Encode a PostgreSQL-style ErrorResponse body from `(field, value)` pairs.
    fn encode_fields(fields: &[(char, &str)]) -> Vec<u8> {
        let mut out = Vec::new();
        for (kind, value) in fields {
            out.push(*kind as u8);
            out.extend_from_slice(value.as_bytes());
            out.push(0);
        }
        out.push(0);
        out
    }

    #[test]
    fn parses_structured_fields() {
        let body = encode_fields(&[
            ('S', "ERROR"),
            ('C', "42P01"),
            ('M', "relation \"foo\" does not exist"),
            ('D', "extra detail"),
            ('H', "check the name"),
        ]);
        let e = parse_backend_error_fields(&body);
        assert_eq!(e.severity.as_deref(), Some("ERROR"));
        assert_eq!(e.code.as_deref(), Some("42P01"));
        assert_eq!(e.message, "relation \"foo\" does not exist");
        assert_eq!(e.detail.as_deref(), Some("extra detail"));
        assert_eq!(e.hint.as_deref(), Some("check the name"));
        assert!(e.diagnostics.contains(&('S', "ERROR".into())));
    }

    #[test]
    fn prefers_non_localized_severity() {
        let body = encode_fields(&[
            ('S', "BŁĄD"),
            ('V', "ERROR"),
            ('C', "XX000"),
            ('M', "failure"),
            ('X', "future-field"),
        ]);
        let e = parse_backend_error_fields(&body);
        assert_eq!(e.severity.as_deref(), Some("ERROR"));
        assert_eq!(e.diagnostics.len(), 5);
        assert!(e.diagnostics.contains(&('X', "future-field".into())));
    }

    #[test]
    fn falls_back_to_raw_text_without_message_field() {
        let e = parse_backend_error_fields(b"plain error text\0");
        assert_eq!(e.message, "plain error text");
        assert!(e.code.is_none());
    }

    #[test]
    fn empty_response_gets_stable_message() {
        let e = parse_backend_error_fields(&[0]);
        assert_eq!(
            e.message,
            "Netezza backend returned an empty error response"
        );
    }

    #[test]
    fn display_includes_sqlstate() {
        let body = encode_fields(&[('S', "ERROR"), ('C', "28P01"), ('M', "auth failed")]);
        let e = parse_backend_error_fields(&body);
        assert_eq!(e.to_string(), "auth failed (SQLSTATE 28P01)");
    }

    #[test]
    fn protocol_length_validation() {
        assert_eq!(validate_protocol_length(0, "f", true).unwrap(), 0);
        assert!(validate_protocol_length(0, "f", false).is_err());
        assert!(validate_protocol_length(-1, "f", true).is_err());
        assert_eq!(
            validate_protocol_length(MAX_PROTOCOL_PAYLOAD, "f", false).unwrap(),
            MAX_PROTOCOL_PAYLOAD
        );
        assert!(validate_protocol_length(MAX_PROTOCOL_PAYLOAD + 1, "f", true).is_err());
    }
}
