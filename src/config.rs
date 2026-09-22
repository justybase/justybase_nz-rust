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

//! Connection configuration and `netezza://` / `nz://` URI parsing.
//!
//! Port of the Node driver `connectionString.ts`.

use crate::error::{NzError, NzResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SecurityLevel {
    /// Default: request unsecured, allow the server to upgrade if it must.
    #[default]
    PreferredUnsecured,
    OnlyUnsecuredSession,
    PreferredSecuredSession,
    OnlySecuredSession,
}

impl SecurityLevel {
    pub fn as_i32(self) -> i32 {
        match self {
            SecurityLevel::PreferredUnsecured => 0,
            SecurityLevel::OnlyUnsecuredSession => 1,
            SecurityLevel::PreferredSecuredSession => 2,
            SecurityLevel::OnlySecuredSession => 3,
        }
    }
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "PreferredUnsecured" => Some(SecurityLevel::PreferredUnsecured),
            "OnlyUnsecuredSession" => Some(SecurityLevel::OnlyUnsecuredSession),
            "PreferredSecuredSession" => Some(SecurityLevel::PreferredSecuredSession),
            "OnlySecuredSession" => Some(SecurityLevel::OnlySecuredSession),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct NzConnectionConfig {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub user: String,
    pub password: String,
    pub security_level: SecurityLevel,
    /// Optional path to a trusted CA certificate (PEM) for TLS sessions.
    pub ssl_cert_path: Option<String>,
    /// When false, self-signed certificates are accepted (TLS only).
    pub reject_unauthorized: bool,
    /// Connection timeout in seconds (default 10).
    pub connection_timeout: u64,
    /// Default per-command timeout in seconds; 0 disables (default 30).
    pub command_timeout: u64,
    /// Application name reported to Netezza for Guardium audit.
    pub app_name: String,
    /// OS user name reported to Netezza.
    pub os_user: String,
    /// Client hostname reported to Netezza.
    pub client_host_name: String,
    /// Numeric Netezza client type (default: Node = 15).
    pub client_type: i16,
}

impl Default for NzConnectionConfig {
    fn default() -> Self {
        Self {
            host: String::new(),
            port: 5480,
            database: String::new(),
            user: String::new(),
            password: String::new(),
            security_level: SecurityLevel::default(),
            ssl_cert_path: None,
            reject_unauthorized: true,
            connection_timeout: 10,
            command_timeout: 30,
            app_name: String::from("netezza-rust"),
            os_user: std::env::var("USER").unwrap_or_else(|_| "unknown".into()),
            client_host_name: hostname_or_unknown(),
            client_type: crate::ClientTypeId::NODE,
        }
    }
}

fn hostname_or_unknown() -> String {
    std::fs::read_to_string("/etc/hostname")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "unknown".into())
}

impl NzConnectionConfig {
    pub fn new(host: &str, database: &str, user: &str, password: &str) -> Self {
        NzConnectionConfig {
            host: host.into(),
            database: database.into(),
            user: user.into(),
            password: password.into(),
            ..Default::default()
        }
    }
}

fn percent_decode(s: &str, plus_as_space: bool) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let (Some(hi), Some(lo)) = (hex_value(bytes[i + 1]), hex_value(bytes[i + 2])) {
                let v = (hi << 4) | lo;
                out.push(v);
                i += 3;
                continue;
            }
        }
        if plus_as_space && bytes[i] == b'+' {
            out.push(b' ');
        } else {
            out.push(bytes[i]);
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).to_string()
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

/// Parse a Netezza connection URI into [`NzConnectionConfig`].
///
/// Supported forms:
/// - `netezza://user:pass@host:5480/database?sslmode=require&appName=myapp`
/// - `nz://user:pass@host/database`
pub fn parse_connection_string(connection_string: &str) -> NzResult<NzConnectionConfig> {
    let trimmed = connection_string.trim();
    let rest = if trimmed.len() >= 10 && trimmed[..10].eq_ignore_ascii_case("netezza://") {
        &trimmed[10..]
    } else if trimmed.len() >= 5 && trimmed[..5].eq_ignore_ascii_case("nz://") {
        &trimmed[5..]
    } else {
        return Err(NzError::Config(format!(
            "Invalid connection string: {connection_string}"
        )));
    };

    let (authority, query) = match rest.split_once('?') {
        Some((a, q)) => (a, Some(q)),
        None => (rest, None),
    };

    let (userinfo_host, path) = match authority.split_once('/') {
        Some((h, p)) => (h, p),
        None => (authority, ""),
    };
    let (user_pass, host_port) = match userinfo_host.rsplit_once('@') {
        Some((u, h)) => (u, h),
        None => ("", userinfo_host),
    };
    let (user, password) = match user_pass.split_once(':') {
        Some((u, p)) => (u, p),
        None => (user_pass, ""),
    };
    let (host, port) = if let Some(bracketed) = host_port.strip_prefix('[') {
        let close = bracketed
            .find(']')
            .ok_or_else(|| NzError::Config("Invalid bracketed host in connection string".into()))?;
        let host = &bracketed[..close];
        let remainder = &bracketed[close + 1..];
        let port = if remainder.is_empty() {
            None
        } else {
            Some(
                remainder
                    .strip_prefix(':')
                    .ok_or_else(|| {
                        NzError::Config("Invalid bracketed host port in connection string".into())
                    })?
                    .parse::<u16>()
                    .map_err(|_| NzError::Config("Invalid port in connection string".into()))?,
            )
        };
        (host, port)
    } else if let Some((h, p)) = host_port.rsplit_once(':') {
        if let Ok(port) = p.parse::<u16>() {
            (h, Some(port))
        } else {
            (host_port, None)
        }
    } else {
        (host_port, None)
    };

    if host.is_empty() {
        return Err(NzError::Config(
            "Connection string must include a host".into(),
        ));
    }
    let database = percent_decode(path, false);
    if database.is_empty() {
        return Err(NzError::Config(
            "Connection string must include a database path, e.g. netezza://user:pass@host/db"
                .into(),
        ));
    }
    if user.is_empty() {
        return Err(NzError::Config(
            "Connection string must include a user".into(),
        ));
    }

    let mut config = NzConnectionConfig {
        host: host.into(),
        database,
        user: percent_decode(user, false),
        password: percent_decode(password, false),
        ..Default::default()
    };
    if let Some(p) = port {
        config.port = p;
    }

    if let Some(query) = query {
        for pair in query.split('&') {
            let (key, value) = match pair.split_once('=') {
                Some((k, v)) => (k, v),
                None => (pair, ""),
            };
            let key = percent_decode(key, true);
            let value = percent_decode(value, true);
            match key.to_ascii_lowercase().as_str() {
                "securitylevel" | "security_level" => {
                    if let Some(level) = SecurityLevel::parse(&value) {
                        config.security_level = level;
                    }
                }
                "sslcerfilepath" | "ssl_cert" | "sslcert" => config.ssl_cert_path = Some(value),
                "rejectunauthorized" | "reject_unauthorized" => {
                    config.reject_unauthorized = value == "true" || value == "1";
                }
                "sslmode" => match value.as_str() {
                    "disable" => config.security_level = SecurityLevel::OnlyUnsecuredSession,
                    "require" | "verify-ca" | "verify-full" => {
                        config.security_level = SecurityLevel::OnlySecuredSession;
                        if value == "require" {
                            config.reject_unauthorized = false;
                        }
                    }
                    _ => {}
                },
                "connectiontimeout" | "connection_timeout" => {
                    if let Ok(v) = value.parse() {
                        config.connection_timeout = v;
                    }
                }
                "appname" | "application_name" => config.app_name = value,
                "osuser" | "os_user" => config.os_user = value,
                "clienthostname" | "client_hostname" => config.client_host_name = value,
                _ => {}
            }
        }
    }

    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_full_uri() {
        let cfg = parse_connection_string(
            "netezza://admin:secret@nz-host:5480/JUST_DATA?sslmode=require&appName=myapp",
        )
        .unwrap();
        assert_eq!(cfg.host, "nz-host");
        assert_eq!(cfg.port, 5480);
        assert_eq!(cfg.database, "JUST_DATA");
        assert_eq!(cfg.user, "admin");
        assert_eq!(cfg.password, "secret");
        assert_eq!(cfg.security_level, SecurityLevel::OnlySecuredSession);
        assert!(!cfg.reject_unauthorized); // sslmode=require
        assert_eq!(cfg.app_name, "myapp");
    }

    #[test]
    fn parses_minimal_uri() {
        let cfg = parse_connection_string("nz://user:pw@host/db").unwrap();
        assert_eq!(cfg.host, "host");
        assert_eq!(cfg.port, 5480);
        assert_eq!(cfg.user, "user");
        assert_eq!(cfg.database, "db");
    }

    #[test]
    fn rejects_missing_database() {
        assert!(parse_connection_string("nz://u:p@h").is_err());
    }

    #[test]
    fn percent_decodes() {
        let cfg = parse_connection_string("nz://u:p%40ss@h/db").unwrap();
        assert_eq!(cfg.password, "p@ss");
    }

    #[test]
    fn accepts_case_insensitive_scheme_and_decodes_query_values() {
        let cfg = parse_connection_string(
            "Netezza://u+name:p+word@host/db+name?appName=my+app&os_user=worker%2B1",
        )
        .unwrap();
        assert_eq!(cfg.user, "u+name");
        assert_eq!(cfg.password, "p+word");
        assert_eq!(cfg.database, "db+name");
        assert_eq!(cfg.app_name, "my app");
        assert_eq!(cfg.os_user, "worker+1");
    }

    #[test]
    fn parses_bracketed_ipv6_host() {
        let cfg = parse_connection_string("nz://u:p@[::1]:5481/db").unwrap();
        assert_eq!(cfg.host, "::1");
        assert_eq!(cfg.port, 5481);
    }
}
