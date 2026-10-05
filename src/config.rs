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
use std::fmt;

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

#[derive(Clone)]
pub struct NzConnectionConfig {
    pub external_files: crate::ExternalFilePolicy,
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
    /// Default per-command timeout in seconds; 0 disables (default 0).
    pub command_timeout: u64,
    /// Application name reported to Netezza for Guardium audit.
    pub app_name: String,
    /// OS user name reported to Netezza.
    pub os_user: String,
    /// Client hostname reported to Netezza.
    pub client_host_name: String,
    /// Numeric Netezza client type (default: JDBC = 3).
    pub client_type: i16,
}

impl fmt::Debug for NzConnectionConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NzConnectionConfig")
            .field("external_files", &self.external_files)
            .field("host", &self.host)
            .field("port", &self.port)
            .field("database", &self.database)
            .field("user", &self.user)
            .field("password", &"[REDACTED]")
            .field("security_level", &self.security_level)
            .field("ssl_cert_path", &self.ssl_cert_path)
            .field("reject_unauthorized", &self.reject_unauthorized)
            .field("connection_timeout", &self.connection_timeout)
            .field("command_timeout", &self.command_timeout)
            .field("app_name", &self.app_name)
            .field("os_user", &self.os_user)
            .field("client_host_name", &self.client_host_name)
            .field("client_type", &self.client_type)
            .finish()
    }
}

impl Default for NzConnectionConfig {
    fn default() -> Self {
        Self {
            external_files: crate::ExternalFilePolicy::Disabled,
            host: String::new(),
            port: 5480,
            database: String::new(),
            user: String::new(),
            password: String::new(),
            security_level: SecurityLevel::default(),
            ssl_cert_path: None,
            reject_unauthorized: true,
            connection_timeout: 10,
            command_timeout: 0,
            app_name: String::from("netezza-rust"),
            os_user: std::env::var("USER").unwrap_or_else(|_| "unknown".into()),
            client_host_name: hostname_or_unknown(),
            client_type: crate::ClientTypeId::SQL_JDBC,
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

fn percent_decode(s: &str, plus_as_space: bool) -> NzResult<String> {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let hi = bytes.get(i + 1).and_then(|byte| hex_value(*byte));
            let lo = bytes.get(i + 2).and_then(|byte| hex_value(*byte));
            match (hi, lo) {
                (Some(hi), Some(lo)) => {
                    out.push((hi << 4) | lo);
                    i += 3;
                    continue;
                }
                _ => {
                    return Err(NzError::Config(
                        "Invalid percent encoding in connection string".into(),
                    ))
                }
            }
        }
        if plus_as_space && bytes[i] == b'+' {
            out.push(b' ');
        } else {
            out.push(bytes[i]);
        }
        i += 1;
    }
    let value = String::from_utf8(out)
        .map_err(|_| NzError::Config("Connection string contains invalid UTF-8".into()))?;
    if value.contains('\0') {
        return Err(NzError::Config("Connection string contains NUL".into()));
    }
    Ok(value)
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn parse_client_type(s: &str) -> NzResult<i16> {
    match s.to_ascii_lowercase().as_str() {
        "jdbc" | "sql_jdbc" | "sql-jdbc" => Ok(crate::ClientTypeId::SQL_JDBC),
        "odbc" | "sql_odbc" | "sql-odbc" => Ok(crate::ClientTypeId::SQL_ODBC),
        "node" => Ok(crate::ClientTypeId::NODE),
        "dotnet" | "sql_dotnet" | "sql-dotnet" => Ok(crate::ClientTypeId::SQL_DOTNET),
        "golang" | "go" | "sql_golang" => Ok(crate::ClientTypeId::SQL_GOLANG),
        "python" | "sql_python" => Ok(crate::ClientTypeId::SQL_PYTHON),
        "oledb" | "sql_oledb" => Ok(crate::ClientTypeId::SQL_OLEDB),
        "sql" => Ok(crate::ClientTypeId::SQL),
        _ => s
            .parse::<i16>()
            .map_err(|_| NzError::Config("Invalid client type".into())),
    }
}

/// Parse a Netezza connection URI into [`NzConnectionConfig`].
///
/// Supported forms:
/// - `netezza://user:pass@host:5480/database?sslmode=require&appName=myapp`
/// - `nz://user:pass@host/database`
pub fn parse_connection_string(connection_string: &str) -> NzResult<NzConnectionConfig> {
    let trimmed = connection_string.trim();
    let rest = if trimmed
        .get(..10)
        .is_some_and(|s| s.eq_ignore_ascii_case("netezza://"))
    {
        &trimmed[10..]
    } else if trimmed
        .get(..5)
        .is_some_and(|s| s.eq_ignore_ascii_case("nz://"))
    {
        &trimmed[5..]
    } else {
        return Err(NzError::Config(
            "Invalid connection string scheme; expected netezza:// or nz://".into(),
        ));
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
        if h.contains(':') {
            return Err(NzError::Config("IPv6 host must be bracketed".into()));
        }
        (
            h,
            Some(
                p.parse::<u16>()
                    .map_err(|_| NzError::Config("Invalid port in connection string".into()))?,
            ),
        )
    } else {
        (host_port, None)
    };

    if port == Some(0) {
        return Err(NzError::Config("Port must be nonzero".into()));
    }
    if host.is_empty() || host.contains('\0') {
        return Err(NzError::Config(
            "Connection string must include a host".into(),
        ));
    }
    let database = percent_decode(path, false)?;
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
        user: percent_decode(user, false)?,
        password: percent_decode(password, false)?,
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
            let key = percent_decode(key, true)?;
            let value = percent_decode(value, true)?;
            match key.to_ascii_lowercase().as_str() {
                "securitylevel" | "security_level" => {
                    config.security_level = SecurityLevel::parse(&value)
                        .ok_or_else(|| NzError::Config("Invalid security level".into()))?;
                }
                "sslcertfilepath" | "sslcerfilepath" | "ssl_cert" | "sslcert" => {
                    config.ssl_cert_path = Some(value)
                }
                "rejectunauthorized" | "reject_unauthorized" => {
                    config.reject_unauthorized = match value.as_str() {
                        "true" | "1" => true,
                        "false" | "0" => false,
                        _ => {
                            return Err(NzError::Config(
                                "Invalid rejectUnauthorized boolean".into(),
                            ))
                        }
                    };
                }
                "sslmode" => match value.as_str() {
                    "disable" => config.security_level = SecurityLevel::OnlyUnsecuredSession,
                    "require" | "verify-ca" | "verify-full" => {
                        config.security_level = SecurityLevel::OnlySecuredSession;
                        if value == "require" {
                            config.reject_unauthorized = false;
                        }
                    }
                    _ => return Err(NzError::Config("Invalid sslmode".into())),
                },
                "connectiontimeout" | "connection_timeout" => {
                    config.connection_timeout = value
                        .parse()
                        .map_err(|_| NzError::Config("Invalid connection timeout".into()))?;
                }
                "commandtimeout" | "command_timeout" => {
                    config.command_timeout = value
                        .parse()
                        .map_err(|_| NzError::Config("Invalid command timeout".into()))?
                }
                "appname" | "application_name" => config.app_name = value,
                "osuser" | "os_user" => config.os_user = value,
                "clienthostname" | "client_hostname" => config.client_host_name = value,
                "clienttype" | "client_type" => config.client_type = parse_client_type(&value)?,
                _ => {}
            }
        }
    }

    Ok(config)
}

/// Fluent connection settings. `build` checks required fields without displaying secrets.
#[derive(Debug, Clone, Default)]
pub struct ConfigBuilder {
    config: NzConnectionConfig,
}
impl ConfigBuilder {
    pub fn host(mut self, value: impl Into<String>) -> Self {
        self.config.host = value.into();
        self
    }
    pub fn port(mut self, value: u16) -> Self {
        self.config.port = value;
        self
    }
    pub fn database(mut self, value: impl Into<String>) -> Self {
        self.config.database = value.into();
        self
    }
    pub fn user(mut self, value: impl Into<String>) -> Self {
        self.config.user = value.into();
        self
    }
    pub fn password(mut self, value: impl Into<String>) -> Self {
        self.config.password = value.into();
        self
    }
    pub fn security_level(mut self, value: SecurityLevel) -> Self {
        self.config.security_level = value;
        self
    }
    pub fn external_files(mut self, value: crate::ExternalFilePolicy) -> Self {
        self.config.external_files = value;
        self
    }
    pub fn connection_timeout(mut self, seconds: u64) -> Self {
        self.config.connection_timeout = seconds;
        self
    }
    pub fn command_timeout(mut self, seconds: u64) -> Self {
        self.config.command_timeout = seconds;
        self
    }
    pub fn application_name(mut self, value: impl Into<String>) -> Self {
        self.config.app_name = value.into();
        self
    }
    pub fn client_type(mut self, value: i16) -> Self {
        self.config.client_type = value;
        self
    }
    pub fn build(self) -> NzResult<NzConnectionConfig> {
        self.config.validate()?;
        Ok(self.config)
    }
}
impl NzConnectionConfig {
    pub fn builder() -> ConfigBuilder {
        ConfigBuilder::default()
    }
    /// Validate values sent as protocol strings. Passwords are never included in errors.
    pub fn validate(&self) -> NzResult<()> {
        if self.host.is_empty()
            || self.database.is_empty()
            || self.user.is_empty()
            || self.port == 0
        {
            return Err(NzError::Config(
                "host, database, user and a nonzero port are required".into(),
            ));
        }
        if [
            &self.host,
            &self.database,
            &self.user,
            &self.password,
            &self.app_name,
            &self.os_user,
            &self.client_host_name,
        ]
        .iter()
        .any(|value| value.contains('\0'))
        {
            return Err(NzError::Config(
                "connection settings cannot contain NUL".into(),
            ));
        }
        Ok(())
    }
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
    fn connection_string_errors_do_not_expose_uri_or_password() {
        let uri = "https://user:secret@host/db";
        let error = parse_connection_string(uri).unwrap_err();
        let message = error.to_string();

        assert!(message.contains("Invalid connection string scheme"));
        assert!(!message.contains(uri));
        assert!(!message.contains("secret"));
    }

    #[test]
    fn parses_the_audited_credential_bearing_uri() {
        let cfg = parse_connection_string("netezza://user:secret@host/db").unwrap();
        assert_eq!(cfg.host, "host");
        assert_eq!(cfg.database, "db");
        assert_eq!(cfg.user, "user");
        assert_eq!(cfg.password, "secret");
    }

    #[test]
    fn connection_config_debug_redacts_password() {
        let cfg = NzConnectionConfig::new("host", "db", "user", "secret");
        let debug = format!("{cfg:?}");

        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains("secret"));
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
    #[test]
    fn unicode_and_invalid_ports_are_rejected_without_panicking() {
        for value in ["aaaaaaaaaé://u:p@h/db", "aaaaé://u:p@h/db", "🦀🦀🦀"] {
            assert!(parse_connection_string(value).is_err());
        }
    }
    #[test]
    fn builder_and_uri_validation_reject_invalid_settings_without_secrets() {
        assert!(NzConnectionConfig::builder()
            .host("h")
            .database("d")
            .user("u")
            .password("secret")
            .build()
            .is_ok());
        assert!(NzConnectionConfig::builder()
            .host("h")
            .database("d")
            .user("u")
            .port(0)
            .build()
            .is_err());
        for uri in [
            "nz://u:p@h:abc/db",
            "nz://u:p@h:0/db",
            "nz://u:p@h:65536/db",
            "nz://u:%00@h/db",
            "nz://u:%FF@h/db",
            "nz://u:%x0@h/db",
            "nz://u:p@h/db?sslmode=bad",
            "nz://u:p@h/db?rejectUnauthorized=typo",
        ] {
            assert!(parse_connection_string(uri).is_err());
        }
    }

    #[test]
    fn default_client_type_is_jdbc() {
        assert_eq!(
            NzConnectionConfig::default().client_type,
            crate::ClientTypeId::SQL_JDBC
        );
        assert_eq!(
            NzConnectionConfig::new("h", "d", "u", "p").client_type,
            crate::ClientTypeId::SQL_JDBC
        );
        assert_eq!(
            parse_connection_string("nz://u:p@h/db")
                .unwrap()
                .client_type,
            crate::ClientTypeId::SQL_JDBC
        );
    }

    #[test]
    fn client_type_can_be_overridden_via_builder_and_uri() {
        let cfg = NzConnectionConfig::builder()
            .host("h")
            .database("d")
            .user("u")
            .password("p")
            .client_type(crate::ClientTypeId::NODE)
            .build()
            .unwrap();
        assert_eq!(cfg.client_type, crate::ClientTypeId::NODE);

        let cfg = parse_connection_string("nz://u:p@h/db?clientType=15").unwrap();
        assert_eq!(cfg.client_type, 15);
        let cfg = parse_connection_string("nz://u:p@h/db?client_type=node").unwrap();
        assert_eq!(cfg.client_type, crate::ClientTypeId::NODE);
        let cfg = parse_connection_string("nz://u:p@h/db?clientType=jdbc").unwrap();
        assert_eq!(cfg.client_type, crate::ClientTypeId::SQL_JDBC);
        assert!(parse_connection_string("nz://u:p@h/db?clientType=bad").is_err());
    }
}
