//! Conformance runner for the Rust driver (conformance/runner-contract.md v1).
//!
//!     cargo run --example conformance_runner -- --profile smoke
//!
//! Exit codes: 0 ok, 1 failures, 2 configuration error, 3 crash.

use std::env;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::Duration;

use nz_rust::{
    Client, ColumnDesc, NzConnectionConfig, NzError, NzValue, Pool, PoolConfig, QueryOptions,
    SecurityLevel, ToSql,
};
use serde_json::{json, Value};

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crate has a parent")
        .to_path_buf()
}

fn env_value(name: &str) -> Option<String> {
    env::var(name).ok().filter(|value| !value.is_empty())
}

struct Settings {
    host: String,
    port: u16,
    database: String,
    user: String,
    password: String,
    schema: String,
}

fn settings() -> Result<Settings, String> {
    let host = env_value("NZ_DEV_HOST");
    let user = env_value("NZ_DEV_USER");
    let password = env_value("NZ_DEV_PASSWORD");
    let database = env_value("NZ_DEV_DB").or_else(|| env_value("NZ_DEV_DATABASE"));
    let mut missing = Vec::new();
    if host.is_none() {
        missing.push("NZ_DEV_HOST");
    }
    if user.is_none() {
        missing.push("NZ_DEV_USER");
    }
    if password.is_none() {
        missing.push("NZ_DEV_PASSWORD");
    }
    if database.is_none() {
        missing.push("NZ_DEV_DB (or NZ_DEV_DATABASE)");
    }
    if !missing.is_empty() {
        return Err(format!("missing configuration: {}", missing.join(", ")));
    }
    let mut host = host.unwrap();
    if host == "localhost" || host == "::1" {
        host = "127.0.0.1".into();
    }
    Ok(Settings {
        host,
        port: env_value("NZ_DEV_PORT")
            .and_then(|value| value.parse().ok())
            .unwrap_or(5480),
        database: database.unwrap(),
        user: user.unwrap(),
        password: password.unwrap(),
        schema: env_value("NZ_CONFORMANCE_SCHEMA").unwrap_or_else(|| "CONFORMANCE".into()),
    })
}

fn load_cases(dir: &Path) -> Result<Vec<Value>, String> {
    let mut cases = Vec::new();
    let mut entries: Vec<_> = fs::read_dir(dir)
        .map_err(|error| format!("cannot read {}: {error}", dir.display()))?
        .filter_map(Result::ok)
        .collect();
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        let path = entry.path();
        if path.extension().and_then(|value| value.to_str()) != Some("json") {
            continue;
        }
        let text = fs::read_to_string(&path).map_err(|error| error.to_string())?;
        let document: Value = serde_json::from_str(&text).map_err(|error| error.to_string())?;
        if document.get("kit_version").and_then(Value::as_i64) != Some(1) {
            return Err(format!("unsupported kit_version in {}", path.display()));
        }
        if let Some(items) = document.get("cases").and_then(Value::as_array) {
            cases.extend(items.iter().cloned());
        }
    }
    Ok(cases)
}

fn select_cases(cases: Vec<Value>, profile: &str, filters: &[String]) -> Vec<Value> {
    cases
        .into_iter()
        .filter(|case| {
            let id = case.get("id").and_then(Value::as_str).unwrap_or("");
            if !filters.is_empty() {
                return filters.iter().any(|needle| id.contains(needle));
            }
            let case_profile = case
                .get("profile")
                .and_then(Value::as_str)
                .unwrap_or("full");
            if profile == "full" {
                case_profile == "full" || case_profile == "smoke"
            } else {
                case_profile == profile
            }
        })
        .collect()
}

fn render_sql(sql: &str, schema: &str) -> String {
    if schema.eq_ignore_ascii_case("CONFORMANCE") {
        sql.to_string()
    } else {
        sql.replace("CONFORMANCE.", &format!("{}.", schema.to_uppercase()))
            .replace("'CONFORMANCE'", &format!("'{}'", schema.to_uppercase()))
    }
}

fn classify_oid(oid: i32) -> &'static str {
    match oid {
        16 => "bool",
        17 => "bytes",
        18 => "char",
        19 => "text",
        20 => "int8",
        21 => "int2",
        23 => "int4",
        25 => "text",
        26 => "int4",
        700 => "float4",
        701 => "float8",
        705 => "unknown",
        1042 => "char",
        1043 => "text",
        1082 => "date",
        1083 => "time",
        1114 => "timestamp",
        1184 => "timestamp",
        1186 => "interval",
        1266 => "timetz",
        1700 => "numeric",
        2500 => "int1",
        2522 => "nchar",
        2530 => "nvarchar",
        2569 => "bytes",
        2950 => "text",
        3614 => "text",
        3802 => "text",
        114 => "text",
        _ => "unknown",
    }
}

fn normalize_timetz(text: &str) -> String {
    let bytes = text.as_bytes();
    if bytes.len() >= 3 {
        let sign = bytes[bytes.len() - 3];
        if (sign == b'+' || sign == b'-') && bytes[bytes.len() - 2..].iter().all(u8::is_ascii_digit)
        {
            return format!("{text}:00");
        }
    }
    text.to_string()
}

fn canonical(value: &NzValue, _type_class: &str) -> Value {
    match value {
        NzValue::Null => Value::Null,
        NzValue::Bool(value) => Value::Bool(*value),
        NzValue::Int2(value) => Value::String(value.to_string()),
        NzValue::Int4(value) => Value::String(value.to_string()),
        NzValue::Int8(value) => Value::String(value.to_string()),
        NzValue::Float4(value) => float_json(*value as f64),
        NzValue::Float8(value) => float_json(*value),
        NzValue::Numeric(text) => Value::String(text.clone()),
        NzValue::Decimal(value) => Value::String(value.to_string()),
        NzValue::Text(text)
        | NzValue::Date(text)
        | NzValue::Time(text)
        | NzValue::Timestamp(text) => Value::String(text.clone()),
        NzValue::Timetz(text) => Value::String(normalize_timetz(text)),
        NzValue::Interval(text) => parse_interval(text),
        NzValue::Bytea(bytes) => Value::String(base64_encode(bytes)),
    }
}

/// Parse the Netezza interval text form ("1 year 2 mons 3 days 04:05:06")
/// and the .NET TimeSpan form ("3.04:05:06") into the kit's canonical
/// months/days/microseconds object.
fn parse_interval(text: &str) -> Value {
    let mut months = 0i64;
    let mut days = 0i64;
    let mut microseconds = 0i64;

    let mut body = text.trim();
    let mut negative = false;
    if let Some(rest) = body.strip_prefix('-') {
        negative = true;
        body = rest.trim();
    }

    let parts: Vec<&str> = body.split_whitespace().collect();
    let mut index = 0;
    while index < parts.len() {
        let token = parts[index];

        if token.contains(':') {
            let mut time = token;
            let mut sign = 1i64;
            if let Some(rest) = time.strip_prefix('-') {
                sign = -1;
                time = rest;
            } else if let Some(rest) = time.strip_prefix('+') {
                time = rest;
            }

            let first_colon = time.find(':').unwrap_or(0);
            let mut token_days = 0i64;
            if let Some(dot) = time.find('.') {
                if dot < first_colon {
                    token_days = time[..dot].parse().unwrap_or(0);
                    time = &time[dot + 1..];
                }
            }

            let mut components = time.split(':');
            let hours: i64 = components.next().unwrap_or("0").parse().unwrap_or(0);
            let minutes: i64 = components.next().unwrap_or("0").parse().unwrap_or(0);
            let seconds_text = components.next().unwrap_or("0");
            let (seconds, fraction) = match seconds_text.split_once('.') {
                Some((whole, frac)) => {
                    let mut digits = String::from(frac);
                    digits.truncate(6);
                    while digits.len() < 6 {
                        digits.push('0');
                    }
                    (
                        whole.parse::<i64>().unwrap_or(0),
                        digits.parse::<i64>().unwrap_or(0),
                    )
                }
                None => (seconds_text.parse::<i64>().unwrap_or(0), 0),
            };

            let total = ((token_days * 24 + hours) * 60 + minutes) * 60_000_000
                + seconds * 1_000_000
                + fraction;
            microseconds += sign * total;
            index += 1;
            continue;
        }

        if let Ok(value) = token.parse::<i64>() {
            if index + 1 < parts.len() {
                let unit = parts[index + 1].to_ascii_lowercase();
                if unit.starts_with("year") {
                    months += value * 12;
                    index += 2;
                    continue;
                }
                if unit.starts_with("mon") {
                    months += value;
                    index += 2;
                    continue;
                }
                if unit.starts_with("day") {
                    days += value;
                    index += 2;
                    continue;
                }
            }
        }

        index += 1;
    }

    if negative {
        months = -months;
        days = -days;
        microseconds = -microseconds;
    }

    json!({
        "months": months,
        "days": days,
        "microseconds": microseconds,
    })
}

fn base64_encode(bytes: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut output = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let first = chunk[0] as u32;
        let second = chunk.get(1).copied().unwrap_or(0) as u32;
        let third = chunk.get(2).copied().unwrap_or(0) as u32;
        let triple = (first << 16) | (second << 8) | third;
        output.push(TABLE[(triple >> 18) as usize & 0x3f] as char);
        output.push(TABLE[(triple >> 12) as usize & 0x3f] as char);
        output.push(if chunk.len() > 1 {
            TABLE[(triple >> 6) as usize & 0x3f] as char
        } else {
            '='
        });
        output.push(if chunk.len() > 2 {
            TABLE[triple as usize & 0x3f] as char
        } else {
            '='
        });
    }
    output
}

fn float_json(value: f64) -> Value {
    if value.is_nan() {
        Value::String("NaN".into())
    } else if value.is_infinite() && value.is_sign_positive() {
        Value::String("Infinity".into())
    } else if value.is_infinite() {
        Value::String("-Infinity".into())
    } else {
        json!(value)
    }
}

fn type_name_for_oid(oid: i32, declared: String) -> String {
    if !declared.starts_with("OID") {
        return declared;
    }
    match oid {
        17 => "BYTEA".into(),
        2568 | 2569 => "VARBINARY".into(),
        _ => declared,
    }
}

fn columns_json(columns: &[ColumnDesc]) -> Vec<Value> {
    columns
        .iter()
        .map(|column| {
            json!({
                "name": column.name,
                "type_name": type_name_for_oid(column.type_oid, column.declared_type_name()),
                "type_class": classify_oid(column.type_oid),
            })
        })
        .collect()
}

fn rows_json(rows: &[nz_rust::Row], columns: &[ColumnDesc]) -> Vec<Value> {
    rows.iter()
        .map(|row| {
            Value::Array(
                row.values()
                    .iter()
                    .enumerate()
                    .map(|(index, value)| {
                        let class = columns
                            .get(index)
                            .map(|column| classify_oid(column.type_oid))
                            .unwrap_or("unknown");
                        canonical(value, class)
                    })
                    .collect(),
            )
        })
        .collect()
}

fn body_params(case: &Value) -> Vec<NzValue> {
    case.get("params")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .map(|item| match item.get("value").unwrap_or(&Value::Null) {
                    Value::Null => NzValue::Null,
                    Value::Bool(value) => NzValue::Bool(*value),
                    Value::String(value) => NzValue::Text(value.clone()),
                    Value::Number(value) => value
                        .as_i64()
                        .map(NzValue::Int8)
                        .unwrap_or_else(|| NzValue::Float8(value.as_f64().unwrap_or_default())),
                    _ => NzValue::Text(item.to_string()),
                })
                .collect()
        })
        .unwrap_or_default()
}

fn error_json(error: &NzError) -> Value {
    let message = error.to_string();
    let lowered = message.to_lowercase();
    let class = if lowered.contains("ssl")
        || lowered.contains("tls")
        || lowered.contains("certificate")
        || lowered.contains("handshake")
    {
        "connection"
    } else {
        match error {
            NzError::Timeout(_) => "timeout",
            NzError::Cancelled(_) => "cancelled",
            NzError::Config(_) => "configuration",
            NzError::Io(_) | NzError::Closed(_) => "connection",
            NzError::Protocol(_) => "other",
            NzError::Unsupported(_) => "other",
            NzError::Database(_) => {
                if lowered.contains("syntax") || lowered.contains("expecting") {
                    "syntax"
                } else if lowered.contains("permission") || lowered.contains("denied") {
                    "permission"
                } else if lowered.contains("function") && lowered.contains("does not exist") {
                    "function"
                } else if lowered.contains("cannot contain null") || lowered.contains("not null") {
                    "constraint"
                } else if lowered.contains("attribute") || lowered.contains("column") {
                    "column"
                } else if lowered.contains("does not exist") || lowered.contains("not found") {
                    "relation"
                } else if lowered.contains("divide by 0")
                    || lowered.contains("out of range")
                    || lowered.contains("invalid")
                    || lowered.contains("can't parse")
                {
                    "type"
                } else {
                    "other"
                }
            }
        }
    };
    let code = match error {
        NzError::Database(database) => database.code.clone(),
        _ => None,
    };
    json!({ "class": class, "message": message, "code": code })
}

fn connection_config(settings: &Settings) -> NzConnectionConfig {
    NzConnectionConfig {
        host: settings.host.clone(),
        port: settings.port,
        database: settings.database.clone(),
        user: settings.user.clone(),
        password: settings.password.clone(),
        // The conformance runner owns the temp directory it exports to and
        // imports from; production callers keep the default Disabled policy.
        external_files: nz_rust::ExternalFilePolicy::Directory(std::env::temp_dir()),
        ..Default::default()
    }
}

fn api_columns(spec: &[(&str, &str, &str)]) -> Value {
    Value::Array(
        spec.iter()
            .map(|(name, type_name, type_class)| {
                json!({
                    "name": name,
                    "type_name": type_name,
                    "type_class": type_class,
                })
            })
            .collect(),
    )
}

fn normalize_ddl(text: &str) -> String {
    text.to_uppercase()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

async fn api_metadata_ddl(
    client: &Client,
    settings: &Settings,
    spec: &Value,
) -> Result<(Value, Vec<Value>), NzError> {
    let schema = spec
        .get("schema")
        .and_then(Value::as_str)
        .unwrap_or(settings.schema.as_str());
    let table = spec.get("table").and_then(Value::as_str).unwrap_or("");
    let meta = client.metadata();
    let mut checks = Vec::new();

    let tables = meta.tables(Some(schema), None).await?;
    for name in spec
        .get("tables")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
    {
        let present = tables
            .iter()
            .any(|item| item.name.eq_ignore_ascii_case(name));
        checks.push(json!(["tables", name, present]));
    }

    let columns = meta.columns(table, Some(schema)).await?;
    for name in spec
        .get("columns")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
    {
        let present = columns
            .iter()
            .any(|item| item.name.eq_ignore_ascii_case(name));
        checks.push(json!(["columns", name, present]));
    }

    let ddl = meta.table_ddl(table, Some(schema), None).await?;
    let haystack = normalize_ddl(&ddl);
    for fragment in spec
        .get("fragments")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
    {
        checks.push(json!([
            "ddl",
            fragment,
            haystack.contains(&normalize_ddl(fragment))
        ]));
    }

    Ok((
        api_columns(&[
            ("check", "VARCHAR", "text"),
            ("name", "VARCHAR", "text"),
            ("ok", "BOOLEAN", "bool"),
        ]),
        checks,
    ))
}

async fn api_pool_reuse(settings: &Settings, spec: &Value) -> Result<(Value, Vec<Value>), NzError> {
    let max_size = spec
        .get("max_size")
        .and_then(Value::as_u64)
        .map(|value| value.max(1) as usize)
        .unwrap_or(1);
    let config = connection_config(settings);
    let mut pool_config = PoolConfig::new(config);
    pool_config.max = max_size;
    pool_config.min = 0;
    let pool = Pool::connect(pool_config).await?;

    let first = pool.get().await?;
    let first_rows = first.query("SELECT CURRENT_SID AS value", &[]).await?;
    let first_sid: i32 = first_rows[0].try_get(0usize)?;
    first.release().await;

    let second = pool.get().await?;
    let second_rows = second.query("SELECT CURRENT_SID AS value", &[]).await?;
    let second_sid: i32 = second_rows[0].try_get(0usize)?;
    second.release().await;

    let within_limit = pool.total_count().await <= max_size;
    pool.close().await;

    Ok((
        api_columns(&[("check", "VARCHAR", "text"), ("ok", "BOOLEAN", "bool")]),
        vec![
            json!(["same_session", first_sid == second_sid]),
            json!(["stats_within_limit", within_limit]),
        ],
    ))
}

async fn api_external_roundtrip(
    client: &Client,
    settings: &Settings,
    spec: &Value,
) -> Result<(Value, Vec<Value>), NzError> {
    let schema = spec
        .get("schema")
        .and_then(Value::as_str)
        .unwrap_or(settings.schema.as_str())
        .to_uppercase();
    let source = format!("{schema}.CASE_EXT_SRC");
    let destination = format!("{schema}.CASE_EXT_DST");
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    let directory = std::env::temp_dir().join(format!(
        "nz-conformance-external-{}-{unique}",
        std::process::id()
    ));
    fs::create_dir_all(&directory)?;
    let sql_directory = directory.to_string_lossy().replace('\\', "/");
    let external_file = format!("{sql_directory}/export.txt");

    let outcome: Result<Vec<Value>, NzError> = async {
        let statements = [
            format!("DROP TABLE {source} IF EXISTS"),
            format!("DROP TABLE {destination} IF EXISTS"),
            format!("CREATE TABLE {source} (ID INTEGER, LABEL VARCHAR(20)) DISTRIBUTE ON (ID)"),
            format!("INSERT INTO {source} VALUES (1, 'one')"),
            format!("INSERT INTO {source} VALUES (2, 'two')"),
            format!(
                "CREATE EXTERNAL TABLE '{external_file}' USING (REMOTESOURCE 'jdbc' DELIMITER '|' LOGDIR '{sql_directory}') AS SELECT * FROM {source}"
            ),
            format!("CREATE TABLE {destination} (ID INTEGER, LABEL VARCHAR(20)) DISTRIBUTE ON (ID)"),
            format!(
                "INSERT INTO {destination} SELECT * FROM EXTERNAL '{external_file}' USING (REMOTESOURCE 'jdbc' DELIMITER '|' LOGDIR '{sql_directory}')"
            ),
        ];
        for statement in statements {
            client.execute(&statement, &[]).await?;
        }
        let rows = client
            .query(
                &format!(
                    "SELECT COUNT(*)::INTEGER AS row_count, COALESCE(SUM(ID), 0)::INTEGER AS checksum FROM {destination}"
                ),
                &[],
            )
            .await?;
        let count: i32 = rows[0].try_get(0usize)?;
        let checksum: i32 = rows[0].try_get(1usize)?;
        Ok(vec![json!([count.to_string(), checksum.to_string()])])
    }
    .await;

    for statement in [
        format!("DROP TABLE {source} IF EXISTS"),
        format!("DROP TABLE {destination} IF EXISTS"),
    ] {
        let _ = client.execute(&statement, &[]).await;
    }
    let _ = fs::remove_dir_all(&directory);

    Ok((
        api_columns(&[
            ("row_count", "INTEGER", "int4"),
            ("checksum", "INTEGER", "int4"),
        ]),
        outcome?,
    ))
}

async fn api_tls_fail_closed(settings: &Settings) -> Result<(Value, Vec<Value>), NzError> {
    let mut config = connection_config(settings);
    config.security_level = SecurityLevel::OnlySecuredSession;
    let connection = Client::connect(&config).await?;
    connection.close().await?;
    Ok((
        api_columns(&[("check", "VARCHAR", "text")]),
        vec![json!(["unexpected_tls_success"])],
    ))
}

async fn execute_api_case(client: &Client, settings: &Settings, case: &Value) -> Value {
    let started = std::time::Instant::now();
    let case_id = case
        .get("id")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let operation = case
        .get("api")
        .and_then(|api| api.get("operation"))
        .and_then(Value::as_str)
        .unwrap_or("");
    let api_spec = case.get("api").cloned().unwrap_or(Value::Null);
    let mut result = json!({
        "case_id": case_id,
        "status": "pass",
        "duration_ms": 0.0,
        "columns": [],
        "rows": [],
        "row_count": 0,
        "result_sets": [],
        "error": null,
        "profile": case.get("profile").and_then(Value::as_str).unwrap_or("full"),
    });

    let outcome: Result<(Value, Vec<Value>), NzError> = async {
        for statement in case
            .get("setup")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
        {
            let statement = render_sql(statement, &settings.schema);
            if let Err(error) = client.execute(&statement, &[]).await {
                if !error.to_string().to_lowercase().contains("does not exist") {
                    return Err(error);
                }
            }
        }

        match operation {
            "metadata_ddl" => api_metadata_ddl(client, settings, &api_spec).await,
            "pool_reuse" => api_pool_reuse(settings, &api_spec).await,
            "external_roundtrip" => api_external_roundtrip(client, settings, &api_spec).await,
            "tls_fail_closed" => api_tls_fail_closed(settings).await,
            other => Err(NzError::Config(format!(
                "unsupported api operation {other}"
            ))),
        }
    }
    .await;

    match outcome {
        Ok((columns, rows)) => {
            result["columns"] = columns;
            result["row_count"] = json!(rows.len());
            result["rows"] = Value::Array(rows.clone());
            result["result_sets"] = json!([{
                "columns": result["columns"].clone(),
                "rows": rows,
                "row_count": result["row_count"].clone(),
            }]);
        }
        Err(error) => {
            result["status"] = json!("error");
            result["error"] = error_json(&error);
        }
    }

    for statement in case
        .get("cleanup")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
    {
        let statement = render_sql(statement, &settings.schema);
        if let Err(error) = client.execute(&statement, &[]).await {
            eprintln!("cleanup warning [{case_id}]: {error}");
        }
    }

    result["duration_ms"] = json!(started.elapsed().as_secs_f64() * 1000.0);
    result
}

async fn execute_case(client: &Client, settings: &Settings, case: &Value) -> Value {
    let started = std::time::Instant::now();
    let case_id = case
        .get("id")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let sql = render_sql(
        case.get("sql").and_then(Value::as_str).unwrap_or(""),
        &settings.schema,
    );
    let mut result = json!({
        "case_id": case_id,
        "status": "pass",
        "duration_ms": 0.0,
        "columns": [],
        "rows": [],
        "row_count": 0,
        "result_sets": [],
        "error": null,
        "profile": case.get("profile").and_then(Value::as_str).unwrap_or("full"),
    });

    let setup: Vec<String> = case
        .get("setup")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(|statement| render_sql(statement, &settings.schema))
                .collect()
        })
        .unwrap_or_default();
    for statement in setup {
        if let Err(error) = client.execute(&statement, &[]).await {
            if !error.to_string().to_lowercase().contains("does not exist") {
                result["status"] = json!("error");
                result["error"] = error_json(&error);
                result["duration_ms"] = json!(started.elapsed().as_secs_f64() * 1000.0);
                return result;
            }
        }
    }

    let multi = case
        .get("multi_statement")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let timeout = case
        .get("timeout")
        .and_then(Value::as_f64)
        .filter(|seconds| *seconds > 0.0)
        .map(Duration::from_secs_f64);
    let params = body_params(case);
    let mut sets: Vec<(Vec<ColumnDesc>, Vec<nz_rust::Row>)> = Vec::new();
    let outcome: Result<(), NzError> = async {
        if multi {
            for statement in sql
                .split(';')
                .map(str::trim)
                .filter(|part| !part.is_empty())
            {
                let query_result = client.query_multi(statement, &[]).await?;
                for set in query_result.result_sets {
                    if !set.columns.is_empty() {
                        sets.push((set.columns, set.rows));
                    }
                }
            }
        } else {
            let refs: Vec<&(dyn ToSql + Sync)> = params
                .iter()
                .map(|value| value as &(dyn ToSql + Sync))
                .collect();
            let query_result = match timeout {
                Some(timeout) => {
                    client
                        .query_multi_with_options(
                            &sql,
                            &refs,
                            QueryOptions {
                                timeout: Some(timeout),
                            },
                        )
                        .await?
                }
                None => client.query_multi(&sql, &refs).await?,
            };
            for set in query_result.result_sets {
                if !set.columns.is_empty() {
                    sets.push((set.columns, set.rows));
                }
            }
        }
        Ok(())
    }
    .await;

    if let Err(error) = outcome {
        result["status"] = json!("error");
        result["error"] = error_json(&error);
    } else {
        let primary_columns = sets
            .first()
            .map(|(columns, _)| columns.clone())
            .unwrap_or_default();
        let primary_rows = sets.first().map(|(_, rows)| rows.as_slice()).unwrap_or(&[]);
        result["columns"] = Value::Array(columns_json(&primary_columns));
        result["rows"] = Value::Array(rows_json(primary_rows, &primary_columns));
        result["row_count"] = json!(primary_rows.len());
        result["result_sets"] = Value::Array(
            sets.iter()
                .map(|(columns, rows)| {
                    json!({
                        "columns": columns_json(columns),
                        "rows": rows_json(rows, columns),
                        "row_count": rows.len(),
                    })
                })
                .collect(),
        );
    }

    let cleanup: Vec<String> = case
        .get("cleanup")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(|statement| render_sql(statement, &settings.schema))
                .collect()
        })
        .unwrap_or_default();
    for statement in cleanup {
        if let Err(error) = client.execute(&statement, &[]).await {
            eprintln!("cleanup warning [{case_id}]: {error}");
        }
    }

    result["duration_ms"] = json!(started.elapsed().as_secs_f64() * 1000.0);
    result
}

#[tokio::main]
async fn main() -> ExitCode {
    let mut args = env::args().skip(1);
    let mut profile = String::new();
    let mut filters = Vec::new();
    let mut cases_dir = workspace_root().join("conformance").join("cases");
    let mut output: Option<String> = None;
    let mut fail_fast = false;
    let mut env_file: Option<String> = None;

    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--profile" => profile = args.next().unwrap_or_default(),
            "--case" => filters.push(args.next().unwrap_or_default()),
            "--cases-dir" => cases_dir = PathBuf::from(args.next().unwrap_or_default()),
            "--out" => output = args.next(),
            "--env" => env_file = args.next(),
            "--fail-fast" => fail_fast = true,
            other => {
                eprintln!("rust runner: unknown argument {other}");
                return ExitCode::from(2);
            }
        }
    }
    if profile.is_empty() {
        eprintln!("rust runner: --profile is required");
        return ExitCode::from(2);
    }
    if let Some(path) = env_file {
        if let Ok(text) = fs::read_to_string(path) {
            for line in text.lines() {
                let line = line.trim();
                if line.is_empty() || line.starts_with('#') {
                    continue;
                }
                if let Some((key, value)) = line.split_once('=') {
                    if env::var(key.trim()).is_err() {
                        env::set_var(
                            key.trim(),
                            value.trim().trim_matches('\'').trim_matches('"'),
                        );
                    }
                }
            }
        }
    }

    let settings = match settings() {
        Ok(settings) => settings,
        Err(message) => {
            eprintln!("rust runner: {message}");
            return ExitCode::from(2);
        }
    };
    let cases = match load_cases(&cases_dir) {
        Ok(cases) => select_cases(cases, &profile, &filters),
        Err(message) => {
            eprintln!("rust runner: {message}");
            return ExitCode::from(2);
        }
    };
    if cases.is_empty() {
        eprintln!("rust runner: no cases selected");
        return ExitCode::from(2);
    }

    let config = connection_config(&settings);
    let client = match Client::connect(&config).await {
        Ok(client) => client,
        Err(error) => {
            eprintln!("rust runner: connection failed: {error}");
            return ExitCode::from(2);
        }
    };

    let mut writer: Box<dyn Write> = match output {
        Some(path) => match fs::File::create(path) {
            Ok(file) => Box::new(std::io::BufWriter::new(file)),
            Err(error) => {
                eprintln!("rust runner: cannot create output: {error}");
                return ExitCode::from(2);
            }
        },
        None => Box::new(std::io::stdout()),
    };

    let mut failures = 0usize;
    for case in cases {
        let result = if case.get("api").is_some() {
            execute_api_case(&client, &settings, &case).await
        } else {
            execute_case(&client, &settings, &case).await
        };
        let line = serde_json::to_string(&result).unwrap_or_else(|_| "{}".into());
        if writeln!(writer, "{line}").is_err() {
            eprintln!("rust runner: output write failed");
            return ExitCode::from(3);
        }
        let _ = writer.flush();
        if result.get("status").and_then(Value::as_str) != Some("pass") {
            failures += 1;
            if fail_fast {
                break;
            }
        }
    }
    let _ = client.close().await;
    if failures == 0 {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(1)
    }
}
