//! Shared live compatibility runner for the Rust and C# drivers.
//!
//! Usage:
//!   cargo run --example cross_driver_compat -- --manifest FILE --output FILE
//!   cargo run --example cross_driver_compat -- --compare RUST.json CSHARP.json --report FILE

use nz_rust::{NzConnection, NzConnectionConfig, NzError, NzValue, QueryResult};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env;
use std::fs;
use std::path::Path;

#[derive(Debug, Deserialize)]
struct Manifest {
    version: u32,
    cases: Vec<Case>,
}

#[derive(Debug, Deserialize)]
struct Case {
    id: String,
    #[allow(dead_code)]
    category: String,
    mode: String,
    sql: String,
    setup: Option<Vec<String>>,
    cleanup: Option<Vec<String>>,
    #[serde(default)]
    compare_types: bool,
    float_tolerance: Option<f64>,
    #[serde(default)]
    trim_trailing_spaces: bool,
    known_divergence: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct Cell {
    #[serde(rename = "type")]
    kind: String,
    value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ResultSetOut {
    columns: Vec<String>,
    rows: Vec<Vec<Option<Cell>>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CaseResult {
    id: String,
    #[serde(rename = "resultSets")]
    result_sets: Vec<ResultSetOut>,
    affected: Option<i64>,
    error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Report {
    driver: String,
    #[serde(rename = "manifestVersion")]
    manifest_version: u32,
    cases: Vec<CaseResult>,
}

#[derive(Debug, Serialize)]
struct DiffReport {
    matched: usize,
    known_differences: Vec<KnownDifference>,
    mismatches: Vec<Mismatch>,
}

#[derive(Debug, Serialize)]
struct KnownDifference {
    id: String,
    reason: String,
    differences: Vec<String>,
}

#[derive(Debug, Serialize)]
struct Mismatch {
    id: String,
    differences: Vec<String>,
}

fn main() {
    let args: Vec<String> = env::args().skip(1).collect();
    if args.first().map(String::as_str) == Some("--compare") {
        compare(&args[1..]);
    } else {
        run_rust(&args);
    }
}

fn run_rust(args: &[String]) {
    let manifest_path = argument(args, "--manifest").expect("--manifest is required");
    let output_path = argument(args, "--output").unwrap_or_else(|| "target/rust.json".into());
    let manifest: Manifest = read_json(&manifest_path);
    let database = database();
    let mut connection = NzConnection::connect(&config(&database))
        .unwrap_or_else(|error| panic!("Rust compatibility runner could not connect: {error}"));
    let mut results = Vec::with_capacity(manifest.cases.len());

    for case in &manifest.cases {
        let sql = expand(&case.sql, &database);
        let run = (|| {
            for setup in case.setup.as_deref().unwrap_or(&[]) {
                connection.batch_execute(&expand(setup, &database))?;
            }
            if case.mode.eq_ignore_ascii_case("execute") {
                Ok(CaseResult {
                    id: case.id.clone(),
                    result_sets: Vec::new(),
                    affected: Some(connection.execute(&sql, &[])?),
                    error: None,
                })
            } else {
                let query = connection.query(&sql, &[])?;
                Ok(query_result(&case.id, &query))
            }
        })();
        let result = match run {
            Ok(value) => value,
            Err(error) => CaseResult {
                id: case.id.clone(),
                result_sets: Vec::new(),
                affected: None,
                error: Some(error_kind(&error)),
            },
        };

        for cleanup in case.cleanup.as_deref().unwrap_or(&[]) {
            let _ = connection.batch_execute(&expand(cleanup, &database));
        }
        results.push(result);
    }

    write_json(
        &output_path,
        &Report {
            driver: "rust".into(),
            manifest_version: manifest.version,
            cases: results,
        },
    );
    println!("saved {output_path}");
}

fn query_result(id: &str, query: &QueryResult) -> CaseResult {
    CaseResult {
        id: id.into(),
        result_sets: query
            .result_sets
            .iter()
            .map(|set| ResultSetOut {
                columns: set
                    .columns
                    .iter()
                    .map(|column| column.name.clone())
                    .collect(),
                rows: set
                    .rows
                    .iter()
                    .map(|row| row.values().iter().map(encode).collect())
                    .collect(),
            })
            .collect(),
        affected: None,
        error: None,
    }
}

fn encode(value: &NzValue) -> Option<Cell> {
    let (kind, value) = match value {
        NzValue::Null => return None,
        NzValue::Bool(value) => ("bool", value.to_string()),
        NzValue::Int2(value) => ("integer", value.to_string()),
        NzValue::Int4(value) => ("integer", value.to_string()),
        NzValue::Int8(value) => ("integer", value.to_string()),
        NzValue::Float4(_) | NzValue::Float8(_) => ("float", value.to_node_canonical()),
        NzValue::Numeric(value) => ("numeric", value.clone()),
        NzValue::Decimal(value) => ("numeric", value.to_string()),
        NzValue::Text(value) => ("text", value.clone()),
        NzValue::Date(value) => ("datetime", format!("{value} 00:00:00.000000")),
        NzValue::Time(value) => ("timespan", value.clone()),
        NzValue::Timetz(value) => ("datetimeoffset", value.clone()),
        NzValue::Timestamp(value) => ("datetime", normalize_timestamp(value)),
        NzValue::Interval(value) => ("timespan", value.clone()),
        NzValue::Bytea(value) => ("bytes", hex_upper(value)),
    };
    Some(Cell {
        kind: kind.into(),
        value,
    })
}

fn compare(args: &[String]) {
    let rust_path = args.first().expect("--compare requires a Rust report");
    let csharp_path = args.get(1).expect("--compare requires a C# report");
    let report_path =
        argument(args, "--report").unwrap_or_else(|| "target/compatibility-diff.json".into());
    let rust_report: Report = read_json(rust_path);
    let csharp_report: Report = read_json(csharp_path);
    let cases: Manifest = read_json(
        &env::var("NZ_COMPAT_MANIFEST")
            .unwrap_or_else(|_| "benchmarks/netezza_cross_driver/compatibility_cases.json".into()),
    );
    let rules: HashMap<_, _> = cases
        .cases
        .into_iter()
        .map(|case| (case.id.clone(), case))
        .collect();
    let csharp_by_id: HashMap<_, _> = csharp_report
        .cases
        .iter()
        .map(|case| (case.id.clone(), case))
        .collect();
    let mut mismatches = Vec::new();
    let mut known_differences = Vec::new();
    let mut matched = 0;

    for rust_case in &rust_report.cases {
        let Some(csharp_case) = csharp_by_id.get(&rust_case.id) else {
            mismatches.push(Mismatch {
                id: rust_case.id.clone(),
                differences: vec!["case is missing from C# report".into()],
            });
            continue;
        };
        let rule = rules.get(&rust_case.id);
        let differences = compare_case(rust_case, csharp_case, rule);
        if differences.is_empty() {
            matched += 1;
        } else if let Some(reason) = rule.and_then(|case| case.known_divergence.clone()) {
            known_differences.push(KnownDifference {
                id: rust_case.id.clone(),
                reason,
                differences,
            });
        } else {
            mismatches.push(Mismatch {
                id: rust_case.id.clone(),
                differences,
            });
        }
    }

    let report = DiffReport {
        matched,
        known_differences,
        mismatches,
    };
    write_json(&report_path, &report);
    println!(
        "compatibility: {} matched, {} mismatched; report={report_path}",
        report.matched,
        report.mismatches.len()
    );
    for difference in &report.known_differences {
        eprintln!(
            "known divergence {}: {} ({})",
            difference.id,
            difference.reason,
            difference.differences.join("; ")
        );
    }
    if !report.mismatches.is_empty() {
        for mismatch in &report.mismatches {
            eprintln!("{}: {}", mismatch.id, mismatch.differences.join("; "));
        }
        std::process::exit(1);
    }
}

fn compare_case(rust: &CaseResult, csharp: &CaseResult, rule: Option<&Case>) -> Vec<String> {
    let mut differences = Vec::new();
    match (&rust.error, &csharp.error) {
        (Some(_), Some(_)) => return differences,
        (None, None) => {}
        (Some(_), None) => differences.push("Rust returned an error, C# did not".into()),
        (None, Some(_)) => differences.push("C# returned an error, Rust did not".into()),
    }
    if !differences.is_empty() {
        return differences;
    }

    if rust.affected != csharp.affected {
        differences.push(format!(
            "affected rows differ: {:?} != {:?}",
            rust.affected, csharp.affected
        ));
    }
    if rust.result_sets.len() != csharp.result_sets.len() {
        differences.push(format!(
            "result-set count differs: {} != {}",
            rust.result_sets.len(),
            csharp.result_sets.len()
        ));
        return differences;
    }
    let compare_types = rule.map(|case| case.compare_types).unwrap_or(false);
    let tolerance = rule.and_then(|case| case.float_tolerance).unwrap_or(0.0);
    let trim_trailing_spaces = rule.map(|case| case.trim_trailing_spaces).unwrap_or(false);
    for (set_index, (rust_set, csharp_set)) in
        rust.result_sets.iter().zip(&csharp.result_sets).enumerate()
    {
        if rust_set.columns != csharp_set.columns {
            differences.push(format!("set {set_index} columns differ"));
        }
        if rust_set.rows.len() != csharp_set.rows.len() {
            differences.push(format!("set {set_index} row count differs"));
            continue;
        }
        for (row_index, (rust_row, csharp_row)) in
            rust_set.rows.iter().zip(&csharp_set.rows).enumerate()
        {
            if rust_row.len() != csharp_row.len() {
                differences.push(format!("set {set_index} row {row_index} width differs"));
                continue;
            }
            for (column_index, (rust_cell, csharp_cell)) in
                rust_row.iter().zip(csharp_row).enumerate()
            {
                if let Some(difference) = compare_cell(
                    rust_cell.as_ref(),
                    csharp_cell.as_ref(),
                    compare_types,
                    tolerance,
                    trim_trailing_spaces,
                ) {
                    differences.push(format!(
                        "set {set_index} row {row_index} column {column_index}: {difference}"
                    ));
                }
            }
        }
    }
    differences
}

fn compare_cell(
    rust: Option<&Cell>,
    csharp: Option<&Cell>,
    compare_types: bool,
    tolerance: f64,
    trim_trailing_spaces: bool,
) -> Option<String> {
    match (rust, csharp) {
        (None, None) => None,
        (None, Some(_)) => Some("Rust NULL != C# value".into()),
        (Some(_), None) => Some("Rust value != C# NULL".into()),
        (Some(rust), Some(csharp)) => {
            if compare_types && rust.kind != csharp.kind {
                return Some(format!("types {} != {}", rust.kind, csharp.kind));
            }
            if rust.kind == "float" || csharp.kind == "float" {
                let left = rust.value.parse::<f64>();
                let right = csharp.value.parse::<f64>();
                if let (Ok(left), Ok(right)) = (left, right) {
                    let effective_tolerance = tolerance.max(0.000001);
                    if (left - right).abs()
                        <= effective_tolerance * left.abs().max(right.abs()).max(1.0)
                    {
                        return None;
                    }
                }
            }
            let comparison_kind = if rust.kind == "timespan" || csharp.kind == "timespan" {
                "timespan"
            } else if rust.kind == "datetime" || csharp.kind == "datetime" {
                "datetime"
            } else {
                rust.kind.as_str()
            };
            let mut left = canonical_value(comparison_kind, &rust.value);
            let mut right = canonical_value(comparison_kind, &csharp.value);
            if trim_trailing_spaces && comparison_kind == "text" {
                left = left.trim_end_matches(' ').into();
                right = right.trim_end_matches(' ').into();
            }
            (left != right).then(|| format!("values {left:?} != {right:?}"))
        }
    }
}

fn canonical_value(kind: &str, value: &str) -> String {
    match kind {
        "bool" => value.to_ascii_lowercase(),
        "bytes" => value.to_ascii_uppercase(),
        "timespan" => normalize_timespan(value),
        "datetime" => normalize_timestamp(value),
        "datetimeoffset" => normalize_timetz(value),
        _ => value.into(),
    }
}

fn normalize_timetz(value: &str) -> String {
    let separator = value
        .char_indices()
        .skip(1)
        .find(|(_, character)| *character == '+' || *character == '-')
        .map(|(index, _)| index);
    let Some(separator) = separator else {
        return normalize_clock(value);
    };
    format!(
        "{}{}",
        normalize_clock(&value[..separator]),
        &value[separator..]
    )
}

fn normalize_clock(value: &str) -> String {
    let Some((prefix, fraction)) = value.split_once('.') else {
        return value.into();
    };
    let fraction = fraction.trim_end_matches('0');
    if fraction.is_empty() {
        prefix.into()
    } else {
        format!("{prefix}.{fraction}")
    }
}

fn normalize_timestamp(value: &str) -> String {
    let value = value.replace('T', " ").replace('Z', "");
    if let Some((date, time)) = value.split_once(' ') {
        let (clock, fraction) = time.split_once('.').unwrap_or((time, ""));
        let mut fraction = fraction.chars().take(6).collect::<String>();
        while fraction.len() < 6 {
            fraction.push('0');
        }
        format!("{date} {clock}.{fraction}")
    } else {
        format!("{value} 00:00:00.000000")
    }
}

fn normalize_timespan(value: &str) -> String {
    let value = value.replace(" days ", ".");
    if let Some((prefix, fraction)) = value.split_once('.') {
        let trimmed = fraction.trim_end_matches('0');
        if trimmed.is_empty() {
            prefix.to_string()
        } else {
            format!("{prefix}.{trimmed}")
        }
    } else {
        value
    }
}

fn config(database: &str) -> NzConnectionConfig {
    NzConnectionConfig {
        host: required("NZ_DEV_HOST"),
        port: env::var("NZ_DEV_PORT")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(5480),
        database: database.into(),
        user: required("NZ_DEV_USER"),
        password: required("NZ_DEV_PASSWORD"),
        ..Default::default()
    }
}

fn database() -> String {
    env::var("NZ_DEV_DB")
        .or_else(|_| env::var("NZ_DEV_DATABASE"))
        .unwrap_or_else(|_| "JUST_DATA".into())
}

fn required(name: &str) -> String {
    env::var(name).unwrap_or_else(|_| panic!("{name} is required"))
}

fn expand(sql: &str, database: &str) -> String {
    sql.replace("__DB__", database)
}

fn error_kind(error: &NzError) -> String {
    match error {
        NzError::Database(database) => {
            format!("database:{}", database.code.as_deref().unwrap_or("unknown"))
        }
        NzError::Timeout(_) => "timeout".into(),
        NzError::Protocol(_) => "protocol".into(),
        NzError::Io(_) => "io".into(),
        NzError::Config(_) => "config".into(),
        NzError::Unsupported(_) => "unsupported".into(),
        NzError::Closed(_) => "closed".into(),
    }
}

fn hex_upper(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02X}")).collect()
}

fn argument(args: &[String], name: &str) -> Option<String> {
    args.windows(2)
        .find(|pair| pair[0] == name)
        .map(|pair| pair[1].clone())
}

fn read_json<T: for<'de> Deserialize<'de>>(path: &str) -> T {
    serde_json::from_str(
        &fs::read_to_string(path).unwrap_or_else(|error| panic!("read {path}: {error}")),
    )
    .unwrap_or_else(|error| panic!("parse {path}: {error}"))
}

fn write_json<T: Serialize>(path: &str, value: &T) {
    if let Some(parent) = Path::new(path).parent() {
        fs::create_dir_all(parent)
            .unwrap_or_else(|error| panic!("create {}: {error}", parent.display()));
    }
    fs::write(path, serde_json::to_string_pretty(value).unwrap())
        .unwrap_or_else(|error| panic!("write {path}: {error}"));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compatibility_manifest_is_valid_and_has_core_cases() {
        let manifest: Manifest = serde_json::from_str(include_str!(
            "../benchmarks/netezza_cross_driver/compatibility_cases.json"
        ))
        .unwrap();
        assert_eq!(manifest.version, 1);
        assert!(manifest.cases.len() >= 40);
        assert!(manifest
            .cases
            .iter()
            .any(|case| case.id == "multi_statement"));
        assert!(manifest
            .cases
            .iter()
            .any(|case| case.id == "temp_table_round_trip"));
    }

    #[test]
    fn timestamp_normalization_matches_csharp_datetime_precision() {
        assert_eq!(
            normalize_timestamp("2024-01-02T03:04:05.1234567Z"),
            "2024-01-02 03:04:05.123456"
        );
        assert_eq!(
            normalize_timestamp("2024-01-02"),
            "2024-01-02 00:00:00.000000"
        );
    }

    #[test]
    fn timespan_normalization_handles_days_and_trailing_zeroes() {
        assert_eq!(
            normalize_timespan("2 days 03:04:05.1200000"),
            "2.03:04:05.12"
        );
        assert_eq!(normalize_timespan("03:04:05.0000000"), "03:04:05");
    }

    #[test]
    fn comparator_accepts_nulls_and_float_tolerance() {
        assert!(compare_cell(None, None, true, 0.0, false).is_none());
        assert!(compare_cell(
            Some(&Cell {
                kind: "float".into(),
                value: "1.0000001".into(),
            }),
            Some(&Cell {
                kind: "float".into(),
                value: "1.0".into(),
            }),
            true,
            0.000001,
            false,
        )
        .is_none());
    }

    #[test]
    fn comparator_reports_value_and_type_differences() {
        let difference = compare_cell(
            Some(&Cell {
                kind: "integer".into(),
                value: "1".into(),
            }),
            Some(&Cell {
                kind: "text".into(),
                value: "1".into(),
            }),
            true,
            0.0,
            false,
        )
        .unwrap();
        assert!(difference.contains("types"));
    }
}
