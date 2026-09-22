//! Dump Netezza query results to a plain-text file.
//!
//! Quick way to confirm the Rust driver end to end: connect, run a query and
//! write every returned row to a tab-separated `.txt` file.
//!
//! ## Usage
//!
//! ```text
//! # real appliance (credentials via env or a connection URI)
//! NZ_HOST=nz-host NZ_DATABASE=JUST_DATA NZ_USER=admin NZ_PASSWORD=secret \
//!   cargo run -p nz_rust --example dump_to_txt -- --out result.txt \
//!     --sql "SELECT 1 AS ONE, 'rust' AS NAME"
//!
//! # or a full URI
//! cargo run -p nz_rust --example dump_to_txt -- \
//!   --conn "netezza://admin:secret@nz-host:5480/JUST_DATA" \
//!   --sql "SELECT * FROM JUST_DATA..DIMACCOUNT" --out dimaccount.txt
//!
//! # offline smoke check (no appliance needed): writes a synthetic result
//! cargo run -p nz_rust --example dump_to_txt -- --demo --out demo.txt
//! ```
//!
//! Environment variables used when `--conn` is absent:
//! `NZ_HOST`, `NZ_PORT` (default 5480), `NZ_DATABASE`, `NZ_USER`,
//! `NZ_PASSWORD`.

use nz_rust::connection::{QueryResult, ResultSet, Row};
use nz_rust::tuple_desc::ColumnDesc;
use nz_rust::types::value::NzValue;
use nz_rust::{write_result_to_txt, NzConnection, NzConnectionConfig};
use std::fs::File;
use std::io::BufWriter;
use std::process::exit;

struct Args {
    conn: Option<String>,
    sql: String,
    out: String,
    header: bool,
    demo: bool,
}

fn parse_args() -> Args {
    let mut args = Args {
        conn: None,
        sql: "SELECT 1 AS ONE, 'rust' AS NAME".into(),
        out: "netezza_dump.txt".into(),
        header: true,
        demo: false,
    };
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--conn" => args.conn = it.next(),
            "--sql" => {
                if let Some(v) = it.next() {
                    args.sql = v;
                }
            }
            "--out" => {
                if let Some(v) = it.next() {
                    args.out = v;
                }
            }
            "--no-header" => args.header = false,
            "--demo" => args.demo = true,
            "-h" | "--help" => {
                println!("dump_to_txt [--conn URI | env NZ_*] [--sql SQL] [--out FILE] [--no-header] [--demo]");
                exit(0);
            }
            other => {
                eprintln!("unknown argument: {other}");
                exit(2);
            }
        }
    }
    args
}

fn config_from_env() -> Result<NzConnectionConfig, String> {
    let host = std::env::var("NZ_HOST").map_err(|_| "NZ_HOST is not set".to_string())?;
    let database =
        std::env::var("NZ_DATABASE").map_err(|_| "NZ_DATABASE is not set".to_string())?;
    let user = std::env::var("NZ_USER").map_err(|_| "NZ_USER is not set".to_string())?;
    let password = std::env::var("NZ_PASSWORD").unwrap_or_default();
    let port = std::env::var("NZ_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(5480);
    Ok(NzConnectionConfig {
        host,
        port,
        database,
        user,
        password,
        ..Default::default()
    })
}

/// Synthetic two-row result used by `--demo` so the writer can be verified
/// without an appliance.
fn demo_result() -> QueryResult {
    let columns = vec![
        ColumnDesc {
            name: "ID".into(),
            type_oid: 23,
            type_len: 4,
            type_mod: -1,
            format: 0,
        },
        ColumnDesc {
            name: "NAME".into(),
            type_oid: 1043,
            type_len: -1,
            type_mod: -1,
            format: 0,
        },
        ColumnDesc {
            name: "AMOUNT".into(),
            type_oid: 1700,
            type_len: -1,
            type_mod: -1,
            format: 0,
        },
        ColumnDesc {
            name: "NOTE".into(),
            type_oid: 1043,
            type_len: -1,
            type_mod: -1,
            format: 0,
        },
    ];
    let rows = vec![
        Row::new(
            columns.clone(),
            vec![
                NzValue::Int4(1),
                NzValue::Text("alice".into()),
                NzValue::Numeric("1234.5600".into()),
                NzValue::Null,
            ],
        ),
        Row::new(
            columns.clone(),
            vec![
                NzValue::Int4(2),
                NzValue::Text("bob\tjr".into()),
                NzValue::Numeric("-0.10".into()),
                NzValue::Text("multi\nline".into()),
            ],
        ),
    ];
    QueryResult {
        result_sets: vec![ResultSet::new(columns, rows)],
        rows_affected: 2,
        notices: vec![],
    }
}

fn main() {
    let args = parse_args();

    let result = if args.demo {
        demo_result()
    } else {
        let mut conn = match &args.conn {
            Some(uri) => match NzConnection::connect_with_str(uri) {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("connect failed: {e}");
                    exit(1);
                }
            },
            None => {
                let cfg = match config_from_env() {
                    Ok(c) => c,
                    Err(e) => {
                        eprintln!(
                            "{e} (pass --conn or set NZ_HOST/NZ_DATABASE/NZ_USER/NZ_PASSWORD)"
                        );
                        exit(1);
                    }
                };
                match NzConnection::connect(&cfg) {
                    Ok(c) => c,
                    Err(e) => {
                        eprintln!("connect failed: {e}");
                        exit(1);
                    }
                }
            }
        };
        match conn.query(&args.sql, &[]) {
            Ok(r) => r,
            Err(e) => {
                eprintln!("query failed: {e}");
                exit(1);
            }
        }
    };

    let file = match File::create(&args.out) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("cannot create {}: {e}", args.out);
            exit(1);
        }
    };
    let mut writer = BufWriter::new(file);
    if let Err(e) = write_result_to_txt(&mut writer, &result, args.header) {
        eprintln!("write failed: {e}");
        exit(1);
    }

    println!(
        "wrote {} row(s) in {} result set(s) to {}",
        result.row_count(),
        result.result_sets.len(),
        args.out
    );
}
