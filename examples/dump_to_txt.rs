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
//!   cargo run -p nz_rust --features compat \
//!     --example dump_to_txt -- --out result.txt \
//!     --sql "SELECT 1 AS ONE, 'rust' AS NAME"
//!
//! # or a full URI
//! cargo run -p nz_rust --features compat \
//!   --example dump_to_txt -- \
//!   --conn "netezza://admin:secret@nz-host:5480/JUST_DATA" \
//!   --sql "SELECT * FROM JUST_DATA..DIMACCOUNT" --out dimaccount.txt
//!
//! # offline smoke check (no appliance needed): writes a synthetic result
//! cargo run -p nz_rust --features compat \
//!   --example dump_to_txt -- --demo --out demo.txt
//! ```
//!
//! Environment variables used when `--conn` is absent:
//! `NZ_HOST`, `NZ_PORT` (default 5480), `NZ_DATABASE`, `NZ_USER`,
//! `NZ_PASSWORD`.

use nz_rust::compat::NzConnection;
use nz_rust::tuple_desc::ColumnDesc;
use nz_rust::types::value::NzValue;
use nz_rust::{
    write_result_to_txt, NzConnectionConfig, QueryResult, ResultSet, Row, TextExportSink,
};
use std::fs::File;
use std::io::{BufWriter, Write};
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
            _other => {
                eprintln!("unknown argument (try --help)");
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

    let file = match File::create(&args.out) {
        Ok(file) => file,
        Err(error) => {
            eprintln!("cannot create output: {error}");
            exit(1);
        }
    };
    let mut writer = BufWriter::new(file);
    let (rows, sets) = if args.demo {
        let result = demo_result();
        if let Err(error) = write_result_to_txt(&mut writer, &result, args.header) {
            eprintln!("write failed: {error}");
            exit(1);
        }
        (result.row_count() as u64, result.result_sets.len())
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
        let mut sink = TextExportSink::new(&mut writer, args.header);
        let summary = match conn.execute_stream(&args.sql, &[], &mut sink) {
            Ok(summary) => summary,
            Err(error) => {
                eprintln!("query/export failed: {error}");
                exit(1);
            }
        };
        conn.close();
        (
            summary.result_sets.iter().map(|set| set.row_count).sum(),
            summary.result_sets.len(),
        )
    };
    if let Err(error) = writer.flush() {
        eprintln!("flush failed: {error}");
        exit(1);
    }
    println!(
        "wrote {rows} row(s) in {sets} result set(s) to {}",
        args.out
    );
}
