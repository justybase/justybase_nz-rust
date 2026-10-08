//! Capture golden wire fixtures from a real Netezza appliance.
//!
//! The tool starts a loopback TCP proxy, points a native `Client` at it and
//! runs a fixed list of **synthetic** statements (see `fixtures()`); the
//! proxy forwards the connection to the appliance and records, for every
//! statement, the exact bytes the appliance answered with, from the first
//! response byte through `ReadyForQuery`.
//!
//! What is never recorded: anything client→server (so no credentials), the
//! handshake and authentication exchange, `BackendKeyData`, and anything the
//! appliance sends outside a statement response. Before a file is written the
//! recorded bytes are scanned for the user name, password and host; the tool
//! aborts instead of writing a fixture that contains one.
//!
//! Format (documented in `tests/fixtures/wire/README.md`):
//! `b"NZWIRE01"`, `u32 LE record count`, then per record `u32 LE sql length`,
//! the SQL bytes, `u32 LE response length`, the response bytes.
//!
//! ```text
//! NZ_DEV_HOST=… NZ_DEV_USER=… NZ_DEV_PASSWORD=… NZ_DEV_DB=… \
//!   cargo run --features compat --example capture_wire_fixtures -- --out tests/fixtures/wire
//! ```
//!
//! Only plaintext sessions can be captured; a server that insists on TLS is
//! reported as an error.

use nz_rust::{Client, NzConnectionConfig, SecurityLevel};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const MAGIC: &[u8; 8] = b"NZWIRE01";
const MAX_FRAME: usize = 10_000_000;

type Record = (String, Vec<u8>);

/// One fixture file: its name and the statements to run, in order. Statements
/// listed in `tolerate_errors` are expected to fail on the server.
struct Fixture {
    name: &'static str,
    statements: Vec<String>,
}

fn int_columns_sql(n: usize) -> (String, String) {
    // Alternating NULL / non-NULL across `n` columns, mixing int and varchar.
    let cells: Vec<String> = (0..n)
        .map(|i| match (i % 2, i % 3 == 2) {
            (0, false) => format!("CAST(NULL AS INTEGER) AS C{i}"),
            (0, true) => format!("CAST(NULL AS VARCHAR(8)) AS C{i}"),
            (_, false) => format!("CAST({i} AS INTEGER) AS C{i}"),
            (_, true) => format!("CAST('v{i}' AS VARCHAR(8)) AS C{i}"),
        })
        .collect();
    let defs: Vec<String> = (0..n)
        .map(|i| format!("C{i} {}", if i % 3 == 2 { "VARCHAR(8)" } else { "INTEGER" }))
        .collect();
    (cells.join(", "), defs.join(", "))
}

fn fixtures() -> Vec<Fixture> {
    let (null_cells, null_defs) = int_columns_sql(33);
    let null_values: Vec<String> = (0..33)
        .map(|i| match (i % 2, i % 3 == 2) {
            (0, _) => "NULL".to_string(),
            (_, false) => format!("{i}"),
            (_, true) => format!("'v{i}'"),
        })
        .collect();
    let numeric_types = [(1, 0), (18, 0), (38, 0), (10, 4), (38, 10), (38, 38)];
    let mut numeric = Vec::new();
    for (index, (p, s)) in numeric_types.iter().enumerate() {
        let table = format!("RUST_WIRE_NUM{index}");
        let values: Vec<&str> = match (p, s) {
            (1, 0) => vec!["0", "9", "-9"],
            (18, 0) => vec!["0", "999999999999999999", "-999999999999999999"],
            (38, 0) => vec!["0", "99999999999999999999999999999999999999"],
            (10, 4) => vec!["0", "3.1400", "-0.0001", "999999.9999"],
            (38, 10) => vec!["1.0000000001", "-12345678901234567890123456.7890123456"],
            _ => vec!["0.99999999999999999999999999999999999999", "-0.5"],
        };
        numeric.push(format!(
            "CREATE TEMP TABLE {table} (ID INTEGER, V NUMERIC({p},{s})) DISTRIBUTE ON RANDOM"
        ));
        for (id, value) in values.iter().enumerate() {
            numeric.push(format!(
                "INSERT INTO {table} VALUES ({id}, CAST('{value}' AS NUMERIC({p},{s})))"
            ));
        }
        numeric.push(format!("SELECT V FROM {table} ORDER BY ID"));
        let literals: Vec<String> = values
            .iter()
            .map(|v| format!("CAST('{v}' AS NUMERIC({p},{s}))"))
            .collect();
        numeric.push(format!("SELECT {}", literals.join(", ")));
    }

    let mut varchar = vec![
        "CREATE TEMP TABLE RUST_WIRE_VC (ID INTEGER, V VARCHAR(64000)) DISTRIBUTE ON RANDOM"
            .to_string(),
    ];
    for length in [32766usize, 32767, 32768] {
        varchar.push(format!(
            "INSERT INTO RUST_WIRE_VC VALUES ({length}, 'A' || REPEAT('x', {}) || 'Z')",
            length - 2
        ));
        varchar.push(format!(
            "SELECT CAST('A' || REPEAT('x', {}) || 'Z' AS VARCHAR(64000))",
            length - 2
        ));
    }
    varchar.push("SELECT ID, V FROM RUST_WIRE_VC ORDER BY ID".to_string());

    vec![
        Fixture {
            name: "text_types",
            statements: vec![
                "SELECT CAST(-2147483648 AS INTEGER) AS I, CAST('Zażółć gęślą jaźń' AS NVARCHAR(40)) AS V, \
                 CAST('3.1400' AS NUMERIC(10,4)) AS N, CAST('2024-02-29' AS DATE) AS D, \
                 CAST('2024-02-29 12:34:56.123456' AS TIMESTAMP) AS TS, TRUE AS B, \
                 CAST(1.5 AS DOUBLE PRECISION) AS F, CAST('12:34:56' AS TIME) AS T"
                    .to_string(),
            ],
        },
        Fixture {
            name: "dbos_types",
            statements: vec![
                "CREATE TEMP TABLE RUST_WIRE_T (I INTEGER, V NVARCHAR(40), N NUMERIC(10,4), D DATE, \
                 TS TIMESTAMP, B BOOLEAN, F DOUBLE PRECISION, T TIME, BI BIGINT, SI SMALLINT, \
                 BY BYTEINT) DISTRIBUTE ON RANDOM"
                    .to_string(),
                "INSERT INTO RUST_WIRE_T VALUES (-2147483648, 'Zażółć gęślą jaźń', 3.1400, '2024-02-29', \
                 '2024-02-29 12:34:56.123456', TRUE, 1.5, '12:34:56', -9223372036854775807, -32768, -128)"
                    .to_string(),
                "INSERT INTO RUST_WIRE_T VALUES (2147483647, '', -0.0001, '0001-01-01', \
                 '1999-12-31 23:59:59.999999', FALSE, -2.5, '00:00:00', 9223372036854775807, 32767, 127)"
                    .to_string(),
                "SELECT I, V, N, D, TS, B, F, T, BI, SI, BY FROM RUST_WIRE_T ORDER BY I".to_string(),
            ],
        },
        Fixture {
            name: "null_matrix",
            statements: vec![
                format!("SELECT {null_cells}"),
                format!("CREATE TEMP TABLE RUST_WIRE_N ({null_defs}) DISTRIBUTE ON RANDOM"),
                format!("INSERT INTO RUST_WIRE_N VALUES ({})", null_values.join(", ")),
                "SELECT * FROM RUST_WIRE_N".to_string(),
            ],
        },
        Fixture {
            name: "numeric_matrix",
            statements: numeric,
        },
        Fixture {
            name: "varchar_32767",
            statements: varchar,
        },
        Fixture {
            name: "multi_result",
            statements: vec![
                "SELECT 1 AS A; SELECT 'x' AS B, 2 AS C; SELECT CAST(NULL AS INTEGER) AS D".to_string(),
            ],
        },
        Fixture {
            name: "notice",
            statements: vec![
                "ROLLBACK".to_string(),
                "COMMIT".to_string(),
                "CREATE TEMP TABLE RUST_WIRE_NT (A INTEGER)".to_string(),
                "SELECT 1 AS AFTER_NOTICE".to_string(),
            ],
        },
        Fixture {
            name: "error",
            statements: vec![
                "SELECT FROM FROM".to_string(),
                "SELECT 1/0".to_string(),
                "SELECT 1 AS AFTER_ERROR".to_string(),
            ],
        },
    ]
}

/// Total length of the first complete backend message in `buffer`, `None` when
/// more bytes are needed.
fn next_frame_len(buffer: &[u8]) -> Result<Option<usize>, String> {
    let Some(&kind) = buffer.first() else {
        return Ok(None);
    };
    let length_at = |offset: usize| -> Result<Option<usize>, String> {
        if buffer.len() < offset + 4 {
            return Ok(None);
        }
        let length = i32::from_be_bytes(buffer[offset..offset + 4].try_into().unwrap());
        if !(0..=MAX_FRAME as i32).contains(&length) {
            return Err(format!("implausible frame length {length} for {kind:#04x}"));
        }
        let total = offset + 4 + length as usize;
        Ok((buffer.len() >= total).then_some(total))
    };
    match kind {
        // ReadyForQuery (and its alternate marker) and skippable control bytes.
        b'Z' | b'L' | b'0' | b'A' => Ok((buffer.len() >= 5).then_some(5)),
        // DBOS row: type, 4 skipped, 4 reserved, i32 length, payload.
        b'Y' => length_at(9),
        b'u' | b'U' | b'l' | b'x' | b'e' => Err(format!(
            "external-table message {kind:#04x} is not capturable"
        )),
        // type, 4 skipped, i32 length, payload.
        _ => length_at(5),
    }
}

#[derive(Default)]
struct Shared {
    /// Statement whose response is being recorded.
    active: Option<String>,
    pending: Vec<u8>,
    response: Vec<u8>,
    records: Vec<Record>,
    error: Option<String>,
}

impl Shared {
    fn feed(&mut self, chunk: &[u8]) {
        if self.active.is_none() {
            return;
        }
        self.pending.extend_from_slice(chunk);
        loop {
            // NUL padding between messages is not part of a response.
            if self.response.is_empty() {
                let zeros = self.pending.iter().take_while(|&&b| b == 0).count();
                self.pending.drain(..zeros);
            }
            match next_frame_len(&self.pending) {
                Err(message) => {
                    self.error = Some(message);
                    self.active = None;
                    return;
                }
                Ok(None) => return,
                Ok(Some(length)) => {
                    let done = matches!(self.pending[0], b'Z' | b'L');
                    self.response.extend_from_slice(&self.pending[..length]);
                    self.pending.drain(..length);
                    if done {
                        let sql = self.active.take().expect("active statement");
                        self.records.push((sql, std::mem::take(&mut self.response)));
                        self.pending.clear();
                        return;
                    }
                }
            }
        }
    }
}

async fn forward_client_to_server(
    mut client: tokio::net::tcp::OwnedReadHalf,
    mut server: tokio::net::tcp::OwnedWriteHalf,
    shared: Arc<Mutex<Shared>>,
) -> std::io::Result<()> {
    loop {
        let mut first = [0u8; 1];
        if client.read(&mut first).await? == 0 {
            return Ok(());
        }
        let mut message = vec![first[0]];
        if first[0] == b'P' {
            // Query: 'P', i32 command number, SQL, NUL.
            let mut header = [0u8; 4];
            client.read_exact(&mut header).await?;
            message.extend_from_slice(&header);
            let mut sql = Vec::new();
            loop {
                let mut byte = [0u8; 1];
                client.read_exact(&mut byte).await?;
                message.push(byte[0]);
                if byte[0] == 0 {
                    break;
                }
                sql.push(byte[0]);
            }
            // Register the statement before it reaches the appliance.
            shared.lock().unwrap().active = Some(String::from_utf8_lossy(&sql).into_owned());
        } else {
            // Handshake / control frame: i32 BE length that includes itself.
            let mut rest = [0u8; 3];
            client.read_exact(&mut rest).await?;
            message.extend_from_slice(&rest);
            let length = i32::from_be_bytes(message[..4].try_into().unwrap());
            if !(4..=65_536).contains(&length) {
                return Err(std::io::Error::other(format!(
                    "unexpected client frame length {length}"
                )));
            }
            let mut body = vec![0u8; length as usize - 4];
            client.read_exact(&mut body).await?;
            message.extend_from_slice(&body);
        }
        server.write_all(&message).await?;
    }
}

async fn forward_server_to_client(
    mut server: tokio::net::tcp::OwnedReadHalf,
    mut client: tokio::net::tcp::OwnedWriteHalf,
    shared: Arc<Mutex<Shared>>,
) -> std::io::Result<()> {
    let mut buffer = vec![0u8; 64 * 1024];
    loop {
        let n = server.read(&mut buffer).await?;
        if n == 0 {
            return Ok(());
        }
        // Record before forwarding, so the client cannot send its next
        // statement before this response has been closed out.
        shared.lock().unwrap().feed(&buffer[..n]);
        client.write_all(&buffer[..n]).await?;
    }
}

async fn serve_proxy(listener: TcpListener, upstream: String, shared: Arc<Mutex<Shared>>) {
    loop {
        let Ok((client, _)) = listener.accept().await else {
            return;
        };
        let upstream = upstream.clone();
        let shared = shared.clone();
        tokio::spawn(async move {
            let Ok(server) = TcpStream::connect(&upstream).await else {
                return;
            };
            let (client_read, client_write) = client.into_split();
            let (server_read, server_write) = server.into_split();
            let up = forward_client_to_server(client_read, server_write, shared.clone());
            let down = forward_server_to_client(server_read, client_write, shared);
            let _ = tokio::join!(up, down);
        });
    }
}

fn require_env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| {
        eprintln!("capture_wire_fixtures: {name} is not set");
        std::process::exit(2)
    })
}

fn write_fixture(
    path: &std::path::Path,
    records: &[Record],
    secrets: &[String],
) -> Result<(), String> {
    for (sql, response) in records {
        for secret in secrets.iter().filter(|s| s.len() >= 3) {
            let needle = secret.to_lowercase().into_bytes();
            let contains = |haystack: &[u8]| {
                haystack
                    .windows(needle.len())
                    .any(|w| w.eq_ignore_ascii_case(&needle))
            };
            if contains(response) || contains(sql.as_bytes()) {
                return Err(format!(
                    "{}: a recorded statement/response contains a configuration secret; refusing to write it",
                    path.display()
                ));
            }
        }
    }
    let mut out = Vec::new();
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&(records.len() as u32).to_le_bytes());
    for (sql, response) in records {
        out.extend_from_slice(&(sql.len() as u32).to_le_bytes());
        out.extend_from_slice(sql.as_bytes());
        out.extend_from_slice(&(response.len() as u32).to_le_bytes());
        out.extend_from_slice(response);
    }
    std::fs::write(path, out).map_err(|e| e.to_string())
}

#[tokio::main]
async fn main() {
    let mut out_dir = PathBuf::from("tests/fixtures/wire");
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--out" => out_dir = PathBuf::from(args.next().expect("--out needs a directory")),
            other => {
                eprintln!("capture_wire_fixtures: unknown argument {other}");
                std::process::exit(2);
            }
        }
    }
    let host = require_env("NZ_DEV_HOST");
    let user = require_env("NZ_DEV_USER");
    let password = require_env("NZ_DEV_PASSWORD");
    let database = std::env::var("NZ_DEV_DB")
        .or_else(|_| std::env::var("NZ_DEV_DATABASE"))
        .unwrap_or_else(|_| require_env("NZ_DEV_DB"));
    let port: u16 = std::env::var("NZ_DEV_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(5480);
    std::fs::create_dir_all(&out_dir).expect("create output directory");
    let secrets = vec![user.clone(), password.clone(), host.clone()];

    for fixture in fixtures() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind proxy");
        let proxy_port = listener.local_addr().unwrap().port();
        let shared = Arc::new(Mutex::new(Shared::default()));
        let proxy = tokio::spawn(serve_proxy(
            listener,
            format!("{host}:{port}"),
            shared.clone(),
        ));
        let config = NzConnectionConfig {
            host: "127.0.0.1".into(),
            port: proxy_port,
            database: database.clone(),
            user: user.clone(),
            password: password.clone(),
            security_level: SecurityLevel::OnlyUnsecuredSession,
            ..Default::default()
        };
        let client = Client::connect(&config).await.unwrap_or_else(|error| {
            eprintln!(
                "capture_wire_fixtures: cannot connect through the capture proxy \
                 (the appliance may require TLS, which cannot be captured): {error}"
            );
            std::process::exit(1)
        });
        for statement in &fixture.statements {
            // Server-side errors are expected for the `error` fixture; the
            // response bytes are what matter.
            let _ = client.query_multi(statement, &[]).await;
            if let Some(message) = shared.lock().unwrap().error.take() {
                eprintln!("capture_wire_fixtures: {}: {message}", fixture.name);
                std::process::exit(1);
            }
        }
        let _ = client.close().await;
        proxy.abort();
        let records = std::mem::take(&mut shared.lock().unwrap().records);
        if records.len() != fixture.statements.len() {
            eprintln!(
                "capture_wire_fixtures: {}: recorded {} of {} responses",
                fixture.name,
                records.len(),
                fixture.statements.len()
            );
            std::process::exit(1);
        }
        let path = out_dir.join(format!("{}.bin", fixture.name));
        if let Err(message) = write_fixture(&path, &records, &secrets) {
            eprintln!("capture_wire_fixtures: {message}");
            std::process::exit(1);
        }
        let bytes: usize = records.iter().map(|(_, r)| r.len()).sum();
        println!(
            "{}: {} statements, {} response bytes",
            path.display(),
            records.len(),
            bytes
        );
    }
}
