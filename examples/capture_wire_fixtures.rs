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
//! recorded bytes are scanned for the password and host name; the tool aborts
//! instead of writing a fixture that contains one. (The user name is not
//! scanned: catalog fixtures legitimately contain role names such as `ADMIN`.)
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
//! `--suite encoding|metadata|ddl|types|errors|all` limits the capture and
//! writes `manifest.json` with each fixture's suite, SHA-256 and statements.
//!
//! Only plaintext sessions can be captured; a server that insists on TLS is
//! reported as an error.

use nz_rust::{Client, NzConnectionConfig, SecurityLevel};
use sha2::{Digest, Sha256};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const MAGIC: &[u8; 8] = b"NZWIRE01";
const MAX_FRAME: usize = 10_000_000;

type Record = (String, Vec<u8>);

/// One fixture file: its name, capture suite, and the statements to run, in
/// order.
struct Fixture {
    name: &'static str,
    suite: &'static str,
    statements: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Suite {
    Encoding,
    Metadata,
    Ddl,
    Types,
    Errors,
    All,
}

impl Suite {
    fn parse(value: &str) -> Option<Self> {
        match value {
            "encoding" => Some(Self::Encoding),
            "metadata" => Some(Self::Metadata),
            "ddl" => Some(Self::Ddl),
            "types" => Some(Self::Types),
            "errors" => Some(Self::Errors),
            "all" => Some(Self::All),
            _ => None,
        }
    }

    fn includes(self, fixture: &Fixture) -> bool {
        self == Self::All || self == Self::parse(fixture.suite).expect("known suite")
    }
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
    const META_SCHEMA: &str = "RUST_WIRE_META";
    const DDL_SCHEMA: &str = "RUST_WIRE_DDL";
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
            suite: "encoding",
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
            suite: "types",
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
            suite: "types",
            statements: vec![
                format!("SELECT {null_cells}"),
                format!("CREATE TEMP TABLE RUST_WIRE_N ({null_defs}) DISTRIBUTE ON RANDOM"),
                format!("INSERT INTO RUST_WIRE_N VALUES ({})", null_values.join(", ")),
                "SELECT * FROM RUST_WIRE_N".to_string(),
            ],
        },
        Fixture {
            name: "numeric_matrix",
            suite: "types",
            statements: numeric,
        },
        Fixture {
            name: "varchar_32767",
            suite: "encoding",
            statements: varchar,
        },
        Fixture {
            name: "multi_result",
            suite: "types",
            statements: vec![
                "SELECT 1 AS A; SELECT 'x' AS B, 2 AS C; SELECT CAST(NULL AS INTEGER) AS D".to_string(),
            ],
        },
        Fixture {
            name: "notice",
            suite: "errors",
            statements: vec![
                "ROLLBACK".to_string(),
                "COMMIT".to_string(),
                "CREATE TEMP TABLE RUST_WIRE_NT (A INTEGER)".to_string(),
                "SELECT 1 AS AFTER_NOTICE".to_string(),
            ],
        },
        Fixture {
            name: "error",
            suite: "errors",
            statements: vec![
                "SELECT FROM FROM".to_string(),
                "SELECT 1/0".to_string(),
                "SELECT 1 AS AFTER_ERROR".to_string(),
            ],
        },
        Fixture {
            name: "latin_varchar",
            suite: "encoding",
            statements: vec![
                "CREATE TEMP TABLE RUST_WIRE_LATIN (ID INTEGER, V VARCHAR(64), C CHAR(10), \
                 N NVARCHAR(64)) DISTRIBUTE ON RANDOM"
                    .to_string(),
                "INSERT INTO RUST_WIRE_LATIN VALUES (1, 'ó', 'ó', 'Zażółć gęślą jaźń')".to_string(),
                "INSERT INTO RUST_WIRE_LATIN VALUES (2, 'café', 'café', '¡Hola! ¿Qué tal?')"
                    .to_string(),
                "SELECT ID, V, C, N FROM RUST_WIRE_LATIN ORDER BY ID".to_string(),
            ],
        },
        Fixture {
            name: "text_encoding",
            suite: "encoding",
            statements: vec![
                "SELECT CAST('ó' AS VARCHAR(10)) AS V, \
                 CAST('Zażółć gęślą jaźń' AS NVARCHAR(40)) AS N, \
                 CAST('ab' AS CHAR(5)) AS C, CAST('ab' AS NCHAR(5)) AS NC"
                    .to_string(),
            ],
        },
        Fixture {
            name: "error_details",
            suite: "errors",
            statements: vec![
                "SELECT * FROM RUST_WIRE_MISSING_TABLE".to_string(),
                "SELECT CAST('not-a-number' AS INTEGER)".to_string(),
                "SELECT 1/0".to_string(),
                "SELECT 1 AS AFTER_ERROR".to_string(),
            ],
        },
        Fixture {
            name: "temporal_boundaries",
            suite: "types",
            statements: vec![
                "CREATE TEMP TABLE RUST_WIRE_TEMPORAL (ID INTEGER, D DATE, TS TIMESTAMP, \
                 T TIME, TZ TIMETZ, IV INTERVAL) DISTRIBUTE ON RANDOM"
                    .to_string(),
                "INSERT INTO RUST_WIRE_TEMPORAL VALUES (1, '0001-01-01', \
                 '0001-01-01 00:00:00', '00:00:00', '12:34:56+05:30', \
                 '-1 year -2 months -3 days -04:05:06.789012')"
                    .to_string(),
                "INSERT INTO RUST_WIRE_TEMPORAL VALUES (2, '9999-12-31', \
                 '9999-12-31 23:59:59.999999', '23:59:59.999999', '00:00:00-00:00', \
                 '1 year 2 months 3 days 04:05:06.789012')"
                    .to_string(),
                "SELECT ID, D, TS, T, TZ, IV FROM RUST_WIRE_TEMPORAL ORDER BY ID".to_string(),
                "SELECT CAST('0001-01-01' AS DATE) AS D, \
                 CAST('9999-12-31 23:59:59.999999' AS TIMESTAMP) AS TS, \
                 CAST('23:59:59.999999' AS TIME) AS T, \
                 CAST('-1 year -2 months -3 days -04:05:06.789012' AS INTERVAL) AS IV"
                    .to_string(),
            ],
        },
        Fixture {
            name: "numeric_edges",
            suite: "types",
            statements: vec![
                "CREATE TEMP TABLE RUST_WIRE_EDGE (ID INTEGER, V NUMERIC(38,19)) \
                 DISTRIBUTE ON RANDOM"
                    .to_string(),
                "INSERT INTO RUST_WIRE_EDGE VALUES (1, 0)".to_string(),
                "INSERT INTO RUST_WIRE_EDGE VALUES (2, -0.0000000000000000001)".to_string(),
                "INSERT INTO RUST_WIRE_EDGE VALUES \
                 (3, 9999999999999999999.9999999999999999999)"
                    .to_string(),
                "INSERT INTO RUST_WIRE_EDGE VALUES \
                 (4, -9999999999999999999.9999999999999999999)"
                    .to_string(),
                "SELECT ID, V FROM RUST_WIRE_EDGE ORDER BY ID".to_string(),
                "SELECT CAST('9999999999999999999.9999999999999999999' AS NUMERIC(38,19)) AS V, \
                 CAST('-0.0000000000000000001' AS NUMERIC(38,19)) AS N"
                    .to_string(),
            ],
        },
        Fixture {
            name: "metadata_core",
            suite: "metadata",
            statements: vec![
                format!("DROP SCHEMA {META_SCHEMA} CASCADE"),
                format!("CREATE SCHEMA {META_SCHEMA}"),
                format!(
                    "CREATE TABLE {META_SCHEMA}.T (ID INTEGER NOT NULL, NAME VARCHAR(20), \
                     AMT NUMERIC(10,2), PRIMARY KEY (ID)) DISTRIBUTE ON (ID) ORGANIZE ON (ID, NAME)"
                ),
                format!("INSERT INTO {META_SCHEMA}.T VALUES (1, 'alpha', 3.14)"),
                format!(
                    "CREATE VIEW {META_SCHEMA}.V AS SELECT ID, NAME FROM {META_SCHEMA}.T"
                ),
                format!("COMMENT ON TABLE {META_SCHEMA}.T IS 'meta table comment'"),
                format!("COMMENT ON COLUMN {META_SCHEMA}.T.NAME IS 'display name'"),
                format!("CREATE SYNONYM {META_SCHEMA}.S FOR {META_SCHEMA}.T"),
                "SELECT current_catalog".to_string(),
                "SELECT current_schema".to_string(),
                "SELECT schema FROM _v_schema ORDER BY schema".to_string(),
                "SELECT database, owner, defschema FROM _v_database ORDER BY database".to_string(),
                format!(
                    "SELECT schema, tablename, owner, objtype, objid, reltuples FROM _v_table \
                     WHERE tablename IS NOT NULL AND schema = '{META_SCHEMA}' \
                     AND schema NOT IN ('DEFINITION_SCHEMA', 'INZA', 'NZ_QUERY_HISTORY') \
                     AND objtype <> 'SYSTEM_TABLE' ORDER BY schema, tablename"
                ),
                format!(
                    "SELECT attname, attnum, format_type, CASE WHEN attnotnull THEN 'N' ELSE 'Y' END, \
                     objid, description FROM _v_relation_column WHERE name = 'T' \
                     AND schema = '{META_SCHEMA}' ORDER BY attnum"
                ),
                format!(
                    "SELECT schema, viewname, owner, objid, definition FROM _v_view \
                     WHERE viewname IS NOT NULL AND schema = '{META_SCHEMA}' \
                     ORDER BY schema, viewname"
                ),
                format!(
                    "SELECT schema, procedure, owner, objid, proceduresignature, returns, builtin, \
                     proceduresource, executedasowner, arguments, description FROM _v_procedure \
                     WHERE procedure IS NOT NULL AND schema = '{META_SCHEMA}' \
                     ORDER BY schema, procedure"
                ),
                format!(
                    "SELECT attname FROM _v_table_dist_map WHERE tablename = 'T' \
                     AND schema = '{META_SCHEMA}' ORDER BY distattnum"
                ),
                format!(
                    "SELECT attname FROM _v_table_organize_column WHERE tablename = 'T' \
                     AND schema = '{META_SCHEMA}' ORDER BY orgseqno"
                ),
                format!(
                    "SELECT schema, tablename, used_bytes, allocated_bytes, \
                     (used_bytes / 1048576)::BIGINT AS size_mb, skew FROM _v_table_storage_stat \
                     WHERE tablename IS NOT NULL AND schema = '{META_SCHEMA}' \
                     ORDER BY used_bytes DESC"
                ),
                "SELECT id, username, dbname, conntime, priority, status, type, \
                 client_os_username FROM _v_session ORDER BY conntime DESC"
                    .to_string(),
                format!(
                    "SELECT schema, function, owner, objid, functionsignature, returns, env \
                     FROM _v_function WHERE function IS NOT NULL AND schema = '{META_SCHEMA}' \
                     ORDER BY schema, function"
                ),
                format!(
                    "SELECT schema, synonym_name, refobjname, refdatabase, refschema, description \
                     FROM _v_synonym WHERE synonym_name IS NOT NULL AND schema = '{META_SCHEMA}' \
                     ORDER BY schema, synonym_name"
                ),
                format!(
                    "SELECT schema, relation, constraintname, contype, attname, pkdatabase, \
                     pkschema, pkrelation, pkattname, updt_type, del_type FROM _v_relation_keydata \
                     WHERE relation IS NOT NULL AND schema = '{META_SCHEMA}' \
                     ORDER BY schema, relation, conseq"
                ),
                format!(
                    "SELECT schema, tablename, attname, distattnum FROM _v_table_dist_map \
                     WHERE tablename IS NOT NULL AND schema = '{META_SCHEMA}' \
                     ORDER BY schema, tablename, distseqno"
                ),
                format!(
                    "SELECT schema, tablename, attname, attnum FROM _v_table_organize_column \
                     WHERE tablename IS NOT NULL AND schema = '{META_SCHEMA}' \
                     ORDER BY schema, tablename, orgseqno"
                ),
                format!(
                    "SELECT schema, objname, objtype, owner, objid, description, createdate \
                     FROM _v_object_data WHERE objname IS NOT NULL AND schema = '{META_SCHEMA}' \
                     AND objtype NOT IN ('AGGREGATE','CONSTRAINT','DATABASE','DATATYPE','GROUP',\
                     'MANAGEMENT INDEX','MANAGEMENT SEQ','MANAGEMENT TABLE','MANAGEMENT VIEW',\
                     'SCHEDULER RULE','SCHEMA','SYSTEM INDEX','SYSTEM SEQ','SYSTEM TABLE',\
                     'SYSTEM VIEW','USER') ORDER BY schema, objtype, objname"
                ),
                format!(
                    "SELECT schema, seqname, owner, objid FROM _v_sequence \
                     WHERE seqname IS NOT NULL AND schema = '{META_SCHEMA}' \
                     ORDER BY schema, seqname"
                ),
                "SELECT username, objid FROM _v_user ORDER BY username".to_string(),
                "SELECT groupname, objid FROM _v_group ORDER BY groupname".to_string(),
                "SELECT qh_sessionid, qh_user, qh_database, qh_sql, qh_tsubmit, qh_tstart, \
                 qh_resrows FROM _v_qryhist WHERE 1=1 ORDER BY qh_tsubmit DESC LIMIT 10"
                    .to_string(),
                format!(
                    "SELECT schema, attname, attnum, format_type, attnotnull, coldefault, \
                     description FROM _v_relation_column WHERE name = 'T' \
                     AND schema = '{META_SCHEMA}' ORDER BY attnum"
                ),
                format!(
                    "SELECT constraintname, contype, attname, pkdatabase, pkschema, pkrelation, \
                     pkattname, updt_type, del_type FROM _v_relation_keydata WHERE relation = 'T' \
                     AND schema = '{META_SCHEMA}' ORDER BY constraintname, conseq"
                ),
                format!(
                    "SELECT description FROM _v_object_data WHERE objname = 'T' \
                     AND schema = '{META_SCHEMA}' AND objtype = 'TABLE'"
                ),
                format!(
                    "SELECT owner FROM _v_table WHERE tablename = 'T' AND schema = '{META_SCHEMA}'"
                ),
                format!("DROP SCHEMA {META_SCHEMA} CASCADE"),
            ],
        },
        Fixture {
            name: "ddl_reconstruction",
            suite: "ddl",
            statements: vec![
                format!("DROP SCHEMA {DDL_SCHEMA} CASCADE"),
                format!("CREATE SCHEMA {DDL_SCHEMA}"),
                format!(
                    "CREATE TABLE {DDL_SCHEMA}.T (ID INTEGER NOT NULL, NAME VARCHAR(20), \
                     AMT NUMERIC(10,2) DEFAULT 0, PRIMARY KEY (ID)) DISTRIBUTE ON (ID) \
                     ORGANIZE ON (ID, NAME)"
                ),
                format!("COMMENT ON TABLE {DDL_SCHEMA}.T IS 'table comment'"),
                format!("COMMENT ON COLUMN {DDL_SCHEMA}.T.NAME IS 'column comment'"),
                format!("CREATE VIEW {DDL_SCHEMA}.V AS SELECT ID, NAME FROM {DDL_SCHEMA}.T"),
                format!("COMMENT ON VIEW {DDL_SCHEMA}.V IS 'view comment'"),
                format!("COMMENT ON COLUMN {DDL_SCHEMA}.V.NAME IS 'view column comment'"),
                format!("CREATE SYNONYM {DDL_SCHEMA}.S FOR {DDL_SCHEMA}.T"),
                format!(
                    "SELECT current_catalog; \
                     SELECT schema, name, attname, attnum, format_type, attnotnull, coldefault, \
                     description FROM _v_relation_column WHERE ((name = 'T' AND schema = '{DDL_SCHEMA}')) \
                     ORDER BY schema, name, attnum; \
                     SELECT schema, tablename, attname FROM _v_table_dist_map \
                     WHERE ((tablename = 'T' AND schema = '{DDL_SCHEMA}')) \
                     ORDER BY schema, tablename, distattnum; \
                     SELECT schema, tablename, attname FROM _v_table_organize_column \
                     WHERE ((tablename = 'T' AND schema = '{DDL_SCHEMA}')) \
                     ORDER BY schema, tablename, orgseqno; \
                     SELECT schema, relation, constraintname, contype, attname, pkdatabase, \
                     pkschema, pkrelation, pkattname, updt_type, del_type FROM _v_relation_keydata \
                     WHERE ((relation = 'T' AND schema = '{DDL_SCHEMA}')) \
                     ORDER BY schema, relation, constraintname, conseq; \
                     SELECT schema, objname, description FROM _v_object_data \
                     WHERE ((objname = 'T' AND schema = '{DDL_SCHEMA}')) AND objtype = 'TABLE'"
                ),
                format!(
                    "SELECT schema, viewname, definition FROM _v_view WHERE viewname = 'V' \
                     AND schema = '{DDL_SCHEMA}' ORDER BY schema, viewname"
                ),
                format!(
                    "SELECT schema, attname, attnum, format_type, attnotnull, coldefault, \
                     description FROM _v_relation_column WHERE name = 'V' \
                     AND schema = '{DDL_SCHEMA}' ORDER BY attnum"
                ),
                format!(
                    "SELECT description FROM _v_object_data WHERE objname = 'V' \
                     AND schema = '{DDL_SCHEMA}' AND objtype = 'TABLE'"
                ),
                format!(
                    "SELECT description FROM _v_object_data WHERE objname = 'V' \
                     AND schema = '{DDL_SCHEMA}'"
                ),
                format!(
                    "SELECT schema, owner, synonym_name, refobjname, description, refdatabase, \
                     refschema FROM _v_synonym WHERE synonym_name = 'S' \
                     AND schema = '{DDL_SCHEMA}'"
                ),
                format!("DROP SCHEMA {DDL_SCHEMA} CASCADE"),
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

fn fixture_writes(statements: &[String]) -> bool {
    statements.iter().any(|statement| {
        let head = statement.trim_start();
        ["CREATE", "INSERT", "UPDATE", "DELETE", "DROP", "TRUNCATE"]
            .iter()
            .any(|keyword| {
                head.len() >= keyword.len() && head[..keyword.len()].eq_ignore_ascii_case(keyword)
            })
    })
}

fn fixture_ddl_allowed() -> bool {
    std::env::var("NZ_DEV_ALLOW_FIXTURE_DDL")
        .ok()
        .is_some_and(|value| {
            matches!(
                value.to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut out = String::with_capacity(digest.len() * 2);
    for byte in digest {
        out.push_str(&format!("{byte:02x}"));
    }
    out
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
    let mut suite = Suite::All;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--out" => out_dir = PathBuf::from(args.next().expect("--out needs a directory")),
            "--suite" => {
                let value = args.next().expect("--suite needs a name");
                suite = Suite::parse(&value).unwrap_or_else(|| {
                    eprintln!(
                        "capture_wire_fixtures: unknown suite {value} \
                         (expected encoding, metadata, ddl, types, errors or all)"
                    );
                    std::process::exit(2);
                });
            }
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
    // The password and host are secrets. The user name is deliberately not
    // scanned: catalog fixtures legitimately contain role/schema names such as
    // ADMIN, and the user name is not a credential.
    let secrets = vec![password.clone(), host.clone()];

    let selected: Vec<Fixture> = fixtures()
        .into_iter()
        .filter(|fixture| suite.includes(fixture))
        .collect();
    if selected.is_empty() {
        eprintln!("capture_wire_fixtures: no fixtures selected for this suite");
        std::process::exit(2);
    }
    if selected
        .iter()
        .any(|fixture| fixture_writes(&fixture.statements))
        && !fixture_ddl_allowed()
    {
        eprintln!(
            "capture_wire_fixtures: selected fixtures create temporary objects; \
             set NZ_DEV_ALLOW_FIXTURE_DDL=1 to allow that"
        );
        std::process::exit(2);
    }

    let probe = NzConnectionConfig {
        host: host.clone(),
        port,
        database: database.clone(),
        user: user.clone(),
        password: password.clone(),
        security_level: SecurityLevel::OnlyUnsecuredSession,
        ..Default::default()
    };
    let (server_version, server_encoding) = match Client::connect(&probe).await {
        Ok(client) => {
            let version = client
                .query_multi("SELECT version() AS V", &[])
                .await
                .ok()
                .and_then(|result| {
                    result.result_sets.first().and_then(|set| {
                        set.rows
                            .first()
                            .and_then(|row| row.try_get::<_, String>(0).ok())
                    })
                })
                .unwrap_or_else(|| "unknown".to_string());
            let encoding = client
                .query_multi("SHOW NZ_ENCODING", &[])
                .await
                .ok()
                .map(|result| result.notices.join("; "))
                .filter(|notices| !notices.is_empty())
                .unwrap_or_else(|| "unknown".to_string());
            let _ = client.close().await;
            (version, encoding)
        }
        Err(error) => {
            eprintln!(
                "capture_wire_fixtures: cannot read server version/encoding directly: {error}"
            );
            ("unknown".to_string(), "unknown".to_string())
        }
    };

    let mut manifest_fixtures = Vec::new();
    for fixture in selected {
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
        let file = std::fs::read(&path).expect("read back fixture");
        manifest_fixtures.push(serde_json::json!({
            "name": fixture.name,
            "suite": fixture.suite,
            "sha256": sha256_hex(&file),
            "statements": fixture.statements,
        }));
    }

    let manifest = serde_json::json!({
        "format": 1,
        "generated_at_unix": SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_secs())
            .unwrap_or(0),
        "server_version": server_version,
        "server_encoding": server_encoding,
        "fixtures": manifest_fixtures,
    });
    let fixture_count = manifest["fixtures"]
        .as_array()
        .map(|fixtures| fixtures.len())
        .unwrap_or(0);
    let manifest_path = out_dir.join("manifest.json");
    let bytes = serde_json::to_vec_pretty(&manifest).expect("serialize manifest");
    std::fs::write(&manifest_path, bytes).expect("write manifest");
    println!("{}: {fixture_count} fixtures", manifest_path.display());
}
