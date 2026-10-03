//! Cross-driver performance harness: 200k-row `FACTPRODUCTINVENTORY` read.
//!
//! Opt-in so the default suite stays appliance-independent:
//! - replay (no database): `NZ_RUN_REPLAY_PERF=1`
//! - live (real appliance): `NZ_RUN_PERF_TESTS=1` + `NZ_DEV_*`
//!
//! The replay fixture is the *shared* `.nzreplay.gz` file recorded by the C#
//! `tools/NzReplayCapture` (`NZRP` v2 + GZip + .NET `BinaryReader` framing).
//! Only the **last segment** (the SELECT response) is replayed: the Rust
//! client performs its own handshake (no C# `ConnSendQuery` SET/metadata
//! prefix), then the server answers every `P`-packet query with those bytes.
//!
//! Allocation metric: a counting global allocator reports total allocated
//! bytes Instride the measured query — the Rust analogue of C#'s
//! `GC.GetAllocatedBytesForCurrentThread()` / BenchmarkDotNet
//! `[MemoryDiagnoser]`. Counters are reset right before the measured query
//! while the connection is already open and the server thread is idle, so
//! the numbers reflect the client read/decode path.

use nz_rust::{NzConnection, NzConnectionConfig, NzValue, QueryStreamSink};
use std::alloc::{GlobalAlloc, Layout, System};
use std::env;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::Instant;

// -- counting allocator ------------------------------------------------------

static ALLOC_BYTES: AtomicU64 = AtomicU64::new(0);
static ALLOC_COUNT: AtomicU64 = AtomicU64::new(0);
static DEALLOC_BYTES: AtomicU64 = AtomicU64::new(0);

struct CountingAllocator;

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOC_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
        System.alloc(layout)
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        DEALLOC_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        System.dealloc(ptr, layout)
    }
}

#[global_allocator]
static GLOBAL: CountingAllocator = CountingAllocator;

fn reset_counters() {
    ALLOC_BYTES.store(0, Ordering::SeqCst);
    ALLOC_COUNT.store(0, Ordering::SeqCst);
    DEALLOC_BYTES.store(0, Ordering::SeqCst);
}

// -- NZRP fixture parsing (.NET BinaryReader framing) -------------------------

struct ReplayFixture {
    query: String,
    expected_rows: usize,
    expected_columns: usize,
    /// Last segment = the SELECT response (self-contained: row desc + rows +
    /// CommandComplete + ReadyForQuery).
    response: Vec<u8>,
}

fn read_7bit_int(data: &[u8], pos: &mut usize) -> usize {
    let mut result = 0usize;
    let mut shift = 0u32;
    loop {
        let b = data[*pos];
        *pos += 1;
        result |= ((b & 0x7F) as usize) << shift;
        shift += 7;
        if b & 0x80 == 0 {
            break;
        }
    }
    result
}

fn load_fixture(path: &str) -> ReplayFixture {
    let compressed = std::fs::read(path).unwrap_or_else(|e| panic!("read fixture {path}: {e}"));
    let mut decoder = flate2::read::GzDecoder::new(&compressed[..]);
    let mut raw = Vec::new();
    decoder
        .read_to_end(&mut raw)
        .expect("gunzip replay fixture");
    let mut pos = 0usize;
    // .NET BinaryWriter writes UInt32 little-endian: 0x4E5A5250 ("NZRP")
    // hits the file as bytes "PRZN".
    assert_eq!(&raw[pos..pos + 4], b"PRZN", "bad magic");
    pos += 4;
    let version = i32::from_le_bytes(raw[pos..pos + 4].try_into().unwrap());
    pos += 4;
    assert_eq!(version, 2, "unsupported replay fixture version");
    let qlen = read_7bit_int(&raw, &mut pos);
    let query = String::from_utf8(raw[pos..pos + qlen].to_vec()).expect("fixture query utf8");
    pos += qlen;
    let expected_rows = i32::from_le_bytes(raw[pos..pos + 4].try_into().unwrap()) as usize;
    pos += 4;
    let expected_columns = i32::from_le_bytes(raw[pos..pos + 4].try_into().unwrap()) as usize;
    pos += 4;
    let segments = i32::from_le_bytes(raw[pos..pos + 4].try_into().unwrap()) as usize;
    pos += 4;
    assert!(segments >= 2, "fixture needs handshake + response segments");
    let mut last = Vec::new();
    for _ in 0..segments {
        let len = i32::from_le_bytes(raw[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        last = raw[pos..pos + len].to_vec();
        pos += len;
    }
    assert_eq!(pos, raw.len(), "trailing bytes in fixture");
    ReplayFixture {
        query,
        expected_rows,
        expected_columns,
        response: last,
    }
}

fn default_fixture_path() -> String {
    if let Ok(path) = env::var("NZ_REPLAY_FIXTURE") {
        return path;
    }
    // In-repo first, then the sibling checkout that records the fixture.
    for candidate in [
        "tests/fixtures/fact200k.nzreplay.gz",
        "../JustyBase.NetezzaDriver/src/JustyBase.NetezzaDriver.TestSupport/Fixtures/fact200k.nzreplay.gz",
    ] {
        if std::path::Path::new(candidate).exists() {
            return candidate.to_string();
        }
    }
    panic!(
        "replay fixture not found; set NZ_REPLAY_FIXTURE to a fact200k.nzreplay.gz path \
         (searched tests/fixtures/ and the sibling JustyBase.NetezzaDriver checkout)"
    );
}

// -- minimal replay server (Rust handshake + canned SELECT response) ----------

fn read_frame(stream: &mut impl Read) -> (i16, Vec<u8>) {
    let mut header = [0u8; 6];
    stream.read_exact(&mut header).unwrap();
    let len = i32::from_be_bytes(header[0..4].try_into().unwrap());
    let opcode = i16::from_be_bytes(header[4..6].try_into().unwrap());
    let payload_len = (len - 6) as usize;
    let mut payload = vec![0u8; payload_len];
    stream.read_exact(&mut payload).unwrap();
    (opcode, payload)
}

fn write_be(stream: &mut impl Write, bytes: &[u8]) {
    stream.write_all(bytes).unwrap();
    stream.flush().unwrap();
}

/// Same handshake the Rust `mock_server.rs` test serves: version negotiation,
/// option loop, auth-ok, BackendKeyData, ReadyForQuery.
fn serve_handshake(stream: &mut TcpStream) {
    let (opcode, payload) = read_frame(stream);
    assert_eq!(opcode, 1, "expected CLIENT_BEGIN");
    assert_eq!(payload.len(), 2, "expected one version word");
    write_be(stream, b"N");

    let (opcode, _payload) = read_frame(stream);
    assert_eq!(opcode, 2, "expected HSV2_DB");
    write_be(stream, b"N");

    let (opcode, _payload) = read_frame(stream);
    assert_eq!(opcode, 11, "expected HSV2_SSL_NEGOTIATE");
    write_be(stream, b"N");

    loop {
        let (opcode, _payload) = read_frame(stream);
        if opcode == 1000 {
            break;
        }
        write_be(stream, b"N");
    }

    let mut auth = vec![b'R'];
    auth.extend_from_slice(&0i32.to_be_bytes());
    write_be(stream, &auth);

    let mut key = vec![b'K'];
    key.extend_from_slice(&[0, 0, 0, 0]);
    key.extend_from_slice(&12i32.to_be_bytes());
    key.extend_from_slice(&5857i32.to_be_bytes());
    key.extend_from_slice(&(-2_092_017_624i32).to_be_bytes());
    write_be(stream, &key);

    let mut ready = vec![b'Z'];
    ready.extend_from_slice(&[0, 0, 0, 0]);
    write_be(stream, &ready);
}

/// Read one frontend `P` + cmd(4) + SQL + NUL packet; `None` on EOF.
fn read_query_packet(stream: &mut TcpStream) -> Option<Vec<u8>> {
    let mut head = [0u8; 5];
    if stream.read_exact(&mut head).is_err() {
        return None;
    }
    if head[0] != b'P' {
        return None;
    }
    let mut sql = Vec::new();
    loop {
        let mut byte = [0u8; 1];
        if stream.read_exact(&mut byte).is_err() {
            return None;
        }
        if byte[0] == 0 {
            break;
        }
        sql.push(byte[0]);
    }
    Some(sql)
}

fn start_replay_server(response: Vec<u8>) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind replay listener");
    let port = listener.local_addr().unwrap().port();
    thread::spawn(move || {
        for stream in listener.incoming() {
            let mut stream = match stream {
                Ok(s) => s,
                Err(_) => break,
            };
            serve_handshake(&mut stream);
            while read_query_packet(&mut stream).is_some() {
                if stream.write_all(&response).is_err() {
                    break;
                }
                if stream.flush().is_err() {
                    break;
                }
            }
        }
    });
    port
}

// -- measurement ---------------------------------------------------------------

struct Sample {
    ms: f64,
    rows: usize,
    cols: usize,
    cells: usize,
    alloc_bytes: u64,
    alloc_count: u64,
    dealloc_bytes: u64,
}

fn run_measured_query(conn: &mut NzConnection, sql: &str) -> Sample {
    reset_counters();
    let start = Instant::now();
    let result = conn.query(sql, &[]).expect("query");
    // Buffered rows are decoded eagerly (C# `Read()` parity); touching
    // `values()` mirrors Python `fetchall()` cell materialization. Note the
    // process-global counters also see the replay server thread's per-query
    // packet parse (~1 small `Vec`), negligible next to ~1M row allocs.
    let cells: usize = result.rows().iter().map(|row| row.values().len()).sum();
    let ms = start.elapsed().as_secs_f64() * 1000.0;
    let rows = result.rows().len();
    let cols = result
        .rows()
        .first()
        .map(|row| row.columns().len())
        .unwrap_or(0);
    Sample {
        ms,
        rows,
        cols,
        cells,
        alloc_bytes: ALLOC_BYTES.load(Ordering::SeqCst),
        alloc_count: ALLOC_COUNT.load(Ordering::SeqCst),
        dealloc_bytes: DEALLOC_BYTES.load(Ordering::SeqCst),
    }
}

fn print_sample(tag: &str, run: usize, s: &Sample) {
    let rows_per_s = s.rows as f64 / (s.ms / 1000.0);
    println!(
        "{tag} run={run} query_ms={ms:.1} rows={rows} cols={cols} cells={cells} \
         rows_per_s={rps:.1} alloc_bytes={ab} alloc_count={ac} \
         bytes_per_row={bpr:.1} bytes_per_cell={bpc:.1} net_bytes={net}",
        tag = tag,
        run = run,
        ms = s.ms,
        rows = s.rows,
        cols = s.cols,
        cells = s.cells,
        rps = rows_per_s,
        ab = s.alloc_bytes,
        ac = s.alloc_count,
        bpr = s.alloc_bytes as f64 / s.rows.max(1) as f64,
        bpc = s.alloc_bytes as f64 / s.cells.max(1) as f64,
        net = s.alloc_bytes as i64 - s.dealloc_bytes as i64,
    );
}

fn env_or(name: &str, fallback: &str) -> String {
    env::var(name).unwrap_or_else(|_| fallback.to_string())
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(f64::total_cmp);
    v[v.len() / 2]
}

/// Streaming sink mirroring the C# bench loop (`while Read(): GetValue(i)`
/// on every cell) without retaining rows — the apples-to-apples counterpart
/// of `ReplayReaderBench.Sync_GetValue` / `LiveFact200kBench`.
struct TouchSink {
    rows: usize,
    cells: usize,
}

impl QueryStreamSink for TouchSink {
    fn on_columns(
        &mut self,
        _: usize,
        _: &[nz_rust::ColumnDesc],
        _: Option<&[bool]>,
    ) -> nz_rust::NzResult<()> {
        Ok(())
    }
    fn on_row(&mut self, _: usize, _: nz_rust::Row) -> nz_rust::NzResult<()> {
        Ok(())
    }
    fn on_values(
        &mut self,
        _: usize,
        _: &[nz_rust::ColumnDesc],
        values: &[NzValue],
    ) -> nz_rust::NzResult<()> {
        self.rows += 1;
        self.cells += values.len();
        for value in values {
            std::hint::black_box(value);
        }
        Ok(())
    }
}

fn run_streaming_query(conn: &mut NzConnection, sql: &str) -> Sample {
    let mut sink = TouchSink { rows: 0, cells: 0 };
    reset_counters();
    let start = Instant::now();
    let summary = conn
        .execute_stream(sql, &[], &mut sink)
        .expect("streaming query");
    let ms = start.elapsed().as_secs_f64() * 1000.0;
    let rows = sink.rows;
    let cols = sink.cells.checked_div(rows.max(1)).unwrap_or(0);
    let _ = summary;
    Sample {
        ms,
        rows,
        cols,
        cells: sink.cells,
        alloc_bytes: ALLOC_BYTES.load(Ordering::SeqCst),
        alloc_count: ALLOC_COUNT.load(Ordering::SeqCst),
        dealloc_bytes: DEALLOC_BYTES.load(Ordering::SeqCst),
    }
}

#[test]
fn replay_fact200k_time_and_allocations() {
    if env::var("NZ_RUN_REPLAY_PERF").ok().as_deref() != Some("1") {
        eprintln!("skipping: set NZ_RUN_REPLAY_PERF=1 to run the replay performance harness");
        return;
    }
    let fixture = load_fixture(&default_fixture_path());
    println!(
        "replay fixture query={} expected_rows={} expected_cols={} response_bytes={}",
        fixture.query,
        fixture.expected_rows,
        fixture.expected_columns,
        fixture.response.len()
    );
    let repeats: usize = env_or("NZ_PERF_REPEATS", "3")
        .parse()
        .expect("NZ_PERF_REPEATS must be an integer");
    assert!(repeats > 0);

    let port = start_replay_server(fixture.response.clone());
    let config = NzConnectionConfig {
        host: "127.0.0.1".into(),
        port,
        database: "JUST_DATA".into(),
        user: "replay".into(),
        password: "replay".into(),
        ..Default::default()
    };
    let mut conn = NzConnection::connect(&config).expect("replay connect");

    let mut ms = Vec::with_capacity(repeats);
    let mut bp_row = Vec::with_capacity(repeats);
    for run in 1..=repeats {
        let s = run_measured_query(&mut conn, &fixture.query);
        assert_eq!(s.rows, fixture.expected_rows, "row count");
        assert_eq!(s.cols, fixture.expected_columns, "column count");
        ms.push(s.ms);
        bp_row.push(s.alloc_bytes as f64 / s.rows as f64);
        print_sample("replay_fact200k", run, &s);
    }
    println!(
        "replay_fact200k median ms={:.1} median_bytes_per_row={:.1}",
        median(ms),
        median(bp_row)
    );

    let mut sms = Vec::with_capacity(repeats);
    let mut sbp = Vec::with_capacity(repeats);
    for run in 1..=repeats {
        let s = run_streaming_query(&mut conn, &fixture.query);
        assert_eq!(s.rows, fixture.expected_rows, "stream row count");
        assert_eq!(s.cols, fixture.expected_columns, "stream column count");
        sms.push(s.ms);
        sbp.push(s.alloc_bytes as f64 / s.rows.max(1) as f64);
        print_sample("replay_fact200k_stream", run, &s);
    }
    println!(
        "replay_fact200k_stream median ms={:.1} median_bytes_per_row={:.1}",
        median(sms),
        median(sbp)
    );
}

#[test]
fn live_fact200k_time_and_allocations() {
    if env::var("NZ_RUN_PERF_TESTS").ok().as_deref() != Some("1") {
        eprintln!("skipping: set NZ_RUN_PERF_TESTS=1 to run the live performance harness");
        return;
    }
    let host = env::var("NZ_DEV_HOST").expect("NZ_DEV_HOST is required");
    let port = env_or("NZ_DEV_PORT", "5480")
        .parse()
        .expect("NZ_DEV_PORT must be an integer");
    let database = env::var("NZ_DEV_DB")
        .or_else(|_| env::var("NZ_DEV_DATABASE"))
        .unwrap_or_else(|_| "JUST_DATA".into());
    let user = env_or("NZ_DEV_USER", "admin");
    let password = env::var("NZ_DEV_PASSWORD").expect("NZ_DEV_PASSWORD is required");
    // Same query the C# fixture recorded, so live numbers are comparable
    // with both the C# live bench and the replay numbers above.
    let sql = env_or(
        "NZ_BENCH_QUERY",
        "SELECT * FROM JUST_DATA..FACTPRODUCTINVENTORY ORDER BY ROWID LIMIT 200000",
    );
    let repeats: usize = env_or("NZ_PERF_REPEATS_LIVE", "1")
        .parse()
        .expect("NZ_PERF_REPEATS_LIVE must be an integer");
    assert!(repeats > 0);

    let config = NzConnectionConfig {
        host,
        port,
        database,
        user,
        password,
        ..Default::default()
    };
    let mut conn = NzConnection::connect(&config).expect("live connect");

    let mut ms = Vec::with_capacity(repeats);
    let mut bp_row = Vec::with_capacity(repeats);
    for run in 1..=repeats {
        let s = run_measured_query(&mut conn, &sql);
        assert_eq!(s.rows, 200_000, "row count");
        ms.push(s.ms);
        bp_row.push(s.alloc_bytes as f64 / s.rows as f64);
        print_sample("live_fact200k", run, &s);
    }
    println!(
        "live_fact200k median ms={:.1} median_bytes_per_row={:.1}",
        median(ms),
        median(bp_row)
    );

    let mut sms = Vec::with_capacity(repeats);
    let mut sbp = Vec::with_capacity(repeats);
    for run in 1..=repeats {
        let s = run_streaming_query(&mut conn, &sql);
        assert_eq!(s.rows, 200_000, "stream row count");
        sms.push(s.ms);
        sbp.push(s.alloc_bytes as f64 / s.rows.max(1) as f64);
        print_sample("live_fact200k_stream", run, &s);
    }
    println!(
        "live_fact200k_stream median ms={:.1} median_bytes_per_row={:.1}",
        median(sms),
        median(sbp)
    );
}
