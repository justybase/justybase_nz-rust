//! Shared offline protocol fixtures for integration tests.
//!
//! The helpers build byte-exact Netezza backend frames, split them into
//! deterministic TCP fragments, and run a scripted multi-connection mock
//! backend. Every test binary includes this module with `mod support;` and
//! uses only a subset of it, so unused-item lints are silenced here instead of
//! in each test file.
#![allow(dead_code)]

use nz_rust::NzConnectionConfig;
use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

/// Password used by every mock configuration. Assertions compare derived
/// hashes only; the value is never printed.
pub const MOCK_PASSWORD: &str = "s3cr3t-Pa55";
/// First backend process id; connection `n` (1-based) reports `BASE_PID + n`.
pub const BASE_PID: i32 = 5_000;
pub const SECRET_KEY: i32 = -2_092_017_624;
pub const CANCEL_REQUEST_CODE: i32 = 80_877_102;

// ---------------------------------------------------------------------------
// Deterministic PRNG (same xorshift64 as tests/protocol_properties.rs).
// ---------------------------------------------------------------------------

pub fn next_random(seed: &mut u64) -> u64 {
    *seed ^= *seed << 13;
    *seed ^= *seed >> 7;
    *seed ^= *seed << 17;
    *seed
}

// ---------------------------------------------------------------------------
// Frame encoders (query phase).
// ---------------------------------------------------------------------------

/// `[type][4 reserved bytes][i32 BE payload length][payload]`.
pub fn frame(kind: u8, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(9 + payload.len());
    out.push(kind);
    out.extend_from_slice(&[0; 4]);
    out.extend_from_slice(&(payload.len() as i32).to_be_bytes());
    out.extend_from_slice(payload);
    out
}

/// A frame header that declares `declared` payload bytes regardless of what
/// follows. Used for malformed-length tests.
pub fn frame_header(kind: u8, declared: i32) -> Vec<u8> {
    let mut out = vec![kind, 0, 0, 0, 0];
    out.extend_from_slice(&declared.to_be_bytes());
    out
}

/// ReadyForQuery has no length field: `Z` + 4 reserved bytes.
pub fn ready() -> Vec<u8> {
    vec![b'Z', 0, 0, 0, 0]
}

pub fn command_complete(tag: &str) -> Vec<u8> {
    let mut payload = tag.as_bytes().to_vec();
    payload.push(0);
    frame(b'C', &payload)
}

pub fn notice(message: &str) -> Vec<u8> {
    frame(b'N', format!("SNOTICE\0M{message}\0\0").as_bytes())
}

pub fn error(sqlstate: &str, message: &str) -> Vec<u8> {
    frame(
        b'E',
        format!("SERROR\0C{sqlstate}\0M{message}\0\0").as_bytes(),
    )
}

/// Text column: `(name, type oid, type length)`.
pub type TextColumn<'a> = (&'a str, i32, i16);

pub const OID_INT4: i32 = 23;
pub const OID_VARCHAR: i32 = 1043;

pub fn row_description_payload(columns: &[TextColumn<'_>]) -> Vec<u8> {
    let mut payload = (columns.len() as u16).to_be_bytes().to_vec();
    for (name, oid, len) in columns {
        payload.extend_from_slice(name.as_bytes());
        payload.push(0);
        payload.extend_from_slice(&oid.to_be_bytes());
        payload.extend_from_slice(&len.to_be_bytes());
        payload.extend_from_slice(&(-1i32).to_be_bytes());
        payload.push(0);
    }
    payload
}

pub fn row_description(columns: &[TextColumn<'_>]) -> Vec<u8> {
    frame(b'T', &row_description_payload(columns))
}

/// Text DataRow payload: MSB-first bitmap (set bit = present), then each
/// present cell as `i32 BE (len + 4)` + bytes.
pub fn text_row_payload(cells: &[Option<&[u8]>]) -> Vec<u8> {
    let mut payload = vec![0u8; cells.len().div_ceil(8)];
    for (index, cell) in cells.iter().enumerate() {
        if cell.is_some() {
            payload[index / 8] |= 1 << (7 - index % 8);
        }
    }
    for cell in cells.iter().flatten() {
        payload.extend_from_slice(&((cell.len() + 4) as i32).to_be_bytes());
        payload.extend_from_slice(cell);
    }
    payload
}

pub fn text_row(cells: &[Option<&[u8]>]) -> Vec<u8> {
    frame(b'D', &text_row_payload(cells))
}

/// `n` INT4 text columns named `C0..C{n-1}`.
pub fn int_columns(n: usize) -> Vec<(String, i32, i16)> {
    (0..n).map(|i| (format!("C{i}"), OID_INT4, 4)).collect()
}

pub fn as_text_columns(columns: &[(String, i32, i16)]) -> Vec<TextColumn<'_>> {
    columns
        .iter()
        .map(|(name, oid, len)| (name.as_str(), *oid, *len))
        .collect()
}

// ---------------------------------------------------------------------------
// DBOS (binary) encoders.
// ---------------------------------------------------------------------------

const DBOS_INT: i32 = 3;
const DBOS_VARCHAR: i32 = 16;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DbosKind {
    Int4,
    Varchar(i32),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DbosCell {
    Int4(i32),
    Text(String),
}

/// A DBOS layout: `kinds[i]` is logical field `i`; `phys[i]` its physical
/// field number (null-bitmap bit position).
#[derive(Clone, Debug)]
pub struct DbosLayout {
    pub kinds: Vec<DbosKind>,
    pub phys: Vec<i32>,
    pub nulls_allowed: bool,
}

impl DbosLayout {
    pub fn ints(n: usize) -> Self {
        Self {
            kinds: vec![DbosKind::Int4; n],
            phys: (0..n as i32).collect(),
            nulls_allowed: true,
        }
    }

    fn bitmap_len(&self) -> usize {
        if self.nulls_allowed {
            self.kinds.len().div_ceil(8)
        } else {
            0
        }
    }

    fn fixed_offsets(&self) -> (Vec<i32>, i32) {
        let mut offset = (2 + self.bitmap_len()) as i32;
        let mut offsets = Vec::with_capacity(self.kinds.len());
        let mut varying = 0;
        for kind in &self.kinds {
            match kind {
                DbosKind::Int4 => {
                    offsets.push(offset);
                    offset += 4;
                }
                DbosKind::Varchar(_) => {
                    offsets.push(varying);
                    varying += 1;
                }
            }
        }
        (offsets, offset)
    }

    /// `X` RowDescriptionStandard payload.
    pub fn descriptor_payload(&self) -> Vec<u8> {
        let (offsets, fixed_size) = self.fixed_offsets();
        let fixed = self
            .kinds
            .iter()
            .filter(|k| matches!(k, DbosKind::Int4))
            .count() as i32;
        let varying = self.kinds.len() as i32 - fixed;
        let mut payload = Vec::new();
        for value in [
            1,
            i32::from(self.nulls_allowed),
            4,
            4,
            fixed,
            varying,
            fixed_size,
            fixed_size,
            self.kinds.len() as i32,
        ] {
            payload.extend_from_slice(&value.to_be_bytes());
        }
        for (index, kind) in self.kinds.iter().enumerate() {
            let (ty, size, fixed_size) = match kind {
                DbosKind::Int4 => (DBOS_INT, 4, 4),
                DbosKind::Varchar(max) => (DBOS_VARCHAR, *max, 0),
            };
            for value in [
                ty,
                size,
                size,
                offsets[index],
                self.phys[index],
                index as i32,
                i32::from(self.nulls_allowed),
                fixed_size,
                0,
            ] {
                payload.extend_from_slice(&value.to_be_bytes());
            }
        }
        payload.extend_from_slice(&0i32.to_be_bytes());
        payload.extend_from_slice(&0i32.to_be_bytes());
        payload
    }

    /// `Y` row payload (after the 8-byte DBOS header).
    pub fn row_payload(&self, cells: &[Option<DbosCell>]) -> Vec<u8> {
        assert_eq!(cells.len(), self.kinds.len());
        let (offsets, fixed_size) = self.fixed_offsets();
        let mut row = vec![0u8; fixed_size as usize];
        let mut varying_values: Vec<Vec<u8>> = vec![Vec::new(); self.kinds.len()];
        for (index, cell) in cells.iter().enumerate() {
            match cell {
                None => {
                    assert!(self.nulls_allowed, "NULL requires nulls_allowed");
                    let phys = self.phys[index] as usize;
                    row[2 + phys / 8] |= 1 << (phys % 8);
                }
                Some(DbosCell::Int4(value)) => {
                    let start = offsets[index] as usize;
                    row[start..start + 4].copy_from_slice(&value.to_le_bytes());
                }
                Some(DbosCell::Text(text)) => {
                    varying_values[offsets[index] as usize] = text.as_bytes().to_vec();
                }
            }
        }
        // Varying fields are written in varying-index order; NULL varying
        // fields still occupy an (empty) slot.
        let varying = self
            .kinds
            .iter()
            .filter(|k| matches!(k, DbosKind::Varchar(_)))
            .count();
        for bytes in varying_values.iter().take(varying) {
            let encoded = bytes.len() + 2;
            row.extend_from_slice(&(encoded as u16).to_le_bytes());
            row.extend_from_slice(bytes);
            if encoded % 2 == 1 {
                row.push(0);
            }
        }
        row
    }
}

pub fn dbos_descriptor(layout: &DbosLayout) -> Vec<u8> {
    frame(b'X', &layout.descriptor_payload())
}

/// `Y` + 4 reserved + i32 reserved + i32 payload length + payload.
pub fn dbos_row_frame(payload: &[u8]) -> Vec<u8> {
    let mut out = vec![b'Y', 0, 0, 0, 0];
    out.extend_from_slice(&0i32.to_be_bytes());
    out.extend_from_slice(&(payload.len() as i32).to_be_bytes());
    out.extend_from_slice(payload);
    out
}

// ---------------------------------------------------------------------------
// Canned responses.
// ---------------------------------------------------------------------------

/// `T(ONE int4, TXT varchar)`, rows `(n, "row")` for n in 1..=rows, `C`, `Z`.
pub fn select_rows(rows: usize) -> Vec<u8> {
    let mut wire = row_description(&[("ONE", OID_INT4, 4), ("TXT", OID_VARCHAR, -1)]);
    for n in 1..=rows {
        let number = n.to_string();
        wire.extend(text_row(&[Some(number.as_bytes()), Some(b"row")]));
    }
    wire.extend(command_complete(&format!("SELECT {rows}")));
    wire.extend(ready());
    wire
}

pub fn select_one() -> Vec<u8> {
    select_rows(1)
}

pub fn simple_command(tag: &str) -> Vec<u8> {
    let mut wire = command_complete(tag);
    wire.extend(ready());
    wire
}

pub fn cancelled_response() -> Vec<u8> {
    let mut wire = error("57014", "query canceled");
    wire.extend(ready());
    wire
}

// ---------------------------------------------------------------------------
// Deterministic fragmentation.
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug)]
pub enum Chunking {
    /// One `write` for the whole buffer.
    Whole,
    /// Fixed-size chunks.
    Fixed(usize),
    /// Chunk sizes 1..=max drawn from a seeded xorshift PRNG.
    Seeded { seed: u64, max: usize },
}

impl Chunking {
    /// The standard matrix from the qualification plan.
    pub fn matrix() -> Vec<Chunking> {
        let mut plans = vec![
            Chunking::Whole,
            Chunking::Fixed(1),
            Chunking::Fixed(2),
            Chunking::Fixed(3),
            Chunking::Fixed(4),
            Chunking::Fixed(7),
        ];
        for seed in [0x5eed_0001, 0x5eed_0002, 0xdead_beef, 0x0123_4567_89ab] {
            plans.push(Chunking::Seeded { seed, max: 13 });
        }
        plans
    }

    /// Lengths of consecutive chunks covering `len` bytes.
    pub fn sizes(&self, len: usize) -> Vec<usize> {
        let mut sizes = Vec::new();
        let mut remaining = len;
        let mut seed = match self {
            Chunking::Seeded { seed, .. } => *seed | 1,
            _ => 1,
        };
        while remaining > 0 {
            let size = match self {
                Chunking::Whole => remaining,
                Chunking::Fixed(n) => (*n).max(1).min(remaining),
                Chunking::Seeded { max, .. } => {
                    (1 + (next_random(&mut seed) as usize % (*max).max(1))).min(remaining)
                }
            };
            sizes.push(size);
            remaining -= size;
        }
        sizes
    }
}

/// Write `bytes` as separate `write` + `flush` calls according to `chunking`.
pub fn write_chunked(stream: &mut impl Write, bytes: &[u8], chunking: Chunking) -> io::Result<()> {
    let mut offset = 0;
    for size in chunking.sizes(bytes.len()) {
        stream.write_all(&bytes[offset..offset + size])?;
        stream.flush()?;
        offset += size;
    }
    Ok(())
}

/// In-memory reader that never returns more than the planned chunk size per
/// `read` call: exact control over every frame boundary.
pub struct ChunkedReader {
    data: Vec<u8>,
    position: usize,
    sizes: Vec<usize>,
    chunk: usize,
}

impl ChunkedReader {
    pub fn new(data: Vec<u8>, chunking: Chunking) -> Self {
        let sizes = chunking.sizes(data.len());
        Self {
            data,
            position: 0,
            sizes,
            chunk: 0,
        }
    }

    /// Two reads split at exactly `boundary`.
    pub fn split_at(data: Vec<u8>, boundary: usize) -> Self {
        let sizes = [boundary, data.len() - boundary]
            .into_iter()
            .filter(|&n| n > 0)
            .collect();
        Self {
            data,
            position: 0,
            sizes,
            chunk: 0,
        }
    }
}

impl Read for ChunkedReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.position >= self.data.len() || buf.is_empty() {
            return Ok(0);
        }
        let planned = self.sizes.get(self.chunk).copied().unwrap_or(usize::MAX);
        let n = planned.min(buf.len()).min(self.data.len() - self.position);
        buf[..n].copy_from_slice(&self.data[self.position..self.position + n]);
        self.position += n;
        if n == planned {
            self.chunk += 1;
        } else if let Some(size) = self.sizes.get_mut(self.chunk) {
            *size -= n;
        }
        Ok(n)
    }
}

// ---------------------------------------------------------------------------
// Handshake scripting.
// ---------------------------------------------------------------------------

/// How the backend answers each `CLIENT_BEGIN`.
#[derive(Clone, Debug)]
pub enum VersionReply {
    Accept,
    /// `M` + ASCII digit.
    Downgrade(u8),
    /// Arbitrary bytes, then the server stops handling the connection.
    Raw(Vec<u8>),
}

/// Authentication request issued after `CLIENT_DONE`.
#[derive(Clone, Debug)]
pub enum AuthScript {
    Ok,
    Password,
    Md5([u8; 2]),
    Sha256([u8; 2]),
    /// Raw bytes written instead of a well-formed `R` request; the server
    /// stops handling the connection afterwards.
    Raw(Vec<u8>),
}

#[derive(Clone, Debug)]
pub struct HandshakeScript {
    pub versions: Vec<VersionReply>,
    pub auth: AuthScript,
    /// Fragmentation applied to every server→client write.
    pub chunking: Chunking,
}

impl Default for HandshakeScript {
    fn default() -> Self {
        Self {
            versions: vec![VersionReply::Accept],
            auth: AuthScript::Ok,
            chunking: Chunking::Whole,
        }
    }
}

/// What the client sent during the handshake.
#[derive(Clone, Debug, Default)]
pub struct HandshakeLog {
    /// Version proposed in each `CLIENT_BEGIN`.
    pub begin_versions: Vec<i16>,
    /// Option opcodes after `CLIENT_BEGIN` (DB, SSL_NEGOTIATE, USER, ...).
    pub opcodes: Vec<i16>,
    /// Raw authentication response payload (after its length prefix).
    pub auth_response: Option<Vec<u8>>,
    /// Whether the handshake reached ReadyForQuery.
    pub completed: bool,
}

pub const OP_CLIENT_BEGIN: i16 = 1;
pub const OP_DB: i16 = 2;
pub const OP_USER: i16 = 3;
pub const OP_REMOTE_PID: i16 = 6;
pub const OP_CLIENT_TYPE: i16 = 8;
pub const OP_PROTOCOL: i16 = 9;
pub const OP_SSL_NEGOTIATE: i16 = 11;
pub const OP_APPNAME: i16 = 13;
pub const OP_CLIENT_OS: i16 = 14;
pub const OP_CLIENT_HOST_NAME: i16 = 15;
pub const OP_CLIENT_OS_USER: i16 = 16;
pub const OP_64BIT_VARLENA: i16 = 17;
pub const OP_CLIENT_DONE: i16 = 1000;

/// Read one handshake option frame: `[i32 BE total len][i16 opcode][payload]`.
pub fn read_option_frame(stream: &mut impl Read) -> io::Result<(i16, Vec<u8>)> {
    let mut header = [0u8; 6];
    stream.read_exact(&mut header)?;
    let length = i32::from_be_bytes(header[..4].try_into().unwrap());
    let opcode = i16::from_be_bytes(header[4..6].try_into().unwrap());
    if !(6..=65_536).contains(&length) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("bad handshake frame length {length}"),
        ));
    }
    let mut payload = vec![0; length as usize - 6];
    stream.read_exact(&mut payload)?;
    Ok((opcode, payload))
}

/// Run the backend side of a handshake whose first `CLIENT_BEGIN` frame
/// (`first`, 8 bytes) has already been consumed by the acceptor.
pub fn serve_handshake_after_begin(
    stream: &mut TcpStream,
    first: [u8; 8],
    script: &HandshakeScript,
    pid: i32,
) -> io::Result<HandshakeLog> {
    let mut log = HandshakeLog::default();
    let mut pending = Some(first);
    for reply in &script.versions {
        let payload = match pending.take() {
            Some(first) => {
                assert_eq!(i16::from_be_bytes([first[4], first[5]]), OP_CLIENT_BEGIN);
                first[6..8].to_vec()
            }
            None => {
                let (opcode, payload) = read_option_frame(stream)?;
                assert_eq!(opcode, OP_CLIENT_BEGIN, "expected CLIENT_BEGIN");
                payload
            }
        };
        log.begin_versions
            .push(i16::from_be_bytes([payload[0], payload[1]]));
        match reply {
            VersionReply::Accept => write_chunked(stream, b"N", script.chunking)?,
            VersionReply::Downgrade(digit) => {
                write_chunked(stream, &[b'M', *digit], script.chunking)?
            }
            VersionReply::Raw(bytes) => {
                write_chunked(stream, bytes, script.chunking)?;
                return Ok(log);
            }
        }
    }

    loop {
        let (opcode, _) = read_option_frame(stream)?;
        log.opcodes.push(opcode);
        if opcode == OP_CLIENT_DONE {
            break;
        }
        write_chunked(stream, b"N", script.chunking)?;
    }

    match &script.auth {
        AuthScript::Ok => write_chunked(stream, &auth_request(0, None), script.chunking)?,
        AuthScript::Password => {
            write_chunked(stream, &auth_request(3, None), script.chunking)?;
            log.auth_response = Some(read_auth_response(stream)?);
        }
        AuthScript::Md5(salt) => {
            write_chunked(stream, &auth_request(5, Some(salt)), script.chunking)?;
            log.auth_response = Some(read_auth_response(stream)?);
        }
        AuthScript::Sha256(salt) => {
            write_chunked(stream, &auth_request(6, Some(salt)), script.chunking)?;
            log.auth_response = Some(read_auth_response(stream)?);
        }
        AuthScript::Raw(bytes) => {
            write_chunked(stream, bytes, script.chunking)?;
            return Ok(log);
        }
    }

    let mut complete = Vec::new();
    // Authentication-OK, then BackendKeyData, then ReadyForQuery.
    complete.extend(auth_request(0, None));
    complete.extend_from_slice(b"K");
    complete.extend_from_slice(&[0; 4]);
    complete.extend_from_slice(&12i32.to_be_bytes());
    complete.extend_from_slice(&pid.to_be_bytes());
    complete.extend_from_slice(&SECRET_KEY.to_be_bytes());
    complete.extend(ready());
    // After a password exchange the backend sends a second `R 0`; the client
    // skips it while waiting for ReadyForQuery. With AUTH_REQ_OK the first
    // `R 0` already consumed the authentication step, so do not duplicate it.
    if matches!(script.auth, AuthScript::Ok) {
        complete.drain(..5);
    }
    write_chunked(stream, &complete, script.chunking)?;
    log.completed = true;
    Ok(log)
}

pub fn auth_request(areq: i32, salt: Option<&[u8; 2]>) -> Vec<u8> {
    let mut out = vec![b'R'];
    out.extend_from_slice(&areq.to_be_bytes());
    if let Some(salt) = salt {
        out.extend_from_slice(salt);
    }
    out
}

fn read_auth_response(stream: &mut impl Read) -> io::Result<Vec<u8>> {
    let mut len = [0u8; 4];
    stream.read_exact(&mut len)?;
    let len = i32::from_be_bytes(len);
    if !(4..=4096).contains(&len) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "bad auth response length",
        ));
    }
    let mut payload = vec![0; len as usize - 4];
    stream.read_exact(&mut payload)?;
    Ok(payload)
}

/// Read a simple-query packet: `P` + i32 command number + SQL + NUL.
/// Returns `None` on EOF.
pub fn read_query(stream: &mut impl Read) -> Option<String> {
    let mut header = [0; 5];
    stream.read_exact(&mut header).ok()?;
    assert_eq!(header[0], b'P', "expected a query packet");
    let mut sql = Vec::new();
    loop {
        let mut byte = [0];
        stream.read_exact(&mut byte).ok()?;
        if byte[0] == 0 {
            return Some(String::from_utf8(sql).expect("query is UTF-8"));
        }
        sql.push(byte[0]);
    }
}

// ---------------------------------------------------------------------------
// Multi-connection mock backend.
// ---------------------------------------------------------------------------

/// Counts out-of-band cancel requests and lets tests wait for them.
#[derive(Default)]
pub struct CancelBoard {
    received: Mutex<Vec<(i32, i32)>>,
    changed: Condvar,
}

impl CancelBoard {
    fn push(&self, pid: i32, key: i32) {
        self.received.lock().unwrap().push((pid, key));
        self.changed.notify_all();
    }

    pub fn count(&self) -> usize {
        self.received.lock().unwrap().len()
    }

    pub fn pids(&self) -> Vec<i32> {
        self.received
            .lock()
            .unwrap()
            .iter()
            .map(|(pid, _)| *pid)
            .collect()
    }

    /// Block until at least `n` cancels arrived; `false` on timeout.
    pub fn wait_for(&self, n: usize, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        let mut guard = self.received.lock().unwrap();
        while guard.len() < n {
            let now = Instant::now();
            if now >= deadline {
                return false;
            }
            guard = self.changed.wait_timeout(guard, deadline - now).unwrap().0;
        }
        true
    }
}

/// One accepted backend connection after a successful handshake.
pub struct Session {
    pub stream: TcpStream,
    /// 1-based physical connection number.
    pub index: usize,
    pub pid: i32,
    pub chunking: Chunking,
    pub cancels: Arc<CancelBoard>,
    pub log: HandshakeLog,
}

impl Session {
    pub fn read_query(&mut self) -> Option<String> {
        read_query(&mut self.stream)
    }

    /// Send bytes with this session's fragmentation plan. Write errors are
    /// returned so handlers can stop when the client hung up.
    pub fn send(&mut self, bytes: &[u8]) -> io::Result<()> {
        write_chunked(&mut self.stream, bytes, self.chunking)
    }

    /// Serve every query with `respond(sql)` until EOF.
    pub fn serve_all(&mut self, respond: impl Fn(&str) -> Vec<u8>) {
        while let Some(sql) = self.read_query() {
            if self.send(&respond(&sql)).is_err() {
                return;
            }
        }
    }
}

type Handler = dyn Fn(&mut Session) + Send + Sync + 'static;

/// A scripted backend that accepts any number of connections. Each
/// connection performs the scripted handshake and is then handed to the
/// handler on its own thread. Cancel requests (16-byte out-of-band packets)
/// are recorded on [`MockServer::cancels`].
pub struct MockServer {
    pub port: u16,
    /// Physical (handshaken) connections accepted so far.
    pub accepted: Arc<AtomicUsize>,
    pub cancels: Arc<CancelBoard>,
    pub logs: Arc<Mutex<Vec<HandshakeLog>>>,
    /// Panics raised by handler threads (assertion failures), surfaced by
    /// [`MockServer::assert_no_handler_panics`].
    handler_panics: Arc<Mutex<Vec<String>>>,
}

impl MockServer {
    pub fn start(
        script: HandshakeScript,
        handler: impl Fn(&mut Session) + Send + Sync + 'static,
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock backend");
        let port = listener.local_addr().unwrap().port();
        let accepted = Arc::new(AtomicUsize::new(0));
        let cancels = Arc::new(CancelBoard::default());
        let logs = Arc::new(Mutex::new(Vec::new()));
        let handler_panics = Arc::new(Mutex::new(Vec::new()));
        let handler: Arc<Handler> = Arc::new(handler);
        {
            let accepted = accepted.clone();
            let cancels = cancels.clone();
            let logs = logs.clone();
            let handler_panics = handler_panics.clone();
            thread::spawn(move || {
                for stream in listener.incoming() {
                    let Ok(mut stream) = stream else { return };
                    let _ = stream.set_nodelay(true);
                    let _ = stream.set_read_timeout(Some(Duration::from_secs(20)));
                    let mut first = [0u8; 8];
                    if stream.read_exact(&mut first).is_err() {
                        continue;
                    }
                    let length = i32::from_be_bytes(first[..4].try_into().unwrap());
                    let code = i32::from_be_bytes(first[4..8].try_into().unwrap());
                    if length == 16 && code == CANCEL_REQUEST_CODE {
                        let mut rest = [0u8; 8];
                        if stream.read_exact(&mut rest).is_ok() {
                            cancels.push(
                                i32::from_be_bytes(rest[..4].try_into().unwrap()),
                                i32::from_be_bytes(rest[4..].try_into().unwrap()),
                            );
                        }
                        continue;
                    }
                    let index = accepted.load(Ordering::SeqCst) + 1;
                    let pid = BASE_PID + index as i32;
                    let script = script.clone();
                    let handler = handler.clone();
                    let accepted = accepted.clone();
                    let cancels = cancels.clone();
                    let logs = logs.clone();
                    let handler_panics = handler_panics.clone();
                    // Count the connection before the handshake finishes so
                    // the next accept gets a distinct pid.
                    accepted.fetch_add(1, Ordering::SeqCst);
                    thread::spawn(move || {
                        let log =
                            match serve_handshake_after_begin(&mut stream, first, &script, pid) {
                                Ok(log) => log,
                                Err(_) => return,
                            };
                        logs.lock().unwrap().push(log.clone());
                        if !log.completed {
                            return;
                        }
                        let mut session = Session {
                            stream,
                            index,
                            pid,
                            chunking: script.chunking,
                            cancels,
                            log,
                        };
                        let outcome =
                            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                handler(&mut session)
                            }));
                        if let Err(panic) = outcome {
                            let message = panic
                                .downcast_ref::<String>()
                                .cloned()
                                .or_else(|| panic.downcast_ref::<&str>().map(|s| s.to_string()))
                                .unwrap_or_else(|| "non-string panic".into());
                            handler_panics.lock().unwrap().push(message);
                        }
                    });
                }
            });
        }
        Self {
            port,
            accepted,
            cancels,
            logs,
            handler_panics,
        }
    }

    pub fn accepted(&self) -> usize {
        self.accepted.load(Ordering::SeqCst)
    }

    pub fn config(&self) -> NzConnectionConfig {
        mock_config(self.port)
    }

    pub fn assert_no_handler_panics(&self) {
        let panics = self.handler_panics.lock().unwrap();
        assert!(panics.is_empty(), "mock handler panicked: {panics:?}");
    }
}

pub fn mock_config(port: u16) -> NzConnectionConfig {
    NzConnectionConfig {
        host: "127.0.0.1".into(),
        port,
        database: "JUST_DATA".into(),
        user: "admin".into(),
        password: MOCK_PASSWORD.into(),
        connection_timeout: 5,
        ..Default::default()
    }
}

/// Poll `condition` until it holds or `timeout` elapses. Used only where the
/// observable event happens on another thread with no channel to wait on
/// (e.g. a pool's background return task); it never extends a deadline to
/// make a test pass.
pub fn wait_until(timeout: Duration, mut condition: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if condition() {
            return true;
        }
        thread::sleep(Duration::from_millis(2));
    }
    condition()
}
