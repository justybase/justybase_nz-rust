//! Reader for the shared `.nzreplay.gz` protocol fixtures.
//!
//! The format is produced by the C# `tools/NzReplayCapture` tool and is a
//! gzip stream around a .NET `BinaryWriter` payload:
//!
//! ```text
//! u32 LE magic 0x4E5A5250 ("NZRP"), i32 LE version (2),
//! .NET string query (7-bit encoded byte length + UTF-8),
//! i32 LE expected rows, i32 LE expected columns, i32 LE segment count,
//! then segment count x (i32 LE length + raw server-to-client bytes).
//! ```
//!
//! `segments[0]` is the handshake response; every later segment answers one
//! client query packet and the last segment is the recorded `SELECT`.

use flate2::read::GzDecoder;
use std::fs;
use std::io::{self, ErrorKind, Read};
use std::path::{Path, PathBuf};

/// `"NZRP"` as written little-endian by .NET `BinaryWriter`.
pub const NZRP_MAGIC: u32 = 0x4E5A_5250;
/// The only fixture version this reader understands.
pub const NZRP_VERSION: i32 = 2;

/// A parsed replay fixture.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NzReplayFixture {
    /// The SQL text recorded with the fixture.
    pub query: String,
    /// Row count the recorded response is expected to produce.
    pub expected_rows: usize,
    /// Column count the recorded response is expected to expose.
    pub expected_columns: usize,
    /// `segments[0]` is the handshake; the rest answer client queries.
    pub segments: Vec<Vec<u8>>,
}

impl NzReplayFixture {
    /// Loads and parses one `.nzreplay.gz` file.
    pub fn load(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref();
        let compressed = fs::read(path)?;
        let mut decoder = GzDecoder::new(&compressed[..]);
        let mut raw = Vec::new();
        decoder.read_to_end(&mut raw)?;
        Self::parse(&raw)
            .map_err(|error| io::Error::new(error.kind(), format!("{}: {error}", path.display())))
    }

    /// Parses an already-decompressed fixture payload.
    pub fn parse(raw: &[u8]) -> io::Result<Self> {
        let mut reader = FixtureReader { data: raw, pos: 0 };

        let magic = reader.u32()?;
        if magic != NZRP_MAGIC {
            return Err(invalid(format!("bad magic 0x{magic:08X}")));
        }
        let version = reader.i32()?;
        if version != NZRP_VERSION {
            return Err(invalid(format!("unsupported version {version}")));
        }

        let query = reader.dotnet_string()?;
        let expected_rows = reader.i32()? as usize;
        let expected_columns = reader.i32()? as usize;
        let segment_count = reader.i32()? as usize;
        if segment_count < 2 {
            return Err(invalid(
                "fixture needs a handshake and at least one response segment".into(),
            ));
        }

        let mut segments = Vec::with_capacity(segment_count);
        for _ in 0..segment_count {
            let length = reader.i32()?;
            if length < 0 {
                return Err(invalid(format!("negative segment length {length}")));
            }
            segments.push(reader.bytes(length as usize)?.to_vec());
        }
        if reader.pos != raw.len() {
            return Err(invalid(format!(
                "{} trailing bytes after the last segment",
                raw.len() - reader.pos
            )));
        }

        Ok(Self {
            query,
            expected_rows,
            expected_columns,
            segments,
        })
    }

    /// The recorded handshake response.
    pub fn handshake(&self) -> &[u8] {
        &self.segments[0]
    }

    /// The last recorded response (the user SQL result).
    pub fn last_response(&self) -> &[u8] {
        self.segments
            .last()
            .expect("fixture has at least two segments")
    }
}

/// Looks for a shipped fixture in the usual workspace locations.
///
/// `NZ_REPLAY_FIXTURE_DIR` wins; then the in-crate copy, the Node copy and
/// the C# `TestSupport/Fixtures` copy that records the fixtures.
pub fn find_fixture(name: &str) -> Option<PathBuf> {
    let file_name = format!("{name}.nzreplay.gz");
    let mut candidates = Vec::new();
    if let Ok(dir) = std::env::var("NZ_REPLAY_FIXTURE_DIR") {
        candidates.push(PathBuf::from(dir).join(&file_name));
    }
    candidates.push(
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/replay")
            .join(&file_name),
    );
    candidates.push(
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../justybase_netezza_node_driver/tests/fixtures/replay")
            .join(&file_name),
    );
    candidates.push(
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../JustyBase.NetezzaDriver/src/JustyBase.NetezzaDriver.TestSupport/Fixtures")
            .join(&file_name),
    );
    candidates.into_iter().find(|path| path.is_file())
}

/// Loads a fixture with [`find_fixture`], panicking when it is missing or
/// malformed. Intended for tests, where a missing fixture is a failure.
pub fn load_shipped(name: &str) -> NzReplayFixture {
    let path = find_fixture(name)
        .unwrap_or_else(|| panic!("replay fixture '{name}' not found; set NZ_REPLAY_FIXTURE_DIR"));
    NzReplayFixture::load(&path).unwrap_or_else(|error| panic!("{error}"))
}

fn invalid(message: String) -> io::Error {
    io::Error::new(ErrorKind::InvalidData, message)
}

struct FixtureReader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl FixtureReader<'_> {
    fn take(&mut self, length: usize) -> io::Result<&[u8]> {
        let end = self
            .pos
            .checked_add(length)
            .ok_or_else(|| invalid("length overflow".into()))?;
        let slice = self
            .data
            .get(self.pos..end)
            .ok_or_else(|| invalid("truncated fixture".into()))?;
        self.pos = end;
        Ok(slice)
    }

    fn bytes(&mut self, length: usize) -> io::Result<&[u8]> {
        self.take(length)
    }

    fn u32(&mut self) -> io::Result<u32> {
        let bytes = self.take(4)?;
        Ok(u32::from_le_bytes(bytes.try_into().unwrap()))
    }

    fn i32(&mut self) -> io::Result<i32> {
        let bytes = self.take(4)?;
        Ok(i32::from_le_bytes(bytes.try_into().unwrap()))
    }

    fn dotnet_string(&mut self) -> io::Result<String> {
        let mut length = 0usize;
        let mut shift = 0u32;
        loop {
            let byte = *self
                .data
                .get(self.pos)
                .ok_or_else(|| invalid("truncated string length".into()))?;
            self.pos += 1;
            length |= ((byte & 0x7f) as usize) << shift;
            if byte & 0x80 == 0 {
                break;
            }
            shift += 7;
            if shift > 28 {
                return Err(invalid("invalid 7-bit string length".into()));
            }
        }
        let bytes = self.take(length)?;
        String::from_utf8(bytes.to_vec()).map_err(|_| invalid("query is not UTF-8".into()))
    }
}
