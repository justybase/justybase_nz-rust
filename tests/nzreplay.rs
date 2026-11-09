//! Offline coverage for the shared `.nzreplay.gz` reader.
//!
//! A missing or malformed shipped fixture is a failure, not a skip, so the
//! reader is exercised against the same recordings the C# and Node drivers
//! replay.

mod support;

use support::nzreplay::{load_shipped, NzReplayFixture, NZRP_MAGIC};

#[test]
fn dimdate_fixture_matches_manifest() {
    let fixture = load_shipped("dimdate");
    assert_eq!(fixture.expected_rows, 3_652);
    assert_eq!(fixture.expected_columns, 19);
    assert_eq!(fixture.query, "SELECT * FROM JUST_DATA.ADMIN.DIMDATE");
    assert!(fixture.segments.len() >= 2);
    assert!(!fixture.handshake().is_empty());
    assert!(fixture.last_response().len() > 100_000);
}

#[test]
fn fact200k_fixture_matches_manifest() {
    let fixture = load_shipped("fact200k");
    assert_eq!(fixture.expected_rows, 200_000);
    assert_eq!(fixture.expected_columns, 7);
    assert!(fixture.query.starts_with("SELECT"));
    assert!(fixture.segments.len() >= 2);
    assert!(fixture.last_response().len() > 1_000_000);
}

#[test]
fn rejects_bad_magic_and_trailing_bytes() {
    let bad_magic = [0u8; 4];
    let error = NzReplayFixture::parse(&bad_magic).unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);

    // A minimal valid header with no segments: magic, version, empty query,
    // rows, columns, segment count = 2.
    let mut raw = Vec::new();
    raw.extend_from_slice(&NZRP_MAGIC.to_le_bytes());
    raw.extend_from_slice(&2i32.to_le_bytes());
    raw.push(0); // empty .NET string
    raw.extend_from_slice(&0i32.to_le_bytes());
    raw.extend_from_slice(&0i32.to_le_bytes());
    raw.extend_from_slice(&2i32.to_le_bytes());
    assert!(NzReplayFixture::parse(&raw).is_err()); // segments are truncated

    raw.extend_from_slice(&1i32.to_le_bytes());
    raw.push(b'x');
    raw.extend_from_slice(&1i32.to_le_bytes());
    raw.push(b'y');
    let fixture = NzReplayFixture::parse(&raw).expect("valid minimal fixture");
    assert_eq!(fixture.segments, vec![b"x".to_vec(), b"y".to_vec()]);
    assert_eq!(fixture.handshake(), b"x");

    raw.push(0);
    assert!(NzReplayFixture::parse(&raw).is_err()); // trailing byte
}
