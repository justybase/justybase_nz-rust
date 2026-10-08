//! Hostile length fields must be rejected *before* allocation.
//!
//! A tracking global allocator records the largest single allocation made by
//! the current thread while measurement is enabled. Each case feeds a declared
//! length (negative, tiny, `MAX_PROTOCOL_PAYLOAD + 1`, `i32::MAX`,
//! 2 000 000 000, ...) to a parser or to a client over the mock backend, and
//! asserts both a clean error and a small peak allocation. Client cases run on
//! a current-thread runtime so the driver's reads happen on the measured
//! thread.

mod support;

use nz_rust::buffer::ReadBuffer;
use nz_rust::error::MAX_PROTOCOL_PAYLOAD;
use nz_rust::tuple_desc::{parse_row_description, ColumnDesc, DbosTupleDesc};
use nz_rust::NzError;
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use support::*;

struct TrackingAllocator;

thread_local! {
    static TRACKING: Cell<bool> = const { Cell::new(false) };
    static PEAK: Cell<usize> = const { Cell::new(0) };
}

fn record(size: usize) {
    let _ = TRACKING.try_with(|tracking| {
        if tracking.get() {
            let _ = PEAK.try_with(|peak| peak.set(peak.get().max(size)));
        }
    });
}

unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record(layout.size());
        System.alloc(layout)
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        record(layout.size());
        System.alloc_zeroed(layout)
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        record(new_size);
        System.realloc(ptr, layout, new_size)
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout)
    }
}

#[global_allocator]
static GLOBAL: TrackingAllocator = TrackingAllocator;

/// Largest single allocation made by `f` on this thread.
fn peak_allocation<T>(f: impl FnOnce() -> T) -> (T, usize) {
    PEAK.with(|peak| peak.set(0));
    TRACKING.with(|tracking| tracking.set(true));
    let value = f();
    TRACKING.with(|tracking| tracking.set(false));
    (value, PEAK.with(Cell::get))
}

const SMALL: usize = 256 * 1024;

const HOSTILE_LENGTHS: [i32; 6] = [
    -1,
    i32::MIN,
    MAX_PROTOCOL_PAYLOAD + 1,
    1_000_000_000,
    2_000_000_000,
    i32::MAX,
];

#[test]
fn read_buffer_rejects_hostile_lengths_before_allocating() {
    for declared in HOSTILE_LENGTHS {
        let mut wire = declared.to_be_bytes().to_vec();
        wire.extend_from_slice(b"short body");
        let (result, peak) = peak_allocation(|| {
            let mut reader = ChunkedReader::new(wire.clone(), Chunking::Fixed(3));
            let mut buffer = ReadBuffer::new();
            let len = buffer.read_i32(&mut reader)?;
            let len = nz_rust::error::validate_protocol_length(len, "test", true)? as usize;
            buffer.read_bytes(&mut reader, len)
        });
        assert!(
            matches!(result, Err(NzError::Protocol(_))),
            "{declared}: {result:?}"
        );
        assert!(peak < SMALL, "{declared}: peak allocation {peak} bytes");
    }
    // Lengths that do not fit i32 are rejected by the buffer itself.
    let (result, peak) = peak_allocation(|| {
        let mut reader = ChunkedReader::new(vec![0; 16], Chunking::Whole);
        ReadBuffer::new().read_bytes(&mut reader, usize::MAX)
    });
    assert!(matches!(result, Err(NzError::Protocol(_))));
    assert!(peak < SMALL);
}

#[test]
fn row_description_column_count_does_not_preallocate_beyond_payload() {
    for count in [1u16, 255, 4_096, u16::MAX] {
        let mut payload = count.to_be_bytes().to_vec();
        payload.extend_from_slice(b"A\0");
        let (result, peak) = peak_allocation(|| parse_row_description(&payload));
        assert!(matches!(result, Err(NzError::Protocol(_))), "{count}");
        assert!(
            peak < 4 * 1024,
            "declared {count} columns in a {}-byte payload allocated {peak} bytes",
            payload.len()
        );
    }
}

fn int_column(name: &str) -> ColumnDesc {
    ColumnDesc {
        name: name.into(),
        type_oid: OID_INT4,
        type_len: 4,
        type_mod: -1,
        format: 0,
    }
}

#[test]
fn dbos_descriptor_and_row_lengths_are_bounded() {
    // Descriptor declaring 100 000 fields in a 36-byte header.
    let mut header = vec![0u8; 32];
    header.extend_from_slice(&100_000i32.to_be_bytes());
    let (result, peak) = peak_allocation(|| DbosTupleDesc::parse(&header, None));
    assert!(matches!(result, Err(NzError::Protocol(_))));
    assert!(peak < SMALL, "descriptor peak {peak}");

    // Hostile field counts and sizes in an otherwise valid header.
    for value in [i32::MIN, -1, i32::MAX, 100_001] {
        for word in 4..9 {
            let mut payload = DbosLayout::ints(2).descriptor_payload();
            payload[word * 4..word * 4 + 4].copy_from_slice(&value.to_be_bytes());
            let (_, peak) = peak_allocation(|| DbosTupleDesc::parse(&payload, None));
            assert!(peak < SMALL, "word {word} = {value}: peak {peak}");
        }
    }

    // Varying field claiming 65 535 bytes in a short row.
    let layout = DbosLayout {
        kinds: vec![DbosKind::Int4, DbosKind::Varchar(16)],
        phys: vec![0, 1],
        nulls_allowed: true,
    };
    let descriptor = DbosTupleDesc::parse(&layout.descriptor_payload(), None).unwrap();
    let mut row = layout.row_payload(&[Some(DbosCell::Int4(1)), Some(DbosCell::Text("ab".into()))]);
    let varying_at = row.len() - 4;
    row[varying_at..varying_at + 2].copy_from_slice(&u16::MAX.to_le_bytes());
    let (result, peak) = peak_allocation(|| descriptor.parse_row(&row));
    assert!(matches!(result, Err(NzError::Protocol(_))));
    assert!(peak < SMALL);
}

#[test]
fn text_cell_lengths_and_error_fields_are_bounded() {
    let columns = vec![int_column("A"), int_column("B")];
    for declared in HOSTILE_LENGTHS.into_iter().chain([0, 1, 3]) {
        let mut payload = vec![0b1100_0000];
        payload.extend_from_slice(&declared.to_be_bytes());
        payload.extend_from_slice(b"12");
        let (result, peak) =
            peak_allocation(|| nz_rust::types::text::parse_text_data_row(&payload, &columns));
        assert!(result.is_err(), "{declared}");
        assert!(peak < SMALL, "{declared}: peak {peak}");
    }
    // Error/notice field parsing allocates proportionally to the input only.
    let mut hostile = Vec::new();
    for _ in 0..1_000 {
        hostile.extend_from_slice(b"M\0");
    }
    hostile.extend_from_slice(&[0xff; 4_096]);
    let (_, peak) = peak_allocation(|| nz_rust::error::parse_backend_error_fields(&hostile));
    assert!(peak < SMALL, "error fields peak {peak}");
}

fn hostile_server() -> MockServer {
    MockServer::start(HandshakeScript::default(), |session| {
        while let Some(sql) = session.read_query() {
            let (kind, declared) = sql
                .strip_prefix("SELECT ")
                .and_then(|rest| rest.split_once(' '))
                .expect("SELECT <kind> <len>");
            let declared: i32 = declared.parse().unwrap();
            let mut wire = Vec::new();
            match kind {
                "D" | "Y" => {
                    wire.extend(row_description(&[("A", OID_INT4, 4)]));
                    if kind == "Y" {
                        wire.extend(dbos_descriptor(&DbosLayout::ints(1)));
                        wire.extend_from_slice(&[b'Y', 0, 0, 0, 0, 0, 0, 0, 0]);
                        wire.extend_from_slice(&declared.to_be_bytes());
                    } else {
                        wire.extend(frame_header(b'D', declared));
                    }
                }
                other => wire.extend(frame_header(other.as_bytes()[0], declared)),
            }
            wire.extend_from_slice(b"tail");
            if session.send(&wire).is_err() {
                return;
            }
        }
    })
}

#[test]
fn native_client_rejects_hostile_frame_lengths_before_allocating() {
    let server = hostile_server();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    for kind in ["T", "D", "X", "Y", "C", "N", "E"] {
        for declared in HOSTILE_LENGTHS {
            let client = runtime
                .block_on(nz_rust::Client::connect(&server.config()))
                .unwrap();
            let sql = format!("SELECT {kind} {declared}");
            let (result, peak) = peak_allocation(|| {
                runtime.block_on(async {
                    tokio::time::timeout(
                        std::time::Duration::from_secs(10),
                        client.query(&sql, &[]),
                    )
                    .await
                })
            });
            let result = result.unwrap_or_else(|_| panic!("{sql}: client hung"));
            assert!(
                matches!(result, Err(NzError::Protocol(_))),
                "{sql}: {result:?}"
            );
            assert!(peak < SMALL, "{sql}: peak allocation {peak} bytes");
            let _ = runtime.block_on(client.close());
        }
    }
    server.assert_no_handler_panics();
}

#[cfg(feature = "compat")]
#[test]
fn legacy_connection_rejects_hostile_frame_lengths_before_allocating() {
    let server = hostile_server();
    for kind in ["T", "D", "X", "Y", "C", "N", "E"] {
        for declared in HOSTILE_LENGTHS {
            let mut config = server.config();
            config.command_timeout = 10;
            let mut conn = nz_rust::NzConnection::connect(&config).unwrap();
            let sql = format!("SELECT {kind} {declared}");
            let (result, peak) = peak_allocation(|| conn.query(&sql, &[]));
            assert!(
                matches!(result, Err(NzError::Protocol(_))),
                "{sql}: {result:?}"
            );
            assert!(peak < SMALL, "{sql}: peak allocation {peak} bytes");
            assert!(conn.is_closed(), "{sql}");
        }
    }
    server.assert_no_handler_panics();
}
