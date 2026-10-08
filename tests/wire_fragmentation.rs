//! TCP fragmentation qualification.
//!
//! TCP does not preserve `write()` boundaries, so every backend message must
//! decode identically whether it arrives in one segment, byte by byte, or in
//! arbitrary (seeded) pieces. Each scenario is replayed under the whole
//! [`Chunking::matrix`] — handshake (`R`, `K`, `Z`) included — and the decoded
//! output is compared with the unfragmented baseline and with hand-written
//! expectations.

mod support;

use nz_rust::buffer::ReadBuffer;
use nz_rust::{NzError, NzValue};
use support::*;

const SQL_TEXT: &str = "SELECT text_rows";
const SQL_DBOS: &str = "SELECT dbos_rows";
const SQL_NOTICE: &str = "SELECT with_notices";
const SQL_ERROR: &str = "SELECT broken";
const SQL_MULTI: &str = "SELECT multi";
const SQL_INSERT: &str = "INSERT rows";

fn dbos_layout() -> DbosLayout {
    DbosLayout {
        kinds: vec![DbosKind::Int4, DbosKind::Varchar(64), DbosKind::Int4],
        phys: vec![0, 1, 2],
        nulls_allowed: true,
    }
}

fn respond(sql: &str) -> Vec<u8> {
    match sql {
        SQL_TEXT => {
            let mut wire = row_description(&[("ONE", OID_INT4, 4), ("TXT", OID_VARCHAR, -1)]);
            wire.extend(text_row(&[Some(b"-2147483648"), Some(b"first")]));
            wire.extend(text_row(&[None, Some("Zażółć".as_bytes())]));
            wire.extend(text_row(&[Some(b"2147483647"), None]));
            wire.extend(command_complete("SELECT 3"));
            wire.extend(ready());
            wire
        }
        SQL_DBOS => {
            let layout = dbos_layout();
            let mut wire = row_description(&[
                ("A", OID_INT4, 4),
                ("B", OID_VARCHAR, -1),
                ("C", OID_INT4, 4),
            ]);
            wire.extend(dbos_descriptor(&layout));
            for cells in [
                [
                    Some(DbosCell::Int4(1)),
                    Some(DbosCell::Text("odd".into())),
                    Some(DbosCell::Int4(-1)),
                ],
                [None, Some(DbosCell::Text("even".into())), None],
                [
                    Some(DbosCell::Int4(i32::MAX)),
                    None,
                    Some(DbosCell::Int4(0)),
                ],
            ] {
                wire.extend(dbos_row_frame(&layout.row_payload(&cells)));
            }
            wire.extend(command_complete("SELECT 3"));
            wire.extend(ready());
            wire
        }
        SQL_NOTICE => {
            let mut wire = notice("before");
            wire.extend(row_description(&[("ONE", OID_INT4, 4)]));
            wire.extend(text_row(&[Some(b"7")]));
            wire.extend(notice("after"));
            wire.extend(command_complete("SELECT 1"));
            wire.extend(ready());
            wire
        }
        SQL_ERROR => {
            let mut wire = error("42000", "syntax error near broken");
            wire.extend(ready());
            wire
        }
        SQL_MULTI => {
            let mut wire = Vec::new();
            for value in [b"1", b"2"] {
                wire.extend(row_description(&[("V", OID_INT4, 4)]));
                wire.extend(text_row(&[Some(value)]));
                wire.extend(command_complete("SELECT 1"));
            }
            wire.extend(ready());
            wire
        }
        SQL_INSERT => simple_command("INSERT 0 5"),
        other => panic!("unexpected SQL {other}"),
    }
}

fn values(rows: &[nz_rust::Row]) -> Vec<Vec<NzValue>> {
    rows.iter()
        .map(|row| row.try_values().expect("row decodes").to_vec())
        .collect()
}

/// Everything observable from one pass over the scenarios.
#[derive(Debug, PartialEq)]
struct Observed {
    text: Vec<Vec<NzValue>>,
    dbos: Vec<Vec<NzValue>>,
    notice_rows: Vec<Vec<NzValue>>,
    notices: Vec<String>,
    error: String,
    multi: Vec<Vec<Vec<NzValue>>>,
    inserted: i64,
    after_error: Vec<Vec<NzValue>>,
}

fn expected() -> Observed {
    use NzValue::*;
    Observed {
        text: vec![
            vec![Int4(i32::MIN), Text("first".into())],
            vec![Null, Text("Zażółć".into())],
            vec![Int4(i32::MAX), Null],
        ],
        dbos: vec![
            vec![Int4(1), Text("odd".into()), Int4(-1)],
            vec![Null, Text("even".into()), Null],
            vec![Int4(i32::MAX), Null, Int4(0)],
        ],
        notice_rows: vec![vec![Int4(7)]],
        notices: vec!["before".into(), "after".into()],
        error: "42000".into(),
        multi: vec![vec![vec![Int4(1)]], vec![vec![Int4(2)]]],
        inserted: 5,
        after_error: vec![vec![Int4(i32::MIN), Text("first".into())]],
    }
}

async fn observe_native(server: &MockServer) -> Observed {
    let client = nz_rust::Client::connect(&server.config())
        .await
        .expect("fragmented handshake");
    let text = values(&client.query(SQL_TEXT, &[]).await.unwrap());
    let dbos = values(&client.query(SQL_DBOS, &[]).await.unwrap());
    let notice_result = client.query_multi(SQL_NOTICE, &[]).await.unwrap();
    let error = match client.query(SQL_ERROR, &[]).await {
        Err(NzError::Database(db)) => db.code.clone().expect("sqlstate"),
        other => panic!("expected database error, got {other:?}"),
    };
    let multi = client
        .query_multi(SQL_MULTI, &[])
        .await
        .unwrap()
        .result_sets
        .iter()
        .map(|set| values(&set.rows))
        .collect();
    let inserted = client.execute(SQL_INSERT, &[]).await.unwrap();
    // The session is still in sync after a SQL error.
    let after_error = values(&client.query(SQL_TEXT, &[]).await.unwrap()[..1]);
    client.close().await.unwrap();
    Observed {
        text,
        dbos,
        notice_rows: values(&notice_result.result_sets[0].rows),
        notices: notice_result.notices,
        error,
        multi,
        inserted,
        after_error,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_client_decodes_identically_under_every_fragmentation_plan() {
    let mut baseline = None;
    for chunking in Chunking::matrix() {
        let server = MockServer::start(
            HandshakeScript {
                chunking,
                ..Default::default()
            },
            |session| session.serve_all(respond),
        );
        let observed = observe_native(&server).await;
        server.assert_no_handler_panics();
        assert_eq!(observed, expected(), "chunking {chunking:?}");
        match &baseline {
            None => baseline = Some(observed),
            Some(baseline) => assert_eq!(&observed, baseline, "chunking {chunking:?}"),
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_stream_and_batches_decode_identically_under_fragmentation() {
    use futures_core::Stream;
    use std::pin::Pin;
    for chunking in Chunking::matrix() {
        let server = MockServer::start(
            HandshakeScript {
                chunking,
                ..Default::default()
            },
            |session| session.serve_all(respond),
        );
        let client = nz_rust::Client::connect(&server.config()).await.unwrap();
        let mut stream = client.query_stream(SQL_DBOS, &[]).await.unwrap();
        let mut streamed = Vec::new();
        while let Some(row) = std::future::poll_fn(|cx| Pin::new(&mut stream).poll_next(cx)).await {
            streamed.push(row.unwrap().try_values().unwrap().to_vec());
        }
        assert_eq!(streamed, expected().dbos, "chunking {chunking:?}");

        let mut batches = client.query_batches(SQL_TEXT, &[]).await.unwrap();
        let mut batched = Vec::new();
        while let Some(batch) =
            std::future::poll_fn(|cx| Pin::new(&mut batches).poll_next(cx)).await
        {
            batched.extend(values(&batch.unwrap()));
        }
        assert_eq!(batched, expected().text, "chunking {chunking:?}");
        client.close().await.unwrap();
        server.assert_no_handler_panics();
    }
}

#[cfg(feature = "compat")]
#[test]
fn legacy_connection_decodes_identically_under_every_fragmentation_plan() {
    for chunking in Chunking::matrix() {
        let server = MockServer::start(
            HandshakeScript {
                chunking,
                ..Default::default()
            },
            |session| session.serve_all(respond),
        );
        let mut conn = nz_rust::NzConnection::connect(&server.config()).expect("handshake");
        assert_eq!(conn.backend_process_id(), BASE_PID + 1);
        assert_eq!(conn.backend_secret_key(), SECRET_KEY);
        let text = values(&conn.query(SQL_TEXT, &[]).unwrap().result_sets[0].rows);
        let dbos = values(&conn.query(SQL_DBOS, &[]).unwrap().result_sets[0].rows);
        let notice_result = conn.query(SQL_NOTICE, &[]).unwrap();
        let error = match conn.query(SQL_ERROR, &[]) {
            Err(NzError::Database(db)) => db.code.clone().expect("sqlstate"),
            other => panic!("expected database error, got {other:?}"),
        };
        let multi = conn
            .query(SQL_MULTI, &[])
            .unwrap()
            .result_sets
            .iter()
            .map(|set| values(&set.rows))
            .collect();
        let inserted = conn.execute(SQL_INSERT, &[]).unwrap();
        let after_error = values(&conn.query(SQL_TEXT, &[]).unwrap().result_sets[0].rows[..1]);
        conn.close();
        let observed = Observed {
            text,
            dbos,
            notice_rows: values(&notice_result.result_sets[0].rows),
            notices: notice_result.notices,
            error,
            multi,
            inserted,
            after_error,
        };
        server.assert_no_handler_panics();
        assert_eq!(observed, expected(), "chunking {chunking:?}");
    }
}

/// Minimal query-phase frame splitter over [`ReadBuffer`] — the reading
/// primitive of the legacy engine — used to prove reassembly at *every*
/// single split point, which a TCP mock cannot guarantee.
fn read_frames(reader: &mut ChunkedReader) -> Result<Vec<(u8, Vec<u8>)>, NzError> {
    let mut buffer = ReadBuffer::new();
    let mut frames = Vec::new();
    loop {
        let kind = buffer.read_byte(reader)?;
        buffer.skip(reader, 4)?;
        if kind == b'Z' {
            frames.push((kind, Vec::new()));
            return Ok(frames);
        }
        if kind == b'Y' {
            buffer.skip(reader, 4)?;
        }
        let len = buffer.read_i32(reader)?;
        let len = nz_rust::error::validate_protocol_length(len, "test", true)? as usize;
        frames.push((kind, buffer.read_bytes(reader, len)?));
    }
}

#[test]
fn read_buffer_reassembles_frames_at_every_single_split_point() {
    for sql in [
        SQL_TEXT, SQL_DBOS, SQL_NOTICE, SQL_ERROR, SQL_MULTI, SQL_INSERT,
    ] {
        let wire = respond(sql);
        let baseline = read_frames(&mut ChunkedReader::new(wire.clone(), Chunking::Whole)).unwrap();
        assert_eq!(baseline.last().map(|f| f.0), Some(b'Z'));
        for boundary in 0..=wire.len() {
            let split = read_frames(&mut ChunkedReader::split_at(wire.clone(), boundary)).unwrap();
            assert_eq!(split, baseline, "{sql} split at {boundary}");
        }
        for chunking in Chunking::matrix() {
            let chunked = read_frames(&mut ChunkedReader::new(wire.clone(), chunking)).unwrap();
            assert_eq!(chunked, baseline, "{sql} {chunking:?}");
        }
    }
}

#[test]
fn read_buffer_reports_eof_inside_a_frame_as_closed_not_panic() {
    let wire = respond(SQL_TEXT);
    for cut in 1..wire.len() {
        let truncated = wire[..cut].to_vec();
        match read_frames(&mut ChunkedReader::new(truncated, Chunking::Fixed(3))) {
            Err(NzError::Closed(_)) => {}
            other => panic!("cut at {cut}: expected Closed, got {other:?}"),
        }
    }
}
