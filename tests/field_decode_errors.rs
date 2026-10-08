//! A value that cannot be decoded (invalid UTF-8 in a VARCHAR, a malformed
//! scalar) is a *data* problem, not a *framing* problem: the whole message was
//! consumed and the stream stays synchronized. The driver must report the
//! decode error for that statement and keep the session; framing faults (bad
//! lengths, truncation, inconsistent cell layout) must still retire it.
//!
//! Motivating case: a Latin (non-UTF-8) database returns non-UTF-8 VARCHAR
//! bytes on the binary path; one such row used to kill the pooled session.

mod support;

use nz_rust::NzError;
use std::time::Duration;
use support::*;

/// Replace the first occurrence of `needle` in `haystack` with `bytes`.
fn patch(haystack: &mut [u8], needle: &[u8], bytes: &[u8]) {
    assert_eq!(needle.len(), bytes.len());
    let at = haystack
        .windows(needle.len())
        .position(|w| w == needle)
        .expect("needle present");
    haystack[at..at + needle.len()].copy_from_slice(bytes);
}

fn text_varchar_row(cell: &[u8]) -> Vec<u8> {
    let mut wire = row_description(&[("ONE", OID_INT4, 4), ("TXT", OID_VARCHAR, -1)]);
    wire.extend(text_row(&[Some(b"1"), Some(cell)]));
    wire.extend(command_complete("SELECT 1"));
    wire.extend(ready());
    wire
}

fn respond(sql: &str) -> Vec<u8> {
    match sql {
        // Invalid UTF-8 in a VARCHAR cell on the text path.
        "TEXT_UTF8" => text_varchar_row(&[b'a', 0xf3, b'b']),
        // A scalar that is not a number in an INT4 column.
        "TEXT_SCALAR" => {
            let mut wire = row_description(&[("ONE", OID_INT4, 4)]);
            wire.extend(text_row(&[Some(b"12x")]));
            wire.extend(command_complete("SELECT 1"));
            wire.extend(ready());
            wire
        }
        // Invalid UTF-8 in a VARCHAR on the binary path.
        "DBOS_UTF8" => {
            let layout = DbosLayout {
                kinds: vec![DbosKind::Int4, DbosKind::Varchar(16)],
                phys: vec![0, 1],
                nulls_allowed: true,
            };
            let mut row =
                layout.row_payload(&[Some(DbosCell::Int4(1)), Some(DbosCell::Text("ab".into()))]);
            patch(&mut row, b"ab", &[0xf3, 0xff]);
            let mut wire = row_description(&[("ONE", OID_INT4, 4), ("TXT", OID_VARCHAR, -1)]);
            wire.extend(dbos_descriptor(&layout));
            wire.extend(dbos_row_frame(&row));
            wire.extend(command_complete("SELECT 1"));
            wire.extend(ready());
            wire
        }
        // Binary rows alternating bad/good, all arriving in one burst, followed
        // by a second result set: every row must be consumed exactly once.
        "DBOS_MANY" => {
            let layout = DbosLayout {
                kinds: vec![DbosKind::Int4, DbosKind::Varchar(16)],
                phys: vec![0, 1],
                nulls_allowed: true,
            };
            let mut wire = row_description(&[("ONE", OID_INT4, 4), ("TXT", OID_VARCHAR, -1)]);
            wire.extend(dbos_descriptor(&layout));
            for n in 1..=6 {
                let mut row = layout
                    .row_payload(&[Some(DbosCell::Int4(n)), Some(DbosCell::Text("ab".into()))]);
                if n % 2 == 1 {
                    patch(&mut row, b"ab", &[0xf3, 0xff]);
                }
                wire.extend(dbos_row_frame(&row));
            }
            wire.extend(command_complete("SELECT 6"));
            wire.extend(row_description(&[("N", OID_INT4, 4)]));
            wire.extend(text_row(&[Some(b"7")]));
            wire.extend(command_complete("SELECT 1"));
            wire.extend(ready());
            wire
        }
        // Bad row followed by a good one, then a second result set.
        "MANY" => {
            let mut wire = row_description(&[("TXT", OID_VARCHAR, -1)]);
            wire.extend(text_row(&[Some(&[0xf3])]));
            wire.extend(text_row(&[Some(b"fine")]));
            wire.extend(command_complete("SELECT 2"));
            wire.extend(row_description(&[("N", OID_INT4, 4)]));
            wire.extend(text_row(&[Some(b"7")]));
            wire.extend(command_complete("SELECT 1"));
            wire.extend(ready());
            wire
        }
        // A server error and an undecodable row in the same response: the
        // server's error is the one to report.
        "SERVER_ERROR_TOO" => {
            let mut wire = row_description(&[("TXT", OID_VARCHAR, -1)]);
            wire.extend(text_row(&[Some(&[0xf3])]));
            wire.extend(error("42000", "server side failure"));
            wire.extend(ready());
            wire
        }
        _ => select_one(),
    }
}

fn start() -> MockServer {
    MockServer::start(HandshakeScript::default(), |session| {
        session.serve_all(respond)
    })
}

async fn assert_session_kept(client: &nz_rust::Client, server: &MockServer, label: &str) {
    assert!(!client.is_closed(), "{label}: session was closed");
    let rows = tokio::time::timeout(Duration::from_secs(10), client.query("SELECT ok", &[]))
        .await
        .unwrap_or_else(|_| panic!("{label}: next query hung"))
        .unwrap_or_else(|e| panic!("{label}: next query failed: {e}"));
    assert_eq!(rows.len(), 1, "{label}");
    assert_eq!(server.accepted(), 1, "{label}: session was replaced");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn undecodable_values_fail_the_statement_but_keep_the_session() {
    let server = start();
    let client = nz_rust::Client::connect(&server.config()).await.unwrap();
    for sql in ["TEXT_UTF8", "TEXT_SCALAR", "DBOS_UTF8", "DBOS_MANY", "MANY"] {
        let result = client.query_multi(sql, &[]).await;
        assert!(
            matches!(result, Err(NzError::Protocol(_))),
            "{sql}: expected the decode error, got {result:?}"
        );
        assert_session_kept(&client, &server, sql).await;
    }
    // Repeatedly, to prove the stream stays aligned.
    for _ in 0..5 {
        assert!(client.query("DBOS_UTF8", &[]).await.is_err());
    }
    assert_session_kept(&client, &server, "repeat").await;
    client.close().await.unwrap();
    server.assert_no_handler_panics();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_server_error_wins_over_a_decode_error() {
    let server = start();
    let client = nz_rust::Client::connect(&server.config()).await.unwrap();
    let result = client.query("SERVER_ERROR_TOO", &[]).await;
    assert!(matches!(result, Err(NzError::Database(_))), "{result:?}");
    assert_session_kept(&client, &server, "server error").await;
    client.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pooled_sessions_survive_undecodable_values() {
    let server = start();
    let mut config = nz_rust::PoolConfig::new(server.config());
    config.max = 1;
    let pool = nz_rust::Pool::new(config).unwrap();
    for _ in 0..3 {
        let lease = pool.get().await.unwrap();
        assert!(lease.query("DBOS_UTF8", &[]).await.is_err());
        lease.release().await;
    }
    let lease = pool.get().await.unwrap();
    assert_eq!(lease.query("SELECT ok", &[]).await.unwrap().len(), 1);
    lease.release().await;
    assert_eq!(server.accepted(), 1, "pool replaced a healthy session");
    pool.close().await;
}

/// Lazy rows (streams, batches) decode per field on access, so the stream
/// completes and only the offending field errors.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lazy_rows_report_the_bad_field_only() {
    use futures_core::Stream;
    let server = start();
    let client = nz_rust::Client::connect(&server.config()).await.unwrap();
    let mut stream = client.query_stream("DBOS_UTF8", &[]).await.unwrap();
    let row = std::future::poll_fn(|cx| std::pin::Pin::new(&mut stream).poll_next(cx))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.try_get::<_, i32>(0).unwrap(), 1);
    // Lazy access reports the bad field (as a Config error: "invalid UTF-8
    // SQL text"); the other fields and the stream are unaffected.
    assert!(row.try_get::<_, String>(1).is_err());
    assert!(
        std::future::poll_fn(|cx| std::pin::Pin::new(&mut stream).poll_next(cx))
            .await
            .is_none()
    );
    drop(stream);
    assert_session_kept(&client, &server, "lazy").await;
}

/// Structural corruption inside a correctly framed row is still a protocol
/// fault: the session must be retired.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inconsistent_row_structure_still_retires_the_session() {
    let server = MockServer::start(HandshakeScript::default(), |session| {
        session.serve_all(|_| {
            let mut wire = row_description(&[("ONE", OID_INT4, 4), ("TXT", OID_VARCHAR, -1)]);
            // Present cell whose length is smaller than its own prefix.
            let mut cell = vec![0b1000_0000];
            cell.extend_from_slice(&2i32.to_be_bytes());
            wire.extend(frame(b'D', &cell));
            wire.extend(command_complete("SELECT 1"));
            wire.extend(ready());
            wire
        })
    });
    let client = nz_rust::Client::connect(&server.config()).await.unwrap();
    assert!(matches!(
        client.query("X", &[]).await,
        Err(NzError::Protocol(_))
    ));
    assert!(wait_until(Duration::from_secs(5), || client.is_closed()));
}

#[cfg(feature = "compat")]
mod legacy {
    use super::*;
    use nz_rust::{ColumnDesc, QueryStreamSink, Row};

    #[derive(Default)]
    struct RowCounter {
        rows: Vec<usize>,
    }

    impl QueryStreamSink for RowCounter {
        fn on_columns(
            &mut self,
            _set: usize,
            _columns: &[ColumnDesc],
            _nullability: Option<&[bool]>,
        ) -> Result<(), NzError> {
            Ok(())
        }
        fn on_row(&mut self, set: usize, _row: Row) -> Result<(), NzError> {
            if self.rows.len() <= set {
                self.rows.resize(set + 1, 0);
            }
            self.rows[set] += 1;
            Ok(())
        }
    }

    #[test]
    fn legacy_buffered_queries_keep_the_session_after_an_undecodable_value() {
        let server = start();
        let mut conn = nz_rust::NzConnection::connect(&server.config()).unwrap();
        for sql in [
            "TEXT_UTF8",
            "TEXT_SCALAR",
            "DBOS_UTF8",
            "DBOS_MANY",
            "MANY",
            "SERVER_ERROR_TOO",
        ] {
            for _ in 0..3 {
                let result = conn.query(sql, &[]);
                match (sql, &result) {
                    ("SERVER_ERROR_TOO", Err(NzError::Database(_))) => {}
                    ("SERVER_ERROR_TOO", other) => {
                        panic!("{sql}: the server's error must win, got {other:?}")
                    }
                    (_, Err(NzError::Protocol(_))) => {}
                    (_, other) => panic!("{sql}: expected the decode error, got {other:?}"),
                }
                assert!(!conn.is_closed(), "{sql}: connection was closed");
            }
        }
        let rows = conn.query("SELECT ok", &[]).unwrap().result_sets[0]
            .rows
            .len();
        assert_eq!(rows, 1);
        assert_eq!(server.accepted(), 1);
        server.assert_no_handler_panics();
    }

    /// Streaming sinks receive the decodable rows; the statement still
    /// reports the decode error and the session stays aligned.
    #[test]
    fn legacy_streaming_keeps_the_session_and_delivers_the_good_rows() {
        let server = start();
        let mut conn = nz_rust::NzConnection::connect(&server.config()).unwrap();
        for (sql, expected) in [("DBOS_MANY", vec![3, 1]), ("MANY", vec![1, 1])] {
            let mut sink = RowCounter::default();
            let result = conn.execute_stream(sql, &[], &mut sink);
            assert!(
                matches!(result, Err(NzError::Protocol(_))),
                "{sql}: {result:?}"
            );
            assert_eq!(sink.rows, expected, "{sql}: rows delivered to the sink");
            assert!(!conn.is_closed(), "{sql}");
        }
        assert_eq!(
            conn.query("SELECT ok", &[]).unwrap().result_sets[0]
                .rows
                .len(),
            1
        );
        assert_eq!(server.accepted(), 1);
    }

    #[test]
    fn legacy_structural_corruption_still_retires_the_connection() {
        let server = MockServer::start(HandshakeScript::default(), |session| {
            session.serve_all(|_| {
                let mut wire = row_description(&[("ONE", OID_INT4, 4)]);
                let mut cell = vec![0b1000_0000];
                cell.extend_from_slice(&2i32.to_be_bytes());
                wire.extend(frame(b'D', &cell));
                wire.extend(command_complete("SELECT 1"));
                wire.extend(ready());
                wire
            })
        });
        let mut conn = nz_rust::NzConnection::connect(&server.config()).unwrap();
        assert!(matches!(conn.query("X", &[]), Err(NzError::Protocol(_))));
        assert!(conn.is_closed());
    }
}
