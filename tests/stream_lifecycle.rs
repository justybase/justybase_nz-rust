//! Streaming lifecycle: abandoning a `RowStream`, `RowBatchStream` or blocking
//! `RowIter` at any point must leave the session synchronized (cancel + drain
//! to ReadyForQuery) or closed — never reusable in an unknown state.
//!
//! The mock streams a large result in chunks and switches to the canceled
//! response as soon as it observes the out-of-band cancel, like the appliance.

mod support;

use futures_core::Stream;
use nz_rust::{NzError, NzValue};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use support::*;

const BIG_ROWS: usize = 50_000;
const CHUNK_ROWS: usize = 100;

/// Serve `SELECT big` (streamed, cancellable), `SELECT small <n>` and
/// `SELECT ok`. Records the SQL of every query.
fn start_streaming_server() -> (MockServer, Arc<Mutex<Vec<String>>>) {
    let queries: Arc<Mutex<Vec<String>>> = Arc::default();
    let log = queries.clone();
    let server = MockServer::start(HandshakeScript::default(), move |session| {
        while let Some(sql) = session.read_query() {
            log.lock().unwrap().push(sql.clone());
            let sent = if sql == "SELECT big" {
                stream_big(session)
            } else if let Some(n) = sql.strip_prefix("SELECT small ") {
                session.send(&select_rows(n.parse().unwrap()))
            } else {
                session.send(&select_one())
            };
            if sent.is_err() {
                return;
            }
        }
    });
    (server, queries)
}

fn stream_big(session: &mut Session) -> std::io::Result<()> {
    let cancels_before = session.cancels.count();
    session.send(&row_description(&[
        ("ONE", OID_INT4, 4),
        ("TXT", OID_VARCHAR, -1),
    ]))?;
    for chunk in 0..BIG_ROWS / CHUNK_ROWS {
        if session.cancels.count() > cancels_before {
            return session.send(&cancelled_response());
        }
        let mut wire = Vec::new();
        for n in chunk * CHUNK_ROWS + 1..=(chunk + 1) * CHUNK_ROWS {
            let number = n.to_string();
            wire.extend(text_row(&[Some(number.as_bytes()), Some(b"row")]));
        }
        session.send(&wire)?;
    }
    let mut tail = command_complete(&format!("SELECT {BIG_ROWS}"));
    tail.extend(ready());
    session.send(&tail)
}

async fn next<S: Stream + Unpin>(stream: &mut S) -> Option<S::Item> {
    tokio::time::timeout(
        Duration::from_secs(10),
        std::future::poll_fn(|cx| Pin::new(&mut *stream).poll_next(cx)),
    )
    .await
    .expect("stream stalled")
}

fn expected_row(n: usize) -> Vec<NzValue> {
    vec![NzValue::Int4(n as i32), NzValue::Text("row".into())]
}

/// After an abandoned stream the same physical session must answer the next
/// query correctly.
async fn assert_session_reusable(client: &nz_rust::Client, server: &MockServer, label: &str) {
    let rows = tokio::time::timeout(Duration::from_secs(10), client.query("SELECT ok", &[]))
        .await
        .unwrap_or_else(|_| panic!("{label}: next query hung"))
        .unwrap_or_else(|e| panic!("{label}: next query failed: {e}"));
    assert_eq!(rows.len(), 1, "{label}");
    assert_eq!(rows[0].try_values().unwrap(), expected_row(1), "{label}");
    assert!(!client.is_closed(), "{label}");
    assert_eq!(server.accepted(), 1, "{label}: session was replaced");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn row_stream_drop_at_any_point_then_reuse() {
    let (server, queries) = start_streaming_server();
    let client = nz_rust::Client::connect(&server.config()).await.unwrap();

    // Before the first row.
    let stream = client.query_stream("SELECT big", &[]).await.unwrap();
    drop(stream);
    assert_session_reusable(&client, &server, "drop before first row").await;
    // Whether the driver task picked the statement up before the drop was
    // observed is a scheduling matter: either it never reached the backend,
    // or it did and was cancelled and drained. Never anything in between.
    let sent_before_drop = queries.lock().unwrap().iter().any(|q| q == "SELECT big");
    if sent_before_drop {
        assert!(
            server.cancels.wait_for(1, Duration::from_secs(5)),
            "a statement sent before the drop was not cancelled"
        );
    }
    assert_eq!(server.cancels.count(), usize::from(sent_before_drop));
    let phase_one_cancels = server.cancels.count();

    // After exactly one row.
    let mut stream = client.query_stream("SELECT big", &[]).await.unwrap();
    assert_eq!(
        next(&mut stream)
            .await
            .unwrap()
            .unwrap()
            .try_values()
            .unwrap(),
        expected_row(1)
    );
    drop(stream);
    assert_session_reusable(&client, &server, "drop after one row").await;

    // In the middle of an internal 256-row batch.
    let mut stream = client.query_stream("SELECT big", &[]).await.unwrap();
    for n in 1..=300 {
        let row = next(&mut stream).await.unwrap().unwrap();
        assert_eq!(row.try_values().unwrap(), expected_row(n));
    }
    drop(stream);
    assert_session_reusable(&client, &server, "drop mid batch").await;

    // At the end: fully consumed, no cancel may be sent.
    let cancels = server.cancels.count();
    let mut stream = client.query_stream("SELECT small 3", &[]).await.unwrap();
    for n in 1..=3 {
        let row = next(&mut stream).await.unwrap().unwrap();
        assert_eq!(row.try_values().unwrap(), expected_row(n));
    }
    assert!(next(&mut stream).await.is_none());
    drop(stream);
    assert_session_reusable(&client, &server, "drop at end").await;
    assert_eq!(
        server.cancels.count(),
        cancels,
        "cancel sent after completion"
    );

    // Every abandoned in-flight big stream was cancelled out of band.
    assert_eq!(server.cancels.count(), phase_one_cancels + 2);
    client.close().await.unwrap();
    server.assert_no_handler_panics();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn query_batches_drop_at_any_point_then_reuse() {
    let (server, _) = start_streaming_server();
    let client = nz_rust::Client::connect(&server.config()).await.unwrap();

    let batches = client.query_batches("SELECT big", &[]).await.unwrap();
    drop(batches);
    assert_session_reusable(&client, &server, "batches drop before first").await;

    let mut batches = client.query_batches("SELECT big", &[]).await.unwrap();
    let first = next(&mut batches).await.unwrap().unwrap();
    assert_eq!(first[0].try_values().unwrap(), expected_row(1));
    drop(batches);
    assert_session_reusable(&client, &server, "batches drop after first").await;

    let mut batches = client.query_batches("SELECT big", &[]).await.unwrap();
    let mut seen = 0;
    while seen < 1_000 {
        let batch = next(&mut batches).await.unwrap().unwrap();
        for row in batch.iter() {
            seen += 1;
            assert_eq!(row.try_values().unwrap(), expected_row(seen));
        }
    }
    drop(batches);
    assert_session_reusable(&client, &server, "batches drop mid response").await;
    client.close().await.unwrap();
    server.assert_no_handler_panics();
}

/// A partially read stream that is kept alive while the same client runs
/// other work must neither deadlock that work nor lose its own rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn held_partial_stream_does_not_block_other_queries() {
    let (server, _) = start_streaming_server();
    let client = nz_rust::Client::connect(&server.config()).await.unwrap();
    let mut first = client.query_stream("SELECT small 3", &[]).await.unwrap();
    assert_eq!(
        next(&mut first)
            .await
            .unwrap()
            .unwrap()
            .try_values()
            .unwrap(),
        expected_row(1)
    );
    // Buffered query on the same session.
    assert_session_reusable(&client, &server, "query while stream held").await;
    // A second stream on a clone while the first still holds rows.
    let clone = client.clone();
    let mut second = tokio::time::timeout(
        Duration::from_secs(10),
        clone.query_stream("SELECT small 2", &[]),
    )
    .await
    .expect("second stream setup stalled")
    .unwrap();
    for n in 1..=2 {
        let row = next(&mut second).await.unwrap().unwrap();
        assert_eq!(row.try_values().unwrap(), expected_row(n));
    }
    assert!(next(&mut second).await.is_none());
    // The held stream still yields its remaining rows in order.
    for n in 2..=3 {
        let row = next(&mut first).await.unwrap().unwrap();
        assert_eq!(row.try_values().unwrap(), expected_row(n));
    }
    assert!(next(&mut first).await.is_none());
    client.close().await.unwrap();
    server.assert_no_handler_panics();
}

#[test]
fn blocking_row_iter_drop_mid_result_then_reuse() {
    let (server, queries) = start_streaming_server();
    let mut client = nz_rust::blocking::Client::connect(&server.config()).unwrap();
    {
        let mut rows = client.query_iter("SELECT big", &[]).unwrap();
        for n in 1..=10 {
            assert_eq!(
                rows.next().unwrap().unwrap().try_values().unwrap(),
                expected_row(n)
            );
        }
    }
    let rows = client.query("SELECT ok", &[]).unwrap();
    assert_eq!(rows[0].try_values().unwrap(), expected_row(1));
    assert!(!client.is_closed());
    assert_eq!(server.accepted(), 1);
    assert!(server.cancels.count() >= 1);
    assert_eq!(queries.lock().unwrap().last().unwrap(), "SELECT ok");
    client.close().unwrap();
    server.assert_no_handler_panics();
}

/// When the cleanup drain cannot reach ReadyForQuery (the backend never ends
/// the abandoned response), the session must be closed rather than reused.
///
/// Streams deliver rows in batches of up to 256, so the mock sends more than
/// one batch before it stalls.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn abandoned_stream_without_terminal_response_closes_the_session() {
    let server = MockServer::start(HandshakeScript::default(), |session| {
        assert_eq!(session.read_query().as_deref(), Some("SELECT hang"));
        let mut wire = row_description(&[("ONE", OID_INT4, 4)]);
        for n in 1..=300 {
            wire.extend(text_row(&[Some(n.to_string().as_bytes())]));
        }
        session.send(&wire).unwrap();
        // Ignore the cancel and never send ReadyForQuery; keep the socket
        // open until the client hangs up.
        let mut sink = [0u8; 64];
        while std::io::Read::read(&mut session.stream, &mut sink).unwrap_or(0) > 0 {}
    });
    let client = nz_rust::Client::connect(&server.config()).await.unwrap();
    let mut stream = client.query_stream("SELECT hang", &[]).await.unwrap();
    assert!(next(&mut stream).await.unwrap().is_ok());
    drop(stream);
    // The cleanup budget is bounded (5 s); the next request must then fail
    // instead of reading the abandoned response.
    let next_query = tokio::time::timeout(Duration::from_secs(15), client.query("SELECT ok", &[]))
        .await
        .expect("cleanup did not finish");
    assert!(
        matches!(
            next_query,
            Err(NzError::Closed(_) | NzError::Io(_) | NzError::Protocol(_))
        ),
        "{next_query:?}"
    );
    assert!(wait_until(Duration::from_secs(5), || client.is_closed()));
    assert_eq!(server.accepted(), 1);
    assert!(server.cancels.wait_for(1, Duration::from_secs(5)));
    server.assert_no_handler_panics();
}
