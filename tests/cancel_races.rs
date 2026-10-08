//! Deterministic cancel races.
//!
//! The mock signals the test the moment statement A reaches a given point
//! (nothing sent, first row sent, half a frame sent, CommandComplete sent),
//! then waits for the out-of-band cancel packet before finishing A. The test
//! cancels exactly at that point and immediately starts statement B. B must
//! succeed with its own result, no extra cancel packet may reach the backend,
//! and the same physical session must be used throughout.

mod support;

use nz_rust::{NzError, NzValue};
use std::sync::Mutex;
use std::time::Duration;
use support::*;
use tokio::sync::mpsc;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Point {
    BeforeFirstRow,
    AfterFirstRow,
    MidFrame,
    AfterCommandComplete,
}

const POINTS: [Point; 4] = [
    Point::BeforeFirstRow,
    Point::AfterFirstRow,
    Point::MidFrame,
    Point::AfterCommandComplete,
];

fn sql_for(point: Point) -> String {
    format!("SELECT A {point:?}")
}

/// Serve statement A at every [`Point`] and `SELECT B <n>` (one row `n`).
fn start_server() -> (MockServer, mpsc::UnboundedReceiver<Point>) {
    let (reached, receiver) = mpsc::unbounded_channel();
    let reached = Mutex::new(reached);
    let server = MockServer::start(HandshakeScript::default(), move |session| {
        while let Some(sql) = session.read_query() {
            if let Some(n) = sql.strip_prefix("SELECT B ") {
                let mut wire = row_description(&[("B", OID_INT4, 4)]);
                wire.extend(text_row(&[Some(n.as_bytes())]));
                wire.extend(command_complete("SELECT 1"));
                wire.extend(ready());
                if session.send(&wire).is_err() {
                    return;
                }
                continue;
            }
            let point = POINTS
                .into_iter()
                .find(|point| sql == sql_for(*point))
                .unwrap_or_else(|| panic!("unexpected SQL {sql}"));
            let cancels_before = session.cancels.count();
            let header = row_description(&[("A", OID_INT4, 4)]);
            let row = text_row(&[Some(b"42")]);
            let (now, later) = match point {
                Point::BeforeFirstRow => (Vec::new(), cancelled_response()),
                Point::AfterFirstRow => {
                    let mut now = header;
                    now.extend(&row);
                    (now, cancelled_response())
                }
                Point::MidFrame => {
                    let mut now = header;
                    now.extend(&row[..row.len() - 2]);
                    let mut later = row[row.len() - 2..].to_vec();
                    later.extend(cancelled_response());
                    (now, later)
                }
                Point::AfterCommandComplete => {
                    let mut now = header;
                    now.extend(&row);
                    now.extend(command_complete("SELECT 1"));
                    (now, ready())
                }
            };
            session.send(&now).unwrap();
            reached.lock().unwrap().send(point).unwrap();
            assert!(
                session
                    .cancels
                    .wait_for(cancels_before + 1, Duration::from_secs(10)),
                "{point:?}: cancel packet never arrived"
            );
            if session.send(&later).is_err() {
                return;
            }
        }
    });
    (server, receiver)
}

fn b_value(rows: &[nz_rust::Row]) -> i32 {
    match rows[0].try_values().unwrap() {
        [NzValue::Int4(n)] => *n,
        other => panic!("unexpected B row {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_generation_does_not_cancel_next_query() {
    let (server, mut reached) = start_server();
    let client = nz_rust::Client::connect(&server.config()).await.unwrap();
    let mut expected_cancels = 0;
    for round in 0..5 {
        for point in POINTS {
            for double in [false, true] {
                let a_client = client.clone();
                let a = tokio::spawn(async move { a_client.query(&sql_for(point), &[]).await });
                assert_eq!(
                    tokio::time::timeout(Duration::from_secs(10), reached.recv())
                        .await
                        .unwrap(),
                    Some(point)
                );
                client.cancel().await.unwrap();
                if double {
                    client.cancel().await.unwrap();
                }
                // B is queued immediately, while A is still cleaning up.
                let b_client = client.clone();
                let b = tokio::spawn(async move {
                    b_client
                        .query(&format!("SELECT B {}", round * 10 + 1), &[])
                        .await
                });
                let a_result = tokio::time::timeout(Duration::from_secs(10), a)
                    .await
                    .unwrap()
                    .unwrap();
                match (&a_result, point) {
                    (Err(NzError::Cancelled(_)), _) => {}
                    // A finished before the interruption was observed.
                    (Ok(_), Point::AfterCommandComplete) => {}
                    other => panic!("{point:?}: unexpected A outcome {other:?}"),
                }
                let b_rows = tokio::time::timeout(Duration::from_secs(10), b)
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap_or_else(|e| panic!("{point:?} double={double}: B failed: {e}"));
                assert_eq!(b_value(&b_rows), round * 10 + 1);
                // Further statements are unaffected as well.
                let c = client.query("SELECT B 7", &[]).await.unwrap();
                assert_eq!(b_value(&c), 7);
                expected_cancels += 1;
                assert_eq!(
                    server.cancels.count(),
                    expected_cancels,
                    "{point:?} double={double}: unexpected extra cancel packet"
                );
            }
        }
    }
    assert_eq!(server.accepted(), 1);
    assert!(server.cancels.pids().iter().all(|pid| *pid == BASE_PID + 1));
    client.close().await.unwrap();
    server.assert_no_handler_panics();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancel_without_active_statement_sends_nothing() {
    let (server, _reached) = start_server();
    let client = nz_rust::Client::connect(&server.config()).await.unwrap();
    // Before any statement, and after one has fully finished.
    client.cancel().await.unwrap();
    assert_eq!(b_value(&client.query("SELECT B 1", &[]).await.unwrap()), 1);
    client.cancel().await.unwrap();
    client.cancel().await.unwrap();
    assert_eq!(b_value(&client.query("SELECT B 2", &[]).await.unwrap()), 2);
    assert_eq!(b_value(&client.query("SELECT B 3", &[]).await.unwrap()), 3);
    assert_eq!(server.cancels.count(), 0, "stray cancel packet");
    client.close().await.unwrap();
    assert!(matches!(client.cancel().await, Err(NzError::Closed(_))));
    server.assert_no_handler_panics();
}

#[cfg(feature = "compat")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn legacy_async_cancel_generation_does_not_cancel_next_query() {
    let (server, mut reached) = start_server();
    let conn = std::sync::Arc::new(
        nz_rust::AsyncNzConnection::connect(&server.config())
            .await
            .unwrap(),
    );
    conn.cancel().await.unwrap();
    let mut expected_cancels = 0;
    for point in POINTS {
        let a_conn = conn.clone();
        let a = tokio::spawn(async move { a_conn.query(&sql_for(point), &[]).await });
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(10), reached.recv())
                .await
                .unwrap(),
            Some(point)
        );
        conn.cancel().await.unwrap();
        let a_result = tokio::time::timeout(Duration::from_secs(10), a)
            .await
            .unwrap()
            .unwrap();
        match (&a_result, point) {
            (Err(NzError::Database(db)), _) => assert_eq!(db.code.as_deref(), Some("57014")),
            (Ok(_), Point::AfterCommandComplete) => {}
            other => panic!("{point:?}: unexpected A outcome {other:?}"),
        }
        let b = conn.query("SELECT B 5", &[]).await.unwrap();
        assert_eq!(b_value(&b.result_sets[0].rows), 5);
        // Cancelling now (no active statement) must not reach the backend.
        conn.cancel().await.unwrap();
        let b = conn.query("SELECT B 6", &[]).await.unwrap();
        assert_eq!(b_value(&b.result_sets[0].rows), 6);
        expected_cancels += 1;
        assert_eq!(server.cancels.count(), expected_cancels, "{point:?}");
    }
    assert_eq!(server.accepted(), 1);
    conn.close().await;
    server.assert_no_handler_panics();
}
