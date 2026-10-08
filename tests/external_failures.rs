//! External-table import/export failure paths against the mock backend.
//!
//! The appliance drives these transfers with `l` (import), `u`/`U` (export)
//! messages in the middle of a statement. Failures on the client side (denied
//! or missing files, a reader that errors mid-stream) must be reported to the
//! server as a transfer error and leave the session synchronized; malformed
//! requests (hostile lengths) must fail with a protocol error and retire the
//! session. Nothing may be written outside the permitted directory.

mod support;

use nz_rust::{ExternalFilePolicy, NzConnectionConfig, NzError};
use std::io::Read;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;
use support::*;
use tokio::io::{AsyncRead, ReadBuf};

/// What the client sent for one transfer.
#[derive(Debug, Default, Clone, PartialEq)]
struct Transfer {
    /// Import: data chunks. Export: unused.
    chunks: Vec<Vec<u8>>,
    /// Import: final status word (3 = done, 2 = error). Export: the start
    /// status (0 = accepted, 1 = refused).
    status: Option<i32>,
}

type Log = Arc<Mutex<Vec<Transfer>>>;

fn temp_path(tag: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "nz_rust_ext_{tag}_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

fn read_i32(session: &mut Session) -> i32 {
    let mut word = [0u8; 4];
    session.stream.read_exact(&mut word).unwrap();
    i32::from_be_bytes(word)
}

fn error_response() -> Vec<u8> {
    let mut wire = error("58030", "external table transfer failed");
    wire.extend(ready());
    wire
}

fn import_request(name: &[u8], buffer_size: i32, session: &mut Session) -> Option<Transfer> {
    let mut frame = vec![b'l'];
    frame.extend_from_slice(&[0; 8]);
    frame.extend_from_slice(name);
    if name.len() <= 4096 {
        frame.push(0);
        frame.extend_from_slice(&1i32.to_be_bytes());
    }
    session.send(&frame).unwrap();
    if name.len() > 4096 {
        return None;
    }
    assert_eq!(read_i32(session), 1, "client protocol version");
    session.send(&0i32.to_be_bytes()).unwrap();
    session.send(&buffer_size.to_be_bytes()).unwrap();
    if !(0..=10_000_000).contains(&buffer_size) {
        return None;
    }
    let mut transfer = Transfer::default();
    loop {
        match read_i32(session) {
            1 => {
                let size = read_i32(session) as usize;
                let mut chunk = vec![0; size];
                session.stream.read_exact(&mut chunk).unwrap();
                transfer.chunks.push(chunk);
            }
            status @ (2 | 3) => {
                transfer.status = Some(status);
                return Some(transfer);
            }
            other => panic!("unexpected import status {other}"),
        }
    }
}

fn export_start(path: &str, session: &mut Session) -> Transfer {
    let mut start = vec![b'u'];
    start.extend_from_slice(&[0; 4 + 10 + 16]);
    start.extend_from_slice(&((path.len() + 1) as i32).to_be_bytes());
    start.extend_from_slice(path.as_bytes());
    start.push(0);
    session.send(&start).unwrap();
    Transfer {
        chunks: Vec::new(),
        status: Some(read_i32(session)),
    }
}

fn export_data(chunks: &[&[u8]], tail: i32) -> Vec<u8> {
    let mut data = vec![b'U'];
    data.extend_from_slice(&[0; 8]);
    for chunk in chunks {
        data.extend_from_slice(&1i32.to_be_bytes());
        data.extend_from_slice(&(chunk.len() as i32).to_be_bytes());
        data.extend_from_slice(chunk);
    }
    data.extend_from_slice(&tail.to_be_bytes());
    data
}

fn start_server(log: Log) -> MockServer {
    MockServer::start(HandshakeScript::default(), move |session| {
        while let Some(sql) = session.read_query() {
            let response = if let Some(name) = sql.strip_prefix("IMPORT ") {
                match import_request(name.as_bytes(), 8192, session) {
                    Some(transfer) => {
                        let failed = transfer.status == Some(2);
                        log.lock().unwrap().push(transfer);
                        if failed {
                            error_response()
                        } else {
                            simple_command("INSERT 0 3")
                        }
                    }
                    None => return,
                }
            } else if let Some(size) = sql.strip_prefix("IMPORT_BUFFER ") {
                let _ = import_request(b"virtual", size.parse().unwrap(), session);
                return;
            } else if sql == "IMPORT_EMPTY" {
                let mut frame = vec![b'l'];
                frame.extend_from_slice(&[0; 9]);
                session.send(&frame).unwrap();
                return;
            } else if sql == "IMPORT_LONGNAME" {
                let _ = import_request(&vec![b'a'; 5_000], 8192, session);
                return;
            } else if let Some(path) = sql.strip_prefix("EXPORT ") {
                let transfer = export_start(path, session);
                let accepted = transfer.status == Some(0);
                log.lock().unwrap().push(transfer);
                if accepted {
                    session
                        .send(&export_data(&[b"1|alpha\n", b"2|beta\n"], 3))
                        .unwrap();
                    simple_command("CREATE EXTERNAL TABLE")
                } else {
                    error_response()
                }
            } else if let Some(path) = sql.strip_prefix("EXPORT_BADCHUNK ") {
                let transfer = export_start(path, session);
                log.lock().unwrap().push(transfer);
                session.send(&export_data(&[], 0)[..9]).unwrap();
                session.send(&1i32.to_be_bytes()).unwrap();
                session.send(&2_000_000_000i32.to_be_bytes()).unwrap();
                return;
            } else if let Some(path) = sql.strip_prefix("EXPORT_ERROR ") {
                let transfer = export_start(path, session);
                log.lock().unwrap().push(transfer);
                // Server-side abort: chunk, then status 2 with a message.
                let mut data = export_data(&[b"partial\n"], 2);
                data.extend_from_slice(&5u16.to_be_bytes());
                data.extend_from_slice(b"abort");
                session.send(&data).unwrap();
                error_response()
            } else {
                select_one()
            };
            if session.send(&response).is_err() {
                return;
            }
        }
    })
}

fn config(server: &MockServer, policy: ExternalFilePolicy) -> NzConnectionConfig {
    NzConnectionConfig {
        external_files: policy,
        command_timeout: 20,
        ..server.config()
    }
}

async fn assert_reusable(client: &nz_rust::Client, server: &MockServer) {
    let rows = tokio::time::timeout(Duration::from_secs(10), client.query("SELECT ok", &[]))
        .await
        .expect("session hung after failed transfer")
        .expect("session unusable after failed transfer");
    assert_eq!(rows.len(), 1);
    assert!(!client.is_closed());
    assert_eq!(server.accepted(), 1, "session was replaced");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn refused_imports_report_a_transfer_error_and_keep_the_session() {
    let missing = temp_path("missing");
    let outside = tempdir();
    let log: Log = Arc::default();
    let server = start_server(log.clone());
    for (label, policy, name) in [
        (
            "disabled policy",
            ExternalFilePolicy::Disabled,
            "/etc/hostname".to_string(),
        ),
        (
            "missing file",
            ExternalFilePolicy::Unrestricted,
            missing.to_string_lossy().into_owned(),
        ),
        (
            "directory traversal",
            ExternalFilePolicy::Directory(outside.join("allowed")),
            outside.join("secret.txt").to_string_lossy().into_owned(),
        ),
    ] {
        let client = nz_rust::Client::connect(&config(&server, policy))
            .await
            .unwrap();
        let before = log.lock().unwrap().len();
        let result = client.query(&format!("IMPORT {name}"), &[]).await;
        assert!(
            matches!(result, Err(NzError::Database(_))),
            "{label}: {result:?}"
        );
        let transfers = log.lock().unwrap().clone();
        assert_eq!(
            transfers[before],
            Transfer {
                chunks: vec![],
                status: Some(2)
            },
            "{label}: client must report a transfer error and send no data"
        );
        assert!(!client.is_closed(), "{label}");
        let rows = client.query("SELECT ok", &[]).await.unwrap();
        assert_eq!(rows.len(), 1, "{label}");
        client.close().await.unwrap();
    }
    server.assert_no_handler_panics();
}

/// An `AsyncRead` that yields `data`, then fails.
struct FailingReader {
    data: Vec<u8>,
    sent: bool,
}

impl AsyncRead for FailingReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if self.sent {
            return Poll::Ready(Err(std::io::Error::other("disk on fire")));
        }
        self.sent = true;
        buf.put_slice(&self.data);
        Poll::Ready(Ok(()))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn import_reader_failing_mid_stream_aborts_the_transfer_cleanly() {
    let log: Log = Arc::default();
    let server = start_server(log.clone());
    let client = nz_rust::Client::connect(&config(&server, ExternalFilePolicy::Disabled))
        .await
        .unwrap();
    let result = client
        .query_with_import_reader(
            "IMPORT virtual-id",
            &[],
            "virtual-id",
            FailingReader {
                data: b"1|alpha\n2|beta\n".to_vec(),
                sent: false,
            },
        )
        .await;
    assert!(matches!(result, Err(NzError::Database(_))), "{result:?}");
    let transfers = log.lock().unwrap().clone();
    assert_eq!(transfers.len(), 1);
    assert_eq!(transfers[0].chunks.concat(), b"1|alpha\n2|beta\n");
    assert_eq!(
        transfers[0].status,
        Some(2),
        "reader error must abort the import"
    );
    assert_reusable(&client, &server).await;

    // A reader that succeeds completes with status 3 and the full payload.
    let result = client
        .query_with_import_reader(
            "IMPORT virtual-ok",
            &[],
            "virtual-ok",
            std::io::Cursor::new(b"3|gamma\n".to_vec()),
        )
        .await
        .unwrap();
    assert_eq!(result.rows_affected, 3);
    let transfers = log.lock().unwrap().clone();
    assert_eq!(transfers[1].chunks.concat(), b"3|gamma\n");
    assert_eq!(transfers[1].status, Some(3));
    client.close().await.unwrap();
    server.assert_no_handler_panics();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn malformed_import_requests_fail_with_protocol_errors_and_retire_the_session() {
    let log: Log = Arc::default();
    let server = start_server(log);
    for (label, sql) in [
        ("negative buffer size", "IMPORT_BUFFER -1".to_string()),
        (
            "buffer size above the protocol limit",
            format!("IMPORT_BUFFER {}", nz_rust::error::MAX_PROTOCOL_PAYLOAD + 1),
        ),
        (
            "buffer size i32::MAX",
            format!("IMPORT_BUFFER {}", i32::MAX),
        ),
        (
            "unterminated 5000 byte filename",
            "IMPORT_LONGNAME".to_string(),
        ),
        ("empty filename", "IMPORT_EMPTY".to_string()),
    ] {
        let client = nz_rust::Client::connect(&config(&server, ExternalFilePolicy::Disabled))
            .await
            .unwrap();
        let result = tokio::time::timeout(Duration::from_secs(15), client.query(&sql, &[]))
            .await
            .unwrap_or_else(|_| panic!("{label}: client hung"));
        assert!(
            matches!(result, Err(NzError::Protocol(_))),
            "{label}: {result:?}"
        );
        assert!(client.is_closed(), "{label}: session must be retired");
    }
    server.assert_no_handler_panics();
}

fn tempdir() -> std::path::PathBuf {
    let dir = temp_path("dir");
    std::fs::create_dir_all(dir.join("allowed")).unwrap();
    dir
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn refused_exports_create_no_files_and_keep_the_session() {
    let root = tempdir();
    let log: Log = Arc::default();
    let server = start_server(log.clone());
    let traversal = root.join("allowed/../escaped.txt");
    let missing_dir = root.join("no_such_dir/out.txt");
    let denied = root.join("allowed/denied.txt");
    for (label, policy, path) in [
        (
            "disabled policy",
            ExternalFilePolicy::Disabled,
            denied.clone(),
        ),
        (
            "directory traversal",
            ExternalFilePolicy::Directory(root.join("allowed")),
            traversal.clone(),
        ),
        (
            "missing directory",
            ExternalFilePolicy::Unrestricted,
            missing_dir.clone(),
        ),
    ] {
        let client = nz_rust::Client::connect(&config(&server, policy))
            .await
            .unwrap();
        let before = log.lock().unwrap().len();
        let result = client
            .query(&format!("EXPORT {}", path.to_string_lossy()), &[])
            .await;
        assert!(
            matches!(result, Err(NzError::Database(_))),
            "{label}: {result:?}"
        );
        assert_eq!(
            log.lock().unwrap()[before].status,
            Some(1),
            "{label}: client must refuse the export"
        );
        assert_reusable_after(&client, label).await;
        client.close().await.unwrap();
    }
    assert!(!denied.exists());
    assert!(!root.join("escaped.txt").exists());
    assert!(!missing_dir.exists());
    std::fs::remove_dir_all(&root).unwrap();
    server.assert_no_handler_panics();
}

async fn assert_reusable_after(client: &nz_rust::Client, label: &str) {
    assert!(!client.is_closed(), "{label}");
    assert_eq!(
        client.query("SELECT ok", &[]).await.unwrap().len(),
        1,
        "{label}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn export_succeeds_inside_the_permitted_directory_and_survives_server_aborts() {
    let root = tempdir();
    let log: Log = Arc::default();
    let server = start_server(log.clone());
    let allowed = root.join("allowed");
    let client = nz_rust::Client::connect(&config(
        &server,
        ExternalFilePolicy::Directory(allowed.clone()),
    ))
    .await
    .unwrap();

    let out = allowed.join("ok.txt");
    client
        .query(&format!("EXPORT {}", out.to_string_lossy()), &[])
        .await
        .unwrap();
    assert_eq!(std::fs::read(&out).unwrap(), b"1|alpha\n2|beta\n");

    // The server aborts the export after one chunk (status 2 + message).
    let aborted = allowed.join("aborted.txt");
    let result = client
        .query(&format!("EXPORT_ERROR {}", aborted.to_string_lossy()), &[])
        .await;
    assert!(matches!(result, Err(NzError::Database(_))), "{result:?}");
    assert_reusable_after(&client, "server abort").await;
    assert_eq!(server.accepted(), 1);
    client.close().await.unwrap();
    std::fs::remove_dir_all(&root).unwrap();
    server.assert_no_handler_panics();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn export_chunk_with_hostile_length_is_a_protocol_error() {
    let root = tempdir();
    let log: Log = Arc::default();
    let server = start_server(log);
    let client = nz_rust::Client::connect(&config(&server, ExternalFilePolicy::Unrestricted))
        .await
        .unwrap();
    let out = root.join("allowed/bad.txt");
    let result = tokio::time::timeout(
        Duration::from_secs(15),
        client.query(&format!("EXPORT_BADCHUNK {}", out.to_string_lossy()), &[]),
    )
    .await
    .expect("client hung on a 2 GB export chunk");
    assert!(matches!(result, Err(NzError::Protocol(_))), "{result:?}");
    assert!(client.is_closed());
    std::fs::remove_dir_all(&root).unwrap();
    server.assert_no_handler_panics();
}

#[cfg(feature = "compat")]
mod legacy {
    use super::*;

    #[test]
    fn legacy_refused_import_and_export_keep_the_session() {
        let log: Log = Arc::default();
        let server = start_server(log.clone());
        let root = tempdir();
        let refused = root.join("allowed/refused.txt");
        let mut conn =
            nz_rust::NzConnection::connect(&config(&server, ExternalFilePolicy::Disabled)).unwrap();
        for sql in [
            "IMPORT /etc/hostname".to_string(),
            format!("EXPORT {}", refused.to_string_lossy()),
        ] {
            let result = conn.query(&sql, &[]);
            assert!(
                matches!(result, Err(NzError::Database(_))),
                "{sql}: {result:?}"
            );
            assert!(!conn.is_closed());
        }
        let transfers = log.lock().unwrap().clone();
        assert_eq!(transfers[0].status, Some(2));
        assert_eq!(transfers[1].status, Some(1));
        assert_eq!(
            conn.query("SELECT ok", &[]).unwrap().result_sets[0]
                .rows
                .len(),
            1
        );
        assert!(!refused.exists());
        assert_eq!(server.accepted(), 1);
        conn.close();
        std::fs::remove_dir_all(&root).unwrap();
        server.assert_no_handler_panics();
    }

    #[test]
    fn legacy_malformed_import_requests_retire_the_session() {
        let log: Log = Arc::default();
        let server = start_server(log);
        for sql in [
            "IMPORT_BUFFER -1".to_string(),
            format!("IMPORT_BUFFER {}", i32::MAX),
            "IMPORT_LONGNAME".to_string(),
            "IMPORT_EMPTY".to_string(),
        ] {
            let mut conn =
                nz_rust::NzConnection::connect(&config(&server, ExternalFilePolicy::Disabled))
                    .unwrap();
            let result = conn.query(&sql, &[]);
            assert!(
                matches!(result, Err(NzError::Protocol(_))),
                "{sql}: {result:?}"
            );
            assert!(conn.is_closed(), "{sql}");
        }
        server.assert_no_handler_panics();
    }
}
