//! Offline end-to-end integration test.
//!
//! Starts a tiny mock appliance on `127.0.0.1` that speaks the exact wire
//! protocol the reference drivers expect (handshake + authentication +
//! BackendKeyData/ReadyForQuery, then a one-statement text result set), and
//! drives [`NzConnection`] against it. This validates the framing, the
//! handshake ack hand-shake and the text DataRow parser without needing a real
//! Netezza appliance.

use futures_core::Stream;
use nz_rust::{
    ColumnDesc, NzConnection, NzConnectionConfig, NzError, QueryStreamSink, Row, SecurityLevel,
};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

/// Read one `[len(4)][opcode(2)][payload]` option frame from the client.
fn read_frame(stream: &mut TcpStream) -> (i16, Vec<u8>) {
    let mut header = [0u8; 6];
    stream.read_exact(&mut header).unwrap();
    let len = i32::from_be_bytes(header[0..4].try_into().unwrap());
    let opcode = i16::from_be_bytes(header[4..6].try_into().unwrap());
    let payload_len = (len - 6) as usize;
    let mut payload = vec![0u8; payload_len];
    stream.read_exact(&mut payload).unwrap();
    (opcode, payload)
}

fn write_be(stream: &mut TcpStream, bytes: &[u8]) {
    stream.write_all(bytes).unwrap();
    stream.flush().unwrap();
}

fn serve_handshake(stream: &mut TcpStream) {
    // 1. Version negotiation.
    let (opcode, payload) = read_frame(stream);
    assert_eq!(opcode, 1, "expected CLIENT_BEGIN");
    assert_eq!(payload.len(), 2, "expected one version word");
    write_be(stream, b"N");

    // 2. Database selection.
    let (opcode, _payload) = read_frame(stream);
    assert_eq!(opcode, 2, "expected HSV2_DB");
    write_be(stream, b"N");

    // 3. SSL negotiation.
    let (opcode, _payload) = read_frame(stream);
    assert_eq!(opcode, 11, "expected HSV2_SSL_NEGOTIATE");
    write_be(stream, b"N");

    // 4. Option loop: acknowledge every frame until CLIENT_DONE.
    loop {
        let (opcode, _payload) = read_frame(stream);
        if opcode == 1000 {
            break;
        }
        write_be(stream, b"N");
    }

    // 5. Authentication: AuthenticationRequest + AUTH_REQ_OK.
    let mut auth = vec![b'R'];
    auth.extend_from_slice(&0i32.to_be_bytes());
    write_be(stream, &auth);

    // 6. BackendKeyData and ReadyForQuery.
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

fn read_query(stream: &mut TcpStream) -> String {
    let mut head = [0u8; 5];
    stream.read_exact(&mut head).unwrap();
    assert_eq!(head[0], b'P', "expected simple query packet");
    let mut sql = Vec::new();
    loop {
        let mut byte = [0u8; 1];
        stream.read_exact(&mut byte).unwrap();
        if byte[0] == 0 {
            break;
        }
        sql.push(byte[0]);
    }
    String::from_utf8(sql).unwrap()
}

fn send_ready(stream: &mut TcpStream) {
    let mut ready = vec![b'Z'];
    ready.extend_from_slice(&[0, 0, 0, 0]);
    write_be(stream, &ready);
}

fn send_select_response(stream: &mut TcpStream) {
    send_message(stream, b'T', &row_description_payload());
    send_message(stream, b'D', &data_row_payload());
    send_message(stream, b'C', b"SELECT 1\0");
    send_ready(stream);
}

fn send_select_with_empty_additional_result(stream: &mut TcpStream) {
    send_select_response_without_ready(stream);
    send_message(stream, b'T', &row_description_payload());
    send_message(stream, b'C', b"SELECT 0\0");
    send_ready(stream);
}

fn send_select_response_without_ready(stream: &mut TcpStream) {
    send_message(stream, b'T', &row_description_payload());
    send_message(stream, b'D', &data_row_payload());
    send_message(stream, b'C', b"SELECT 1\0");
}

fn send_cancelled_response(stream: &mut TcpStream) {
    // Structured ErrorResponse payload: severity, SQLSTATE, message, end.
    let payload = b"SFATAL\0C57014\0Mquery canceled\0\0";
    send_message(stream, b'E', payload);
    send_ready(stream);
}

#[derive(Default)]
struct CountingSink {
    columns: usize,
    rows: usize,
}

impl QueryStreamSink for CountingSink {
    fn on_columns(
        &mut self,
        _result_set_index: usize,
        columns: &[ColumnDesc],
        _nullability: Option<&[bool]>,
    ) -> Result<(), NzError> {
        self.columns = columns.len();
        Ok(())
    }

    fn on_row(&mut self, _result_set_index: usize, _row: Row) -> Result<(), NzError> {
        self.rows += 1;
        Ok(())
    }
}

/// Send a backend message: `[type][4-byte shared header][len(4)][payload]`.
fn send_message(stream: &mut TcpStream, msg_type: u8, payload: &[u8]) {
    let mut frame = vec![msg_type];
    frame.extend_from_slice(&[0, 0, 0, 0]); // shared header (ignored by reader)
    frame.extend_from_slice(&(payload.len() as i32).to_be_bytes());
    frame.extend_from_slice(payload);
    write_be(stream, &frame);
}

/// Frame a text RowDescription payload for two columns: INTEGER "ONE" and
/// VARCHAR "TXT".
fn row_description_payload() -> Vec<u8> {
    let mut p = Vec::new();
    p.extend_from_slice(&2u16.to_be_bytes());
    for (name, oid, len) in [("ONE", 23i32, 4i16), ("TXT", 1043, -1)] {
        p.extend_from_slice(name.as_bytes());
        p.push(0);
        p.extend_from_slice(&oid.to_be_bytes());
        p.extend_from_slice(&len.to_be_bytes());
        p.extend_from_slice(&(-1i32).to_be_bytes()); // type modifier
        p.push(0); // text format
    }
    p
}

/// Frame one text DataRow payload: bitmap + per-column `vlen(i32) + bytes`.
fn data_row_payload() -> Vec<u8> {
    let mut p = vec![0b1100_0000u8]; // both columns present
    p.extend_from_slice(&(4i32 + 1).to_be_bytes());
    p.extend_from_slice(b"7");
    p.extend_from_slice(&(4i32 + 3).to_be_bytes());
    p.extend_from_slice(b"abc");
    p
}

/// Serve a single connection through handshake, query and clean shutdown.
fn serve(mut stream: TcpStream) {
    // 1. Version negotiation: client sends HSV2_CLIENT_BEGIN (1) + version.
    let (opcode, payload) = read_frame(&mut stream);
    assert_eq!(opcode, 1, "expected CLIENT_BEGIN");
    assert_eq!(payload.len(), 2, "expected one version word");
    write_be(&mut stream, b"N");

    // 2. Database selection: HSV2_DB (2) + NUL-terminated name.
    let (opcode, _payload) = read_frame(&mut stream);
    assert_eq!(opcode, 2, "expected HSV2_DB");
    write_be(&mut stream, b"N");

    // 3. SSL negotiation: HSV2_SSL_NEGOTIATE (11) + i32 security level.
    let (opcode, _payload) = read_frame(&mut stream);
    assert_eq!(opcode, 11, "expected HSV2_SSL_NEGOTIATE");
    write_be(&mut stream, b"N");

    // 4. Option loop: ack every frame with 'N' until CLIENT_DONE (1000).
    loop {
        let (opcode, _payload) = read_frame(&mut stream);
        if opcode == 1000 {
            break;
        }
        write_be(&mut stream, b"N");
    }

    // 5. Authentication: AuthenticationRequest (R) + AUTH_REQ_OK (0).
    let mut auth = vec![b'R'];
    auth.extend_from_slice(&0i32.to_be_bytes());
    write_be(&mut stream, &auth);

    // 6. Connection complete: BackendKeyData then ReadyForQuery.
    //
    // 'K' does not use the generic length-prefixed framing: the driver reads
    // exactly 16 bytes after the type byte (prefix + length + pid + key).
    let mut key = vec![b'K'];
    key.extend_from_slice(&[0, 0, 0, 0]); // prefix
    key.extend_from_slice(&12i32.to_be_bytes()); // declared length
    key.extend_from_slice(&5857i32.to_be_bytes()); // backend pid
    key.extend_from_slice(&(-2_092_017_624i32).to_be_bytes()); // secret key
    write_be(&mut stream, &key);

    // 'Z' is type + a 4-byte prefix.
    let mut ready = vec![b'Z'];
    ready.extend_from_slice(&[0, 0, 0, 0]);
    write_be(&mut stream, &ready);

    // 7. Query: 'P' + command number(4) + SQL + NUL.
    let mut head = [0u8; 5];
    stream.read_exact(&mut head).unwrap();
    assert_eq!(head[0], b'P', "expected simple query packet");
    loop {
        let mut b = [0u8; 1];
        stream.read_exact(&mut b).unwrap();
        if b[0] == 0 {
            break;
        }
    }

    // 8. One text result set: RowDescription, DataRow, CommandComplete, Ready.
    send_message(&mut stream, b'T', &row_description_payload());
    send_message(&mut stream, b'D', &data_row_payload());
    send_message(&mut stream, b'C', b"SELECT 1\0");
    let mut ready = vec![b'Z'];
    ready.extend_from_slice(&[0, 0, 0, 0]);
    write_be(&mut stream, &ready);
}

#[test]
fn async_cancel_uses_out_of_band_connection_and_preserves_session() {
    let listener = match TcpListener::bind("127.0.0.1:0") {
        Ok(listener) => listener,
        Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
            eprintln!("skipping TCP mock: loopback listeners are unavailable: {error}");
            return;
        }
        Err(error) => panic!("cannot bind TCP mock: {error}"),
    };
    let addr = listener.local_addr().unwrap();
    let (query_started_tx, query_started_rx) = mpsc::channel();
    let server = thread::spawn(move || {
        let (mut main, _) = listener.accept().unwrap();
        serve_handshake(&mut main);
        assert_eq!(read_query(&mut main), "SELECT slow");
        query_started_tx.send(()).unwrap();

        let (mut cancel, _) = listener.accept().unwrap();
        let mut packet = [0u8; 16];
        cancel.read_exact(&mut packet).unwrap();
        assert_eq!(&packet[0..4], &16i32.to_be_bytes());
        assert_eq!(&packet[4..8], &80877102i32.to_be_bytes());
        assert_eq!(&packet[8..12], &5857i32.to_be_bytes());
        assert_eq!(&packet[12..16], &(-2_092_017_624i32).to_be_bytes());

        send_cancelled_response(&mut main);
        assert_eq!(read_query(&mut main), "SELECT recovered");
        send_select_response(&mut main);
    });

    let config = NzConnectionConfig {
        host: "127.0.0.1".into(),
        port: addr.port(),
        database: "JUST_DATA".into(),
        user: "admin".into(),
        password: "secret".into(),
        connection_timeout: 1,
        command_timeout: 5,
        ..Default::default()
    };

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let connection = nz_rust::AsyncNzConnection::connect(&config)
            .await
            .expect("handshake should succeed");
        let query_connection = connection.clone();
        let query_task = tokio::spawn(async move {
            query_connection
                .query_values("SELECT slow", Vec::new())
                .await
        });

        tokio::task::spawn_blocking(move || {
            query_started_rx
                .recv_timeout(Duration::from_secs(2))
                .expect("query should reach the mock server");
        })
        .await
        .unwrap();

        connection
            .cancel()
            .await
            .expect("cancel packet should be sent");
        let query_result = query_task.await.unwrap();
        assert!(matches!(query_result, Err(nz_rust::NzError::Database(_))));

        let recovered = connection
            .query("SELECT recovered", &[])
            .await
            .expect("same session should be reusable after cancel");
        assert_eq!(recovered.row_count(), 1);
        connection.close().await;
    });

    server.join().unwrap();
}

#[test]
fn command_timeout_is_absolute_and_resynchronizes_the_same_session() {
    let listener = match TcpListener::bind("127.0.0.1:0") {
        Ok(listener) => listener,
        Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
            eprintln!("skipping TCP mock: loopback listeners are unavailable: {error}");
            return;
        }
        Err(error) => panic!("cannot bind TCP mock: {error}"),
    };
    let addr = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        let (mut main, _) = listener.accept().unwrap();
        serve_handshake(&mut main);
        assert_eq!(read_query(&mut main), "SELECT timeout");
        thread::sleep(Duration::from_millis(250));

        let (mut cancel, _) = listener.accept().unwrap();
        let mut packet = [0u8; 16];
        cancel.read_exact(&mut packet).unwrap();
        assert_eq!(&packet[4..8], &80877102i32.to_be_bytes());
        send_cancelled_response(&mut main);

        assert_eq!(read_query(&mut main), "SELECT after_timeout");
        send_select_response(&mut main);
    });

    let config = NzConnectionConfig {
        host: "127.0.0.1".into(),
        port: addr.port(),
        database: "JUST_DATA".into(),
        user: "admin".into(),
        password: "secret".into(),
        connection_timeout: 1,
        command_timeout: 5,
        ..Default::default()
    };
    let mut connection = NzConnection::connect(&config).expect("handshake should succeed");
    let started = std::time::Instant::now();
    let error = connection
        .query_with_timeout("SELECT timeout", &[], Some(Duration::from_millis(80)))
        .expect_err("delayed response should time out");
    assert!(matches!(error, nz_rust::NzError::Timeout(_)));
    assert!(started.elapsed() < Duration::from_secs(1));

    let recovered = connection
        .query("SELECT after_timeout", &[])
        .expect("same session should be reusable after timeout");
    assert_eq!(recovered.row_count(), 1);
    connection.close();
    server.join().unwrap();
}

#[test]
fn streaming_timeout_covers_fetch_after_rows_start() {
    let listener = match TcpListener::bind("127.0.0.1:0") {
        Ok(listener) => listener,
        Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
            eprintln!("skipping TCP mock: loopback listeners are unavailable: {error}");
            return;
        }
        Err(error) => panic!("cannot bind TCP mock: {error}"),
    };
    let addr = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        let (mut main, _) = listener.accept().unwrap();
        serve_handshake(&mut main);
        assert_eq!(read_query(&mut main), "SELECT stream_timeout");
        // Send metadata, then stall before the first row. This exercises the
        // fetch-side deadline rather than only the initial command write/read.
        send_message(&mut main, b'T', &row_description_payload());
        thread::sleep(Duration::from_millis(250));

        let (mut cancel, _) = listener.accept().unwrap();
        let mut packet = [0u8; 16];
        cancel.read_exact(&mut packet).unwrap();
        assert_eq!(&packet[4..8], &80877102i32.to_be_bytes());
        send_cancelled_response(&mut main);
        assert_eq!(read_query(&mut main), "SELECT after_stream_timeout");
        send_select_response(&mut main);
    });

    let config = NzConnectionConfig {
        host: "127.0.0.1".into(),
        port: addr.port(),
        database: "JUST_DATA".into(),
        user: "admin".into(),
        password: "secret".into(),
        connection_timeout: 1,
        command_timeout: 5,
        ..Default::default()
    };
    let mut connection = NzConnection::connect(&config).expect("handshake should succeed");
    let error = connection
        .execute_stream_with_timeout(
            "SELECT stream_timeout",
            &[],
            Some(Duration::from_millis(80)),
            &mut CountingSink::default(),
        )
        .expect_err("delayed row should time out while fetching");
    assert!(matches!(error, NzError::Timeout(_)));

    let recovered = connection
        .query("SELECT after_stream_timeout", &[])
        .expect("same session should be reusable after stream timeout");
    assert_eq!(recovered.row_count(), 1);
    connection.close();
    server.join().unwrap();
}

#[test]
fn only_secure_session_rejects_plaintext_downgrade() {
    let listener = match TcpListener::bind("127.0.0.1:0") {
        Ok(listener) => listener,
        Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
            eprintln!("skipping TCP mock: loopback listeners are unavailable: {error}");
            return;
        }
        Err(error) => panic!("cannot bind TCP mock: {error}"),
    };
    let addr = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        assert_eq!(read_frame(&mut stream).0, 1);
        write_be(&mut stream, b"N");
        assert_eq!(read_frame(&mut stream).0, 2);
        write_be(&mut stream, b"N");
        assert_eq!(read_frame(&mut stream).0, 11);
        write_be(&mut stream, b"N");
    });

    let config = NzConnectionConfig {
        host: "127.0.0.1".into(),
        port: addr.port(),
        database: "JUST_DATA".into(),
        user: "admin".into(),
        password: "secret".into(),
        security_level: SecurityLevel::OnlySecuredSession,
        ..Default::default()
    };
    let error = match NzConnection::connect(&config) {
        Ok(_) => panic!("plain response must be rejected"),
        Err(error) => error,
    };
    assert!(matches!(error, NzError::Protocol(_)));
    server.join().unwrap();
}

#[test]
fn handshake_and_query_round_trip_against_mock_server() {
    let listener = match TcpListener::bind("127.0.0.1:0") {
        Ok(listener) => listener,
        Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
            eprintln!("skipping TCP mock: loopback listeners are unavailable: {error}");
            return;
        }
        Err(error) => panic!("cannot bind TCP mock: {error}"),
    };
    let addr = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        serve(stream);
    });

    let config = NzConnectionConfig {
        host: "127.0.0.1".into(),
        port: addr.port(),
        database: "JUST_DATA".into(),
        user: "admin".into(),
        password: "secret".into(),
        ..Default::default()
    };

    let mut conn = NzConnection::connect(&config).expect("handshake should succeed");
    assert_eq!(conn.backend_process_id(), 5857);
    assert_eq!(conn.backend_secret_key(), -2_092_017_624);

    let result = conn.query("SELECT 7 AS ONE, 'abc' AS TXT", &[]).unwrap();
    assert_eq!(result.row_count(), 1);
    let row = &result.rows()[0];
    assert_eq!(row.try_get::<_, i32>("ONE").unwrap(), 7);
    assert_eq!(row.try_get::<_, String>("TXT").unwrap(), "abc");
    assert_eq!(result.columns()[0].type_name(), "INTEGER");
    assert_eq!(result.rows_affected, 1);

    conn.close();
    server.join().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_async_client_round_trip_against_mock_server() {
    let listener = match TcpListener::bind("127.0.0.1:0") {
        Ok(listener) => listener,
        Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
            eprintln!("skipping TCP mock: loopback listeners are unavailable: {error}");
            return;
        }
        Err(error) => panic!("cannot bind TCP mock: {error}"),
    };
    let addr = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        serve(stream);
    });

    let config = NzConnectionConfig {
        host: "127.0.0.1".into(),
        port: addr.port(),
        database: "JUST_DATA".into(),
        user: "admin".into(),
        password: "secret".into(),
        ..Default::default()
    };
    let (client, connection) = nz_rust::connect(&config).await.unwrap();
    let driver = tokio::spawn(connection);
    let rows = client
        .query("SELECT 7 AS ONE, 'abc' AS TXT", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].try_get::<_, i32>("ONE").unwrap(), 7);
    assert_eq!(rows[0].try_get::<_, String>("TXT").unwrap(), "abc");
    client.close().await.unwrap();
    assert!(driver.await.unwrap().is_ok());
    server.join().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_async_database_error_keeps_session_reusable() {
    let listener = match TcpListener::bind("127.0.0.1:0") {
        Ok(listener) => listener,
        Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
            eprintln!("skipping TCP mock: loopback listeners are unavailable: {error}");
            return;
        }
        Err(error) => panic!("cannot bind TCP mock: {error}"),
    };
    let addr = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        serve_handshake(&mut stream);
        assert_eq!(read_query(&mut stream), "SELECT invalid");
        send_cancelled_response(&mut stream);
        assert_eq!(read_query(&mut stream), "SELECT recovered");
        send_select_response(&mut stream);
    });

    let config = NzConnectionConfig {
        host: "127.0.0.1".into(),
        port: addr.port(),
        database: "JUST_DATA".into(),
        user: "admin".into(),
        password: "secret".into(),
        ..Default::default()
    };
    let (client, connection) = nz_rust::connect(&config).await.unwrap();
    let driver = tokio::spawn(connection);
    let error = client
        .query("SELECT invalid", &[])
        .await
        .expect_err("database error should be returned to the caller");
    assert!(matches!(error, NzError::Database(_)));

    let rows = client
        .query("SELECT recovered", &[])
        .await
        .expect("the same native session should accept the next query");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].try_get::<_, i32>("ONE").unwrap(), 7);

    client.close().await.unwrap();
    assert!(driver.await.unwrap().is_ok());
    server.join().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_async_row_stream_drains_and_decodes_rows() {
    let listener = match TcpListener::bind("127.0.0.1:0") {
        Ok(listener) => listener,
        Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
            eprintln!("skipping TCP mock: loopback listeners are unavailable: {error}");
            return;
        }
        Err(error) => panic!("cannot bind TCP mock: {error}"),
    };
    let addr = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        serve_handshake(&mut stream);
        assert_eq!(read_query(&mut stream), "SELECT stream");
        send_message(&mut stream, b'T', &row_description_payload());
        for _ in 0..3 {
            send_message(&mut stream, b'D', &data_row_payload());
        }
        send_message(&mut stream, b'C', b"SELECT 3\0");
        send_ready(&mut stream);
    });

    let config = NzConnectionConfig {
        host: "127.0.0.1".into(),
        port: addr.port(),
        database: "JUST_DATA".into(),
        user: "admin".into(),
        password: "secret".into(),
        ..Default::default()
    };
    let (client, connection) = nz_rust::connect(&config).await.unwrap();
    let driver = tokio::spawn(connection);
    let mut rows = client.query_stream("SELECT stream", &[]).await.unwrap();
    let mut count = 0;
    while let Some(row) =
        std::future::poll_fn(|cx| std::pin::Pin::new(&mut rows).poll_next(cx)).await
    {
        let row = row.unwrap();
        assert_eq!(row.try_get::<_, i32>("ONE").unwrap(), 7);
        count += 1;
    }
    assert_eq!(count, 3);
    client.close().await.unwrap();
    assert!(driver.await.unwrap().is_ok());
    server.join().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_async_stream_reports_empty_additional_result_set() {
    let listener = match TcpListener::bind("127.0.0.1:0") {
        Ok(listener) => listener,
        Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
            eprintln!("skipping TCP mock: loopback listeners are unavailable: {error}");
            return;
        }
        Err(error) => panic!("cannot bind TCP mock: {error}"),
    };
    let addr = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        serve_handshake(&mut stream);
        assert_eq!(read_query(&mut stream), "SELECT first; SELECT empty_second");
        send_select_with_empty_additional_result(&mut stream);
    });

    let config = NzConnectionConfig {
        host: "127.0.0.1".into(),
        port: addr.port(),
        database: "JUST_DATA".into(),
        user: "admin".into(),
        password: "secret".into(),
        ..Default::default()
    };
    let (client, connection) = nz_rust::connect(&config).await.unwrap();
    let driver = tokio::spawn(connection);
    let mut rows = client
        .query_stream("SELECT first; SELECT empty_second", &[])
        .await
        .unwrap();

    let first = std::future::poll_fn(|cx| std::pin::Pin::new(&mut rows).poll_next(cx))
        .await
        .expect("first result set should produce a row")
        .unwrap();
    assert_eq!(first.try_get::<_, i32>("ONE").unwrap(), 7);

    let error = std::future::poll_fn(|cx| std::pin::Pin::new(&mut rows).poll_next(cx))
        .await
        .expect("second result set should be reported")
        .expect_err("query_stream must reject additional result sets");
    assert!(matches!(error, NzError::Config(message) if message.contains("first result set")));
    assert!(
        std::future::poll_fn(|cx| std::pin::Pin::new(&mut rows).poll_next(cx))
            .await
            .is_none()
    );

    client.close().await.unwrap();
    assert!(driver.await.unwrap().is_ok());
    server.join().unwrap();
}
