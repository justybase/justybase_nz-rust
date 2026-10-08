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
    ColumnDesc, NzConnection, NzConnectionConfig, NzError, QueryStreamEvent, QueryStreamSink, Row,
    SecurityLevel,
};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

/// Read one `[len(4)][opcode(2)][payload]` option frame from the client.
fn read_frame(stream: &mut impl Read) -> (i16, Vec<u8>) {
    let mut header = [0u8; 6];
    stream.read_exact(&mut header).unwrap();
    let len = i32::from_be_bytes(header[0..4].try_into().unwrap());
    let opcode = i16::from_be_bytes(header[4..6].try_into().unwrap());
    let payload_len = (len - 6) as usize;
    let mut payload = vec![0u8; payload_len];
    stream.read_exact(&mut payload).unwrap();
    (opcode, payload)
}

fn write_be(stream: &mut impl Write, bytes: &[u8]) {
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

    serve_handshake_options(stream);
}

fn serve_handshake_options(stream: &mut (impl Read + Write)) {
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

fn read_query(stream: &mut impl Read) -> String {
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

fn send_ready(stream: &mut impl Write) {
    let mut ready = vec![b'Z'];
    ready.extend_from_slice(&[0, 0, 0, 0]);
    write_be(stream, &ready);
}

fn mock_config(port: u16) -> NzConnectionConfig {
    NzConnectionConfig {
        external_files: nz_rust::ExternalFilePolicy::Unrestricted,
        host: "127.0.0.1".into(),
        port,
        database: "JUST_DATA".into(),
        user: "admin".into(),
        password: "secret".into(),
        ..Default::default()
    }
}

fn send_import_request(stream: &mut TcpStream, source: &str, buffer_size: i32) {
    let mut frame = vec![b'l'];
    frame.extend_from_slice(&[0; 8]);
    frame.extend_from_slice(source.as_bytes());
    frame.push(0);
    frame.extend_from_slice(&1i32.to_be_bytes());
    write_be(stream, &frame);
    let mut version = [0; 4];
    stream.read_exact(&mut version).unwrap();
    assert_eq!(i32::from_be_bytes(version), 1);
    write_be(stream, &0i32.to_be_bytes());
    write_be(stream, &buffer_size.to_be_bytes());
}

fn read_import_data(stream: &mut TcpStream, max_chunk: usize) -> Vec<u8> {
    let mut bytes = Vec::new();
    loop {
        let mut word = [0; 4];
        stream.read_exact(&mut word).unwrap();
        match i32::from_be_bytes(word) {
            1 => {
                stream.read_exact(&mut word).unwrap();
                let size = i32::from_be_bytes(word) as usize;
                assert!(size > 0 && size <= max_chunk);
                let mut chunk = vec![0; size];
                stream.read_exact(&mut chunk).unwrap();
                bytes.extend(chunk);
            }
            3 => return bytes,
            other => panic!("unexpected import status: {other}"),
        }
    }
}

fn send_select_response(stream: &mut impl Write) {
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

fn send_select_response_without_ready(stream: &mut impl Write) {
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

struct AbortingSink {
    rows: usize,
}

impl QueryStreamSink for AbortingSink {
    fn on_columns(
        &mut self,
        _result_set_index: usize,
        _columns: &[ColumnDesc],
        _nullability: Option<&[bool]>,
    ) -> Result<(), NzError> {
        Ok(())
    }

    fn on_row(&mut self, _result_set_index: usize, _row: Row) -> Result<(), NzError> {
        self.rows += 1;
        Err(NzError::Config("consumer stopped".into()))
    }
}

struct RowSignalSink {
    row_seen: mpsc::Sender<()>,
}

impl QueryStreamSink for RowSignalSink {
    fn on_columns(
        &mut self,
        _result_set_index: usize,
        _columns: &[ColumnDesc],
        _nullability: Option<&[bool]>,
    ) -> Result<(), NzError> {
        Ok(())
    }

    fn on_row(&mut self, _result_set_index: usize, _row: Row) -> Result<(), NzError> {
        self.row_seen
            .send(())
            .map_err(|error| NzError::Closed(error.to_string()))
    }
}

/// Send a backend message: `[type][4-byte shared header][len(4)][payload]`.
fn send_message(stream: &mut impl Write, msg_type: u8, payload: &[u8]) {
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
        external_files: nz_rust::ExternalFilePolicy::Unrestricted,
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
fn async_cancel_after_streamed_row_preserves_session() {
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
        assert_eq!(read_query(&mut main), "SELECT streaming");
        send_message(&mut main, b'T', &row_description_payload());
        send_message(&mut main, b'D', &data_row_payload());

        let (mut cancel, _) = listener.accept().unwrap();
        let mut packet = [0u8; 16];
        cancel.read_exact(&mut packet).unwrap();
        assert_eq!(&packet[0..4], &16i32.to_be_bytes());
        assert_eq!(&packet[4..8], &80877102i32.to_be_bytes());
        assert_eq!(&packet[8..12], &5857i32.to_be_bytes());
        assert_eq!(&packet[12..16], &(-2_092_017_624i32).to_be_bytes());

        send_cancelled_response(&mut main);
        assert_eq!(read_query(&mut main), "SELECT after_stream_cancel");
        send_select_response(&mut main);
    });

    let config = NzConnectionConfig {
        external_files: nz_rust::ExternalFilePolicy::Unrestricted,
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
    let local = tokio::task::LocalSet::new();
    let (row_seen_tx, row_seen_rx) = mpsc::channel();
    local.block_on(&runtime, async {
        let connection = nz_rust::AsyncNzConnection::connect(&config)
            .await
            .expect("handshake should succeed");
        let query_connection = connection.clone();
        let query_task = tokio::task::spawn_local(async move {
            query_connection
                .execute_stream(
                    "SELECT streaming",
                    &[],
                    RowSignalSink {
                        row_seen: row_seen_tx,
                    },
                )
                .await
        });

        tokio::task::spawn_blocking(move || {
            row_seen_rx
                .recv_timeout(Duration::from_secs(2))
                .expect("stream should deliver its first row before cancel");
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
            .query("SELECT after_stream_cancel", &[])
            .await
            .expect("same session should be reusable after stream cancel");
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
        external_files: nz_rust::ExternalFilePolicy::Unrestricted,
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
        external_files: nz_rust::ExternalFilePolicy::Unrestricted,
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
fn streaming_sink_abort_cancels_and_preserves_same_session() {
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
        assert_eq!(read_query(&mut main), "SELECT stream_abort");
        send_message(&mut main, b'T', &row_description_payload());
        send_message(&mut main, b'D', &data_row_payload());
        send_message(&mut main, b'D', &data_row_payload());

        let (mut cancel, _) = listener.accept().unwrap();
        let mut packet = [0u8; 16];
        cancel.read_exact(&mut packet).unwrap();
        assert_eq!(&packet[0..4], &16i32.to_be_bytes());
        assert_eq!(&packet[4..8], &80877102i32.to_be_bytes());
        assert_eq!(&packet[8..12], &5857i32.to_be_bytes());
        assert_eq!(&packet[12..16], &(-2_092_017_624i32).to_be_bytes());

        send_cancelled_response(&mut main);
        assert_eq!(read_query(&mut main), "SELECT after_stream_abort");
        send_select_response(&mut main);
    });

    let config = NzConnectionConfig {
        external_files: nz_rust::ExternalFilePolicy::Unrestricted,
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
    let mut sink = AbortingSink { rows: 0 };
    let error = connection
        .execute_stream("SELECT stream_abort", &[], &mut sink)
        .expect_err("sink abort should return the consumer error");
    assert!(matches!(error, NzError::Config(message) if message == "consumer stopped"));
    assert_eq!(sink.rows, 1, "no rows should be delivered after sink abort");

    let recovered = connection
        .query("SELECT after_stream_abort", &[])
        .expect("same session should be reusable after a sink abort");
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
        external_files: nz_rust::ExternalFilePolicy::Unrestricted,
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
        external_files: nz_rust::ExternalFilePolicy::Unrestricted,
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
        external_files: nz_rust::ExternalFilePolicy::Unrestricted,
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
        external_files: nz_rust::ExternalFilePolicy::Unrestricted,
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
        external_files: nz_rust::ExternalFilePolicy::Unrestricted,
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
async fn native_query_batches_are_produced_in_bounded_groups() {
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
        assert_eq!(read_query(&mut stream), "SELECT batches");
        send_message(&mut stream, b'T', &row_description_payload());
        for _ in 0..300 {
            send_message(&mut stream, b'D', &data_row_payload());
        }
        send_message(&mut stream, b'C', b"SELECT 300\0");
        send_ready(&mut stream);
    });

    let (client, connection) = nz_rust::connect(&mock_config(addr.port())).await.unwrap();
    let driver = tokio::spawn(connection);
    let mut batches = client.query_batches("SELECT batches", &[]).await.unwrap();
    let mut sizes = Vec::new();
    let mut total = 0;
    while let Some(batch) =
        std::future::poll_fn(|cx| std::pin::Pin::new(&mut batches).poll_next(cx)).await
    {
        let batch = batch.unwrap();
        assert!(!batch.is_empty());
        assert!(batch.len() <= 256);
        assert!(batch
            .iter()
            .all(|row| row.try_get::<_, i32>("ONE").unwrap() == 7));
        total += batch.len();
        sizes.push(batch.len());
    }
    assert_eq!(sizes, [256, 44]);
    assert_eq!(total, 300);

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
        external_files: nz_rust::ExternalFilePolicy::Unrestricted,
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_query_batches_preserve_first_result_set_only_semantics() {
    let listener = match TcpListener::bind("127.0.0.1:0") {
        Ok(listener) => listener,
        Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => return,
        Err(error) => panic!("cannot bind TCP mock: {error}"),
    };
    let address = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        serve_handshake(&mut stream);
        assert_eq!(read_query(&mut stream), "SELECT first; SELECT empty_second");
        send_select_with_empty_additional_result(&mut stream);
    });
    let (client, connection) = nz_rust::connect(&mock_config(address.port()))
        .await
        .unwrap();
    let driver = tokio::spawn(connection);
    let mut batches = client
        .query_batches("SELECT first; SELECT empty_second", &[])
        .await
        .unwrap();
    let first_batch = std::future::poll_fn(|cx| std::pin::Pin::new(&mut batches).poll_next(cx))
        .await
        .expect("first set should yield a batch")
        .unwrap();
    assert_eq!(first_batch.len(), 1);
    let error = std::future::poll_fn(|cx| std::pin::Pin::new(&mut batches).poll_next(cx))
        .await
        .expect("second set must be reported")
        .expect_err("query_batches exposes only the first result set");
    assert!(matches!(error, NzError::Config(message) if message.contains("first result set")));
    assert!(
        std::future::poll_fn(|cx| std::pin::Pin::new(&mut batches).poll_next(cx))
            .await
            .is_none()
    );
    client.close().await.unwrap();
    assert!(driver.await.unwrap().is_ok());
    server.join().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_async_stream_emits_notices_between_rows() {
    let listener = match TcpListener::bind("127.0.0.1:0") {
        Ok(listener) => listener,
        Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => return,
        Err(error) => panic!("cannot bind TCP mock: {error}"),
    };
    let address = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        serve_handshake(&mut stream);
        assert_eq!(read_query(&mut stream), "SELECT notices");
        send_message(&mut stream, b'N', b"SNOTICE\0Mbefore row\0\0");
        send_message(&mut stream, b'T', &row_description_payload());
        send_message(&mut stream, b'D', &data_row_payload());
        send_message(&mut stream, b'N', b"SNOTICE\0Mafter row\0\0");
        send_message(&mut stream, b'C', b"SELECT 1\0");
        send_ready(&mut stream);
    });
    let (client, connection) = nz_rust::connect(&mock_config(address.port()))
        .await
        .unwrap();
    let driver = tokio::spawn(connection);
    let mut events = client
        .query_stream_events("SELECT notices", &[])
        .await
        .unwrap();
    let mut order = Vec::new();
    while let Some(event) =
        std::future::poll_fn(|cx| std::pin::Pin::new(&mut events).poll_next(cx)).await
    {
        match event.unwrap() {
            QueryStreamEvent::Notice(message) => order.push(message),
            QueryStreamEvent::ResultSetStart { .. }
            | QueryStreamEvent::ResultSetEnd { .. }
            | QueryStreamEvent::CommandComplete { .. } => {}
            QueryStreamEvent::Row(row) => {
                assert_eq!(row.try_get::<_, i32>("ONE").unwrap(), 7);
                order.push("row".into());
            }
        }
    }
    assert_eq!(order, ["before row", "row", "after row"]);
    assert_eq!(events.notices(), ["before row", "after row"]);
    client.close().await.unwrap();
    assert!(driver.await.unwrap().is_ok());
    server.join().unwrap();
}

#[test]
fn synchronous_import_reader_sends_bounded_chunks() {
    let listener = match TcpListener::bind("127.0.0.1:0") {
        Ok(listener) => listener,
        Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => return,
        Err(error) => panic!("cannot bind TCP mock: {error}"),
    };
    let address = listener.local_addr().unwrap();
    let source = format!("virtual://sync-{}", std::process::id());
    let expected = vec![b'x'; 1001];
    let source_for_server = source.clone();
    let expected_for_server = expected.clone();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        serve_handshake(&mut stream);
        assert_eq!(read_query(&mut stream), "INSERT FROM EXTERNAL");
        send_import_request(&mut stream, &source_for_server, 17);
        assert_eq!(read_import_data(&mut stream, 17), expected_for_server);
        send_message(&mut stream, b'C', b"INSERT 1\0");
        send_ready(&mut stream);
    });
    let mut connection = NzConnection::connect(&mock_config(address.port())).unwrap();
    connection
        .query_with_import_reader(
            "INSERT FROM EXTERNAL",
            &[],
            &source,
            std::io::Cursor::new(expected),
        )
        .unwrap();
    connection.close();
    server.join().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_async_import_reader_sends_bounded_chunks() {
    let listener = match TcpListener::bind("127.0.0.1:0") {
        Ok(listener) => listener,
        Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => return,
        Err(error) => panic!("cannot bind TCP mock: {error}"),
    };
    let address = listener.local_addr().unwrap();
    let source = format!("virtual://async-{}", std::process::id());
    let expected = vec![b'y'; 1001];
    let source_for_server = source.clone();
    let expected_for_server = expected.clone();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        serve_handshake(&mut stream);
        assert_eq!(read_query(&mut stream), "INSERT FROM EXTERNAL");
        send_import_request(&mut stream, &source_for_server, 23);
        assert_eq!(read_import_data(&mut stream, 23), expected_for_server);
        send_message(&mut stream, b'C', b"INSERT 1\0");
        send_ready(&mut stream);
    });
    let (client, connection) = nz_rust::connect(&mock_config(address.port()))
        .await
        .unwrap();
    let driver = tokio::spawn(connection);
    client
        .query_with_import_reader(
            "INSERT FROM EXTERNAL",
            &[],
            &source,
            std::io::Cursor::new(expected),
        )
        .await
        .unwrap();
    client.close().await.unwrap();
    assert!(driver.await.unwrap().is_ok());
    server.join().unwrap();
}

#[test]
fn external_export_writes_each_protocol_chunk() {
    let listener = match TcpListener::bind("127.0.0.1:0") {
        Ok(listener) => listener,
        Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => return,
        Err(error) => panic!("cannot bind TCP mock: {error}"),
    };
    let address = listener.local_addr().unwrap();
    let path = std::env::temp_dir().join(format!(
        "nz_rust_export_{}_{}.txt",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let filename = path.to_string_lossy().into_owned();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        serve_handshake(&mut stream);
        assert_eq!(read_query(&mut stream), "EXPORT EXTERNAL");
        let mut start = vec![b'u'];
        start.extend_from_slice(&[0; 4 + 10 + 16]);
        start.extend_from_slice(&((filename.len() + 1) as i32).to_be_bytes());
        start.extend_from_slice(filename.as_bytes());
        start.push(0);
        write_be(&mut stream, &start);
        let mut ack = [0; 4];
        stream.read_exact(&mut ack).unwrap();
        assert_eq!(ack, [0; 4]);
        let mut data = vec![b'U'];
        data.extend_from_slice(&[0; 8]);
        for part in [b"1|alpha\n".as_slice(), b"2|beta\n".as_slice()] {
            data.extend_from_slice(&1i32.to_be_bytes());
            data.extend_from_slice(&(part.len() as i32).to_be_bytes());
            data.extend_from_slice(part);
        }
        data.extend_from_slice(&3i32.to_be_bytes());
        write_be(&mut stream, &data);
        send_message(&mut stream, b'C', b"CREATE EXTERNAL TABLE\0");
        send_ready(&mut stream);
    });
    let mut connection = NzConnection::connect(&mock_config(address.port())).unwrap();
    connection.query("EXPORT EXTERNAL", &[]).unwrap();
    connection.close();
    server.join().unwrap();
    assert_eq!(std::fs::read(&path).unwrap(), b"1|alpha\n2|beta\n");
    std::fs::remove_file(path).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_cancel_with_full_event_queue_drains_and_reuses_session() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let config = mock_config(listener.local_addr().unwrap().port());
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let server = thread::spawn(move || {
        let (mut main, _) = listener.accept().unwrap();
        main.set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        serve_handshake(&mut main);
        assert_eq!(read_query(&mut main), "SELECT stalled");
        send_message(&mut main, b'T', &row_description_payload());
        for _ in 0..512 {
            send_message(&mut main, b'D', &data_row_payload());
        }
        started_tx.send(()).unwrap();
        let (mut cancel, _) = listener.accept().unwrap();
        let mut packet = [0; 16];
        cancel.read_exact(&mut packet).unwrap();
        assert_eq!(
            i32::from_be_bytes(packet[4..8].try_into().unwrap()),
            80877102
        );
        send_cancelled_response(&mut main);
        assert_eq!(read_query(&mut main), "SELECT recovered");
        send_select_response(&mut main);
    });
    let client = nz_rust::Client::connect(&config).await.unwrap();
    let mut rows = client.query_stream("SELECT stalled", &[]).await.unwrap();
    started_rx.await.unwrap();
    client.cancel().await.unwrap();
    let recovered = tokio::time::timeout(
        Duration::from_secs(8),
        client.query("SELECT recovered", &[]),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(recovered[0].try_get::<_, i32>(0).unwrap(), 7);
    let mut cancelled = false;
    while let Some(item) =
        std::future::poll_fn(|cx| std::pin::Pin::new(&mut rows).poll_next(cx)).await
    {
        if matches!(item, Err(NzError::Cancelled(_))) {
            cancelled = true;
        }
    }
    assert!(cancelled);
    client.close().await.unwrap();
    server.join().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_pool_drop_returns_slot_after_cleanup() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let config = mock_config(listener.local_addr().unwrap().port());
    let server = thread::spawn(move || {
        let (mut main, _) = listener.accept().unwrap();
        main.set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        serve_handshake(&mut main);
        assert_eq!(read_query(&mut main), "ROLLBACK");
        send_message(&mut main, b'C', b"ROLLBACK\0");
        send_ready(&mut main);
        assert_eq!(read_query(&mut main), "SELECT reused");
        send_select_response(&mut main);
        assert_eq!(read_query(&mut main), "ROLLBACK");
        send_message(&mut main, b'C', b"ROLLBACK\0");
        send_ready(&mut main);
    });
    let mut options = nz_rust::AsyncNzPoolConfig::new(config);
    options.max = 1;
    options.wait_timeout = Some(Duration::from_secs(5));
    let pool = nz_rust::AsyncNzPool::new(options).unwrap();
    drop(pool.get().await.unwrap());
    let holder = pool.get().await.unwrap();
    assert_eq!(holder.query("SELECT reused", &[]).await.unwrap().len(), 1);
    holder.release().await;
    assert_eq!(pool.total_count().await, 1);
    assert_eq!(pool.idle_count().await, 1);
    pool.end().await;
    assert_eq!(pool.total_count().await, 0);
    server.join().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transaction_guard_serializes_clones_and_rolls_back_on_drop() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let config = mock_config(listener.local_addr().unwrap().port());
    let server = thread::spawn(move || {
        let (mut main, _) = listener.accept().unwrap();
        main.set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        serve_handshake(&mut main);
        for sql in ["BEGIN", "ROLLBACK"] {
            assert_eq!(read_query(&mut main), sql);
            send_message(&mut main, b'C', format!("{sql}\0").as_bytes());
            send_ready(&mut main);
        }
        assert_eq!(read_query(&mut main), "SELECT outside");
        send_select_response(&mut main);
    });
    let client = nz_rust::Client::connect(&config).await.unwrap();
    let transaction = client.transaction().await.unwrap();
    let clone = client.clone();
    let outside = tokio::spawn(async move { clone.query("SELECT outside", &[]).await });
    tokio::task::yield_now().await;
    assert!(!outside.is_finished());
    drop(transaction);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), outside)
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .len(),
        1
    );
    client.close().await.unwrap();
    server.join().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn canceled_pool_connect_rolls_back_reservation() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let config = mock_config(listener.local_addr().unwrap().port());
    let server = thread::spawn(move || {
        let (mut stalled, _) = listener.accept().unwrap();
        stalled
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut buffer = [0; 128];
        while stalled.read(&mut buffer).unwrap() != 0 {}
        let (mut main, _) = listener.accept().unwrap();
        serve_handshake(&mut main);
        assert_eq!(read_query(&mut main), "ROLLBACK");
        send_message(&mut main, b'C', b"ROLLBACK\0");
        send_ready(&mut main);
    });
    let mut options = nz_rust::PoolConfig::new(config);
    options.max = 1;
    options.wait_timeout = Some(Duration::from_millis(100));
    let pool = nz_rust::Pool::new(options).unwrap();
    assert!(matches!(pool.get().await, Err(NzError::Timeout(_))));
    assert_eq!(pool.total_count().await, 0);
    let holder = pool.get().await.unwrap();
    holder.release().await;
    assert_eq!(pool.idle_count().await, 1);
    pool.close().await;
    server.join().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_timeout_retains_parser_mid_frame_and_reuses_session() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let config = mock_config(listener.local_addr().unwrap().port());
    let server = thread::spawn(move || {
        let (mut main, _) = listener.accept().unwrap();
        serve_handshake(&mut main);
        assert_eq!(read_query(&mut main), "SELECT slow_frame");
        send_message(&mut main, b'T', &row_description_payload());
        let body = data_row_payload();
        let mut header = vec![b'D'];
        header.extend_from_slice(&[0; 4]);
        header.extend_from_slice(&(body.len() as i32).to_be_bytes());
        header.extend_from_slice(&body[..3]);
        write_be(&mut main, &header);
        let (mut cancel, _) = listener.accept().unwrap();
        let mut packet = [0; 16];
        cancel.read_exact(&mut packet).unwrap();
        write_be(&mut main, &body[3..]);
        send_cancelled_response(&mut main);
        assert_eq!(read_query(&mut main), "SELECT recovered");
        send_select_response(&mut main);
    });
    let client = nz_rust::Client::connect(&config).await.unwrap();
    let mut rows = client
        .query_stream_with_options(
            "SELECT slow_frame",
            &[],
            nz_rust::QueryOptions {
                timeout: Some(Duration::from_millis(50)),
            },
        )
        .await
        .unwrap();
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        std::future::poll_fn(|cx| std::pin::Pin::new(&mut rows).poll_next(cx)),
    )
    .await
    .unwrap();
    assert!(matches!(result, Some(Err(NzError::Timeout(_)))));
    assert_eq!(
        client.query("SELECT recovered", &[]).await.unwrap().len(),
        1
    );
    client.close().await.unwrap();
    server.join().unwrap();
}

#[cfg(feature = "ssl")]
fn serve_tls_transport(
    mut stream: TcpStream,
) -> rustls::StreamOwned<rustls::ServerConnection, TcpStream> {
    // Static localhost fixture keys are exclusively for tests.
    assert_eq!(read_frame(&mut stream).0, 1);
    write_be(&mut stream, b"N");
    assert_eq!(read_frame(&mut stream).0, 2);
    write_be(&mut stream, b"N");
    assert_eq!(read_frame(&mut stream).0, 11);
    write_be(&mut stream, b"S");
    assert_eq!(read_frame(&mut stream).0, 12);
    let certificates = rustls_pemfile::certs(&mut std::io::Cursor::new(include_bytes!(
        "fixtures/tls/localhost-cert.pem"
    )))
    .collect::<Result<Vec<_>, _>>()
    .unwrap();
    let key = rustls_pemfile::private_key(&mut std::io::Cursor::new(include_bytes!(
        "fixtures/tls/localhost-key.pem"
    )))
    .unwrap()
    .unwrap();
    let config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certificates, key)
        .unwrap();
    rustls::StreamOwned::new(
        rustls::ServerConnection::new(std::sync::Arc::new(config)).unwrap(),
        stream,
    )
}

#[cfg(feature = "ssl")]
fn serve_tls_handshake(
    stream: TcpStream,
) -> rustls::StreamOwned<rustls::ServerConnection, TcpStream> {
    let mut stream = serve_tls_transport(stream);
    write_be(&mut stream, b"N");
    serve_handshake_options(&mut stream);
    stream
}

#[cfg(feature = "ssl")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_and_legacy_tls_handshake_query_and_certificate_validation() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let mut config = mock_config(listener.local_addr().unwrap().port());
    config.security_level = SecurityLevel::OnlySecuredSession;
    config.ssl_cert_path = Some(format!(
        "{}/tests/fixtures/tls/localhost-cert.pem",
        env!("CARGO_MANIFEST_DIR")
    ));
    let server = thread::spawn(move || {
        for _ in 0..2 {
            let (stream, _) = listener.accept().unwrap();
            let mut stream = serve_tls_handshake(stream);
            assert_eq!(read_query(&mut stream), "SELECT tls");
            send_select_response(&mut stream);
        }
    });
    let client = nz_rust::Client::connect(&config).await.unwrap();
    assert_eq!(client.query("SELECT tls", &[]).await.unwrap().len(), 1);
    client.close().await.unwrap();
    let sync_config = config.clone();
    tokio::task::spawn_blocking(move || {
        let mut connection = NzConnection::connect(&sync_config).unwrap();
        assert_eq!(connection.query("SELECT tls", &[]).unwrap().rows().len(), 1);
        connection.close();
    })
    .await
    .unwrap();
    server.join().unwrap();
}

#[cfg(feature = "ssl")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_and_legacy_tls_reject_untrusted_certificate() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let mut config = mock_config(listener.local_addr().unwrap().port());
    config.security_level = SecurityLevel::OnlySecuredSession;
    let server = thread::spawn(move || {
        for _ in 0..2 {
            let (stream, _) = listener.accept().unwrap();
            let mut tls = serve_tls_transport(stream);
            assert!(tls.conn.complete_io(&mut tls.sock).is_err());
        }
    });
    assert!(nz_rust::Client::connect(&config).await.is_err());
    tokio::task::spawn_blocking(move || {
        assert!(NzConnection::connect(&config).is_err());
    })
    .await
    .unwrap();
    server.join().unwrap();
}

/// The pools' idle-socket probe must not retire healthy TLS sessions: pending
/// TLS records (e.g. TLS 1.3 session tickets) are not protocol data.
#[cfg(feature = "ssl")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_and_legacy_pools_reuse_idle_tls_sessions() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let mut config = mock_config(listener.local_addr().unwrap().port());
    config.security_level = SecurityLevel::OnlySecuredSession;
    config.ssl_cert_path = Some(format!(
        "{}/tests/fixtures/tls/localhost-cert.pem",
        env!("CARGO_MANIFEST_DIR")
    ));
    let server = thread::spawn(move || {
        // Exactly one physical session per pool: a retired session would
        // need a second accept that never comes.
        let (stream, _) = listener.accept().unwrap();
        let mut native = serve_tls_handshake(stream);
        for _ in 0..2 {
            assert_eq!(read_query(&mut native), "SELECT tls");
            send_select_response(&mut native);
            assert_eq!(read_query(&mut native), "ROLLBACK");
            send_message(&mut native, b'C', b"ROLLBACK\0");
            send_ready(&mut native);
        }
        let (stream, _) = listener.accept().unwrap();
        let mut legacy = serve_tls_handshake(stream);
        for _ in 0..2 {
            assert_eq!(read_query(&mut legacy), "SELECT tls");
            send_select_response(&mut legacy);
        }
    });
    let mut options = nz_rust::PoolConfig::new(config.clone());
    options.max = 1;
    let pool = nz_rust::Pool::new(options).unwrap();
    for _ in 0..2 {
        let lease = pool.get().await.unwrap();
        assert_eq!(lease.query("SELECT tls", &[]).await.unwrap().len(), 1);
        lease.release().await;
    }
    assert_eq!(pool.total_count().await, 1);
    pool.close().await;
    tokio::task::spawn_blocking(move || {
        let mut options = nz_rust::NzPoolConfig::new(config);
        options.max = 1;
        let pool = nz_rust::NzPool::new(options).unwrap();
        for _ in 0..2 {
            let mut conn = pool.get().unwrap();
            assert_eq!(conn.query("SELECT tls", &[]).unwrap().rows().len(), 1);
        }
        assert_eq!(pool.total_count(), 1);
        pool.close();
    })
    .await
    .unwrap();
    server.join().unwrap();
}
