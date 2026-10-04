//! Mock-server coverage for the public Tokio-native client, independent of
//! the optional `compat` API and a live Netezza appliance.

use futures_core::Stream;
use nz_rust::{Client, NzConnectionConfig, NzError};
use std::future::poll_fn;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::pin::Pin;
use std::thread;

fn read_frame(stream: &mut impl Read) -> (i16, Vec<u8>) {
    let mut header = [0u8; 6];
    stream.read_exact(&mut header).unwrap();
    let length = i32::from_be_bytes(header[..4].try_into().unwrap()) as usize;
    let opcode = i16::from_be_bytes(header[4..6].try_into().unwrap());
    let mut payload = vec![0; length - 6];
    stream.read_exact(&mut payload).unwrap();
    (opcode, payload)
}

fn write_all(stream: &mut impl Write, bytes: &[u8]) {
    stream.write_all(bytes).unwrap();
    stream.flush().unwrap();
}

fn serve_handshake(stream: &mut TcpStream) {
    let (opcode, _) = read_frame(stream);
    assert_eq!(opcode, 1);
    write_all(stream, b"N");
    let (opcode, _) = read_frame(stream);
    assert_eq!(opcode, 2);
    write_all(stream, b"N");
    let (opcode, _) = read_frame(stream);
    assert_eq!(opcode, 11);
    write_all(stream, b"N");
    loop {
        let (opcode, _) = read_frame(stream);
        if opcode == 1000 {
            break;
        }
        write_all(stream, b"N");
    }

    let mut auth = vec![b'R'];
    auth.extend_from_slice(&0i32.to_be_bytes());
    write_all(stream, &auth);
    let mut key = vec![b'K'];
    key.extend_from_slice(&[0; 4]);
    key.extend_from_slice(&12i32.to_be_bytes());
    key.extend_from_slice(&5857i32.to_be_bytes());
    key.extend_from_slice(&(-2_092_017_624i32).to_be_bytes());
    write_all(stream, &key);
    let mut ready = vec![b'Z'];
    ready.extend_from_slice(&[0; 4]);
    write_all(stream, &ready);
}

fn read_query(stream: &mut TcpStream) -> Option<String> {
    let mut header = [0; 5];
    if stream.read_exact(&mut header).is_err() {
        return None;
    }
    assert_eq!(header[0], b'P');
    let mut sql = Vec::new();
    loop {
        let mut byte = [0];
        stream.read_exact(&mut byte).ok()?;
        if byte[0] == 0 {
            return Some(String::from_utf8(sql).unwrap());
        }
        sql.push(byte[0]);
    }
}

fn send_message(stream: &mut impl Write, kind: u8, payload: &[u8]) {
    let mut frame = Vec::with_capacity(9 + payload.len());
    frame.push(kind);
    frame.extend_from_slice(&[0; 4]);
    frame.extend_from_slice(&(payload.len() as i32).to_be_bytes());
    frame.extend_from_slice(payload);
    write_all(stream, &frame);
}

fn row_description() -> Vec<u8> {
    let mut payload = 2u16.to_be_bytes().to_vec();
    for (name, oid, size) in [("ONE", 23i32, 4i16), ("TXT", 1043i32, -1i16)] {
        payload.extend_from_slice(name.as_bytes());
        payload.push(0);
        payload.extend_from_slice(&oid.to_be_bytes());
        payload.extend_from_slice(&size.to_be_bytes());
        payload.extend_from_slice(&(-1i32).to_be_bytes());
        payload.push(0);
    }
    payload
}

fn data_row(value: i32) -> Vec<u8> {
    let mut payload = vec![0b1100_0000];
    let number = value.to_string();
    payload.extend_from_slice(&((4 + number.len()) as i32).to_be_bytes());
    payload.extend_from_slice(number.as_bytes());
    payload.extend_from_slice(&7i32.to_be_bytes());
    payload.extend_from_slice(b"row");
    payload
}

fn dbos_descriptor() -> Vec<u8> {
    let mut payload = Vec::with_capacity(80);
    for value in [1i32, 0, 4, 4, 1, 0, 6, 6, 1] {
        payload.extend_from_slice(&value.to_be_bytes());
    }
    for value in [3i32, 4, 4, 2, 0, 0, 0, 4, 0, 0, 0] {
        payload.extend_from_slice(&value.to_be_bytes());
    }
    payload
}

fn send_dbos_row(stream: &mut impl Write, payload: &[u8]) {
    let mut frame = vec![b'Y'];
    frame.extend_from_slice(&[0; 4]);
    frame.extend_from_slice(&0i32.to_be_bytes());
    frame.extend_from_slice(&(payload.len() as i32).to_be_bytes());
    frame.extend_from_slice(payload);
    write_all(stream, &frame);
}

fn dbos_response(sql: &str) -> Vec<u8> {
    let mut wire = Vec::new();
    send_message(&mut wire, b'X', &dbos_descriptor());
    let mut row = vec![0, 0];
    row.extend_from_slice(&7i32.to_le_bytes());
    send_dbos_row(&mut wire, &row);
    if sql == "SELECT DBOS_BAD_TAIL" {
        send_dbos_row(&mut wire, &vec![0xff; 64 * 1024]);
    }
    send_message(&mut wire, b'C', b"SELECT 1\0");
    wire.push(b'Z');
    wire.extend_from_slice(&[0; 4]);
    wire
}

fn response(sql: &str) -> Vec<u8> {
    let mut wire = Vec::new();
    if matches!(sql, "SELECT DBOS_BAD_TAIL" | "SELECT DBOS_ONE") {
        return dbos_response(sql);
    }
    if sql == "SELECT MULTISET" {
        for value in [1, 2] {
            send_message(&mut wire, b'T', &row_description());
            send_message(&mut wire, b'D', &data_row(value));
            send_message(&mut wire, b'C', b"SELECT 1\0");
        }
        wire.push(b'Z');
        wire.extend_from_slice(&[0; 4]);
        return wire;
    }
    send_message(&mut wire, b'T', &row_description());
    let row_count = if sql == "SELECT EMPTY" {
        0
    } else if sql == "SELECT ONE" || sql == "SELECT 'O''Brien'" {
        1
    } else if sql == "SELECT DISCARD" || sql == "SELECT ONE_BAD_TAIL" {
        2
    } else {
        3
    };
    for index in 0..row_count {
        if matches!(sql, "SELECT DISCARD" | "SELECT ONE_BAD_TAIL") && index == 1 {
            // A valid outer frame with intentionally invalid row internals.
            send_message(&mut wire, b'D', &vec![0xff; 64 * 1024]);
        } else {
            send_message(&mut wire, b'D', &data_row(index + 1));
        }
    }
    let tag = format!("SELECT {row_count}\0");
    send_message(&mut wire, b'C', tag.as_bytes());
    wire.push(b'Z');
    wire.extend_from_slice(&[0; 4]);
    wire
}

fn spawn_server() -> (u16, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let thread = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(std::time::Duration::from_secs(10)))
            .unwrap();
        serve_handshake(&mut stream);
        let expected = [
            "SELECT MANY",
            "SELECT MANY",
            "SELECT MANY",
            "SELECT ONE_BAD_TAIL",
            "SELECT MULTISET",
            "SELECT DBOS_BAD_TAIL",
            "SELECT DBOS_ONE",
            "SELECT DISCARD",
            "SELECT ONE",
            "SELECT EMPTY",
            "SELECT MANY",
            "SELECT MANY",
            "SELECT 'O''Brien'",
        ];
        for sql_expected in expected {
            let sql = read_query(&mut stream).expect("client query packet");
            assert_eq!(sql, sql_expected);
            write_all(&mut stream, &response(&sql));
        }
    });
    (port, thread)
}

fn config(port: u16) -> NzConnectionConfig {
    NzConnectionConfig {
        host: "127.0.0.1".into(),
        port,
        database: "JUST_DATA".into(),
        user: "admin".into(),
        password: "secret".into(),
        ..Default::default()
    }
}

async fn next_item<S: Stream + Unpin>(stream: &mut S) -> Option<S::Item> {
    poll_fn(|cx| Pin::new(&mut *stream).poll_next(cx)).await
}

#[tokio::test]
async fn native_query_stream_batch_cardinality_and_discard_paths() {
    let (port, server) = spawn_server();
    let client = Client::connect(&config(port)).await.unwrap();

    let rows = client.query("SELECT MANY", &[]).await.unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].try_get::<_, i32>("one").unwrap(), 1);
    assert_eq!(rows[0].try_get::<_, String>("TXT").unwrap(), "row");

    let error = client.query_one("SELECT MANY", &[]).await.unwrap_err();
    assert!(matches!(error, NzError::Config(message) if message.contains("got 3")));
    let error = client.query_opt("SELECT MANY", &[]).await.unwrap_err();
    assert!(matches!(error, NzError::Config(message) if message.contains("got 3")));

    let error = client
        .query_one("SELECT ONE_BAD_TAIL", &[])
        .await
        .unwrap_err();
    assert!(matches!(error, NzError::Config(message) if message.contains("got 2")));
    let error = client.query_one("SELECT MULTISET", &[]).await.unwrap_err();
    assert!(matches!(error, NzError::Config(message) if message.contains("multiple result sets")));
    let error = client
        .query_one("SELECT DBOS_BAD_TAIL", &[])
        .await
        .unwrap_err();
    assert!(matches!(error, NzError::Config(message) if message.contains("got 2")));
    let dbos_row = client.query_one("SELECT DBOS_ONE", &[]).await.unwrap();
    assert_eq!(dbos_row.try_get::<_, i32>(0).unwrap(), 7);

    // A 64 KiB invalid row body exercises bounded draining without decoding.
    assert_eq!(client.execute("SELECT DISCARD", &[]).await.unwrap(), 2);
    let row = client.query_one("SELECT ONE", &[]).await.unwrap();
    assert_eq!(row.try_get::<_, i32>(0).unwrap(), 1);
    assert!(client
        .query_opt("SELECT EMPTY", &[])
        .await
        .unwrap()
        .is_none());

    let mut stream = client.query_stream("SELECT MANY", &[]).await.unwrap();
    let mut streamed_rows = 0;
    while let Some(row) = next_item(&mut stream).await {
        assert!(row.is_ok());
        streamed_rows += 1;
    }
    assert_eq!(streamed_rows, 3);

    let mut batches = client.query_batches("SELECT MANY", &[]).await.unwrap();
    let mut batched_rows = 0;
    while let Some(batch) = next_item(&mut batches).await {
        batched_rows += batch.unwrap().len();
    }
    assert_eq!(batched_rows, 3);

    let value = "O'Brien";
    assert_eq!(client.execute("SELECT $1", &[&value]).await.unwrap(), 1);

    client.close().await.unwrap();
    server.join().unwrap();
}
