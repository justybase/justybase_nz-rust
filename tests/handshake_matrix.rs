//! Handshake version negotiation and authentication matrix, end to end over
//! the mock backend for both the native and the legacy engine.
//!
//! The backend records every option frame and the raw authentication
//! response, so the tests check the exact bytes the driver sends for each
//! negotiated version and authentication method. The password never appears
//! in assertion messages; only derived digests are compared.

mod support;

use nz_rust::{NzConnectionConfig, NzError};
use sha2::Digest;
use support::*;

const USER: &str = "admin";
const DATABASE: &str = "JUST_DATA";

fn cstring(value: &str) -> Vec<u8> {
    let mut out = value.as_bytes().to_vec();
    out.push(0);
    out
}

/// Expected option opcodes after `CLIENT_BEGIN` for a negotiated version.
fn expected_opcodes(version: i16) -> Vec<i16> {
    let mut opcodes = vec![OP_DB, OP_SSL_NEGOTIATE, OP_USER];
    if version == 4 || version == 6 {
        opcodes.extend([
            OP_APPNAME,
            OP_CLIENT_OS,
            OP_CLIENT_HOST_NAME,
            OP_CLIENT_OS_USER,
        ]);
    }
    opcodes.extend([OP_PROTOCOL, OP_REMOTE_PID, OP_CLIENT_TYPE]);
    if version >= 5 {
        opcodes.push(OP_64BIT_VARLENA);
    }
    opcodes.push(OP_CLIENT_DONE);
    opcodes
}

fn check_option_payloads(log: &HandshakeLog, config: &NzConnectionConfig, version: i16) {
    let payload_of = |opcode: i16| -> &[u8] {
        let index = log
            .opcodes
            .iter()
            .position(|&op| op == opcode)
            .unwrap_or_else(|| panic!("opcode {opcode} missing"));
        &log.option_payloads[index]
    };
    assert_eq!(payload_of(OP_DB), cstring(DATABASE));
    assert_eq!(payload_of(OP_USER), cstring(USER));
    // PROTOCOL carries (PG protocol 3, data protocol 5).
    assert_eq!(payload_of(OP_PROTOCOL), [0, 3, 0, 5]);
    assert_eq!(payload_of(OP_REMOTE_PID).len(), 4);
    assert_eq!(payload_of(OP_CLIENT_TYPE).len(), 2);
    assert_eq!(payload_of(OP_CLIENT_DONE), b"");
    if version >= 5 {
        assert_eq!(payload_of(OP_64BIT_VARLENA), [0, 1]);
    }
    if version == 4 || version == 6 {
        assert_eq!(payload_of(OP_APPNAME), cstring(&config.app_name));
        assert_eq!(payload_of(OP_CLIENT_OS_USER), cstring(&config.os_user));
        assert_eq!(
            payload_of(OP_CLIENT_HOST_NAME),
            cstring(&config.client_host_name)
        );
    }
}

/// Connect with both engines (legacy only with `compat`); returns one result
/// per engine, labelled.
async fn connect_all(server: &MockServer) -> Vec<(&'static str, Result<(), NzError>)> {
    let config = server.config();
    let mut results = Vec::new();
    let native = nz_rust::Client::connect(&config).await;
    results.push((
        "native",
        native.map(|client| {
            assert!(!client.is_closed());
        }),
    ));
    #[cfg(feature = "compat")]
    {
        let legacy = tokio::task::spawn_blocking(move || {
            nz_rust::NzConnection::connect(&config).map(|mut conn| {
                assert_eq!(conn.backend_process_id(), BASE_PID + 2);
                assert_eq!(conn.backend_secret_key(), SECRET_KEY);
                conn.close();
            })
        })
        .await
        .unwrap();
        results.push(("legacy", legacy));
    }
    results
}

fn assert_no_secret(error: &NzError) {
    let rendered = format!("{error} {error:?}");
    assert!(
        !rendered.contains(MOCK_PASSWORD),
        "error text exposes the password"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn handshake_version_matrix_sends_the_right_options() {
    let cases: Vec<(Vec<VersionReply>, Vec<i16>, i16)> = vec![
        (vec![VersionReply::Accept], vec![6], 6),
        (
            vec![VersionReply::Downgrade(b'5'), VersionReply::Accept],
            vec![6, 5],
            5,
        ),
        (
            vec![VersionReply::Downgrade(b'4'), VersionReply::Accept],
            vec![6, 4],
            4,
        ),
        (
            vec![VersionReply::Downgrade(b'3'), VersionReply::Accept],
            vec![6, 3],
            3,
        ),
        (
            vec![VersionReply::Downgrade(b'2'), VersionReply::Accept],
            vec![6, 2],
            2,
        ),
        (
            vec![
                VersionReply::Downgrade(b'5'),
                VersionReply::Downgrade(b'4'),
                VersionReply::Downgrade(b'3'),
                VersionReply::Downgrade(b'2'),
                VersionReply::Accept,
            ],
            vec![6, 5, 4, 3, 2],
            2,
        ),
    ];
    for (versions, begins, negotiated) in cases {
        let server = MockServer::start(
            HandshakeScript {
                versions,
                ..Default::default()
            },
            |session| session.serve_all(|_| select_one()),
        );
        for (engine, result) in connect_all(&server).await {
            result.unwrap_or_else(|e| panic!("{engine} v{negotiated}: {e}"));
        }
        let logs = server.logs.lock().unwrap().clone();
        assert!(!logs.is_empty());
        let config = server.config();
        for log in logs {
            assert_eq!(log.begin_versions, begins, "v{negotiated}");
            assert_eq!(log.opcodes, expected_opcodes(negotiated), "v{negotiated}");
            check_option_payloads(&log, &config, negotiated);
            assert!(log.completed);
        }
        server.assert_no_handler_panics();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn handshake_rejects_unknown_and_malformed_version_replies() {
    let mut error_frame = b"E".to_vec();
    let body = b"SFATAL\0C08P01\0Mprotocol version rejected\0\0";
    error_frame.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
    error_frame.extend_from_slice(body);
    let cases: Vec<(&str, Vec<VersionReply>)> = vec![
        ("unknown-digit", vec![VersionReply::Downgrade(b'9')]),
        ("version-1", vec![VersionReply::Downgrade(b'1')]),
        ("non-digit", vec![VersionReply::Downgrade(b'x')]),
        ("garbage-byte", vec![VersionReply::Raw(b"Q".to_vec())]),
        ("eof-after-M", vec![VersionReply::Raw(b"M".to_vec())]),
        ("eof-immediately", vec![VersionReply::Raw(Vec::new())]),
        (
            "same-version-again",
            vec![
                VersionReply::Downgrade(b'5'),
                VersionReply::Downgrade(b'5'),
                VersionReply::Accept,
            ],
        ),
        (
            "upgrade-after-downgrade",
            vec![
                VersionReply::Downgrade(b'4'),
                VersionReply::Downgrade(b'5'),
                VersionReply::Accept,
            ],
        ),
        (
            "error-response",
            vec![VersionReply::Raw(error_frame.clone())],
        ),
    ];
    for (name, versions) in cases {
        let server = MockServer::start(
            HandshakeScript {
                versions,
                ..Default::default()
            },
            |session| session.serve_all(|_| select_one()),
        );
        for (engine, result) in connect_all(&server).await {
            let error = result
                .err()
                .unwrap_or_else(|| panic!("{engine} {name}: malformed negotiation accepted"));
            assert!(
                matches!(
                    error,
                    NzError::Protocol(_)
                        | NzError::Closed(_)
                        | NzError::Io(_)
                        | NzError::Database(_)
                ),
                "{engine} {name}: {error:?}"
            );
            if name == "error-response" {
                assert!(matches!(error, NzError::Database(_)), "{engine}: {error:?}");
            }
            assert_no_secret(&error);
        }
    }
}

fn base64_unpadded(data: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for chunk in data.chunks(3) {
        let b = [
            chunk[0],
            chunk.get(1).copied().unwrap_or(0),
            chunk.get(2).copied().unwrap_or(0),
        ];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        for i in 0..=chunk.len() {
            out.push(TABLE[((n >> (18 - 6 * i)) & 63) as usize] as char);
        }
    }
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn authentication_matrix_sends_exact_credentials() {
    let salt = [0x5a, 0xc3];
    let mut salted = salt.to_vec();
    salted.extend_from_slice(MOCK_PASSWORD.as_bytes());
    let md5_expected = cstring(&base64_unpadded(&md5::compute(&salted).0));
    let sha_expected = cstring(&base64_unpadded(&sha2::Sha256::digest(&salted)));
    let cases: Vec<(&str, AuthScript, Option<Vec<u8>>)> = vec![
        ("ok", AuthScript::Ok, None),
        (
            "password",
            AuthScript::Password,
            Some(cstring(MOCK_PASSWORD)),
        ),
        ("md5", AuthScript::Md5(salt), Some(md5_expected)),
        ("sha256", AuthScript::Sha256(salt), Some(sha_expected)),
    ];
    let plans = [
        Chunking::Whole,
        Chunking::Fixed(1),
        Chunking::Seeded {
            seed: 0xa17,
            max: 5,
        },
    ];
    for ((name, auth, expected), chunking) in cases
        .into_iter()
        .flat_map(|case| plans.iter().map(move |plan| (case.clone(), *plan)))
    {
        let server = MockServer::start(
            HandshakeScript {
                auth,
                chunking,
                ..Default::default()
            },
            |session| session.serve_all(|_| select_one()),
        );
        for (engine, result) in connect_all(&server).await {
            if let Err(error) = result {
                assert_no_secret(&error);
                panic!("{engine} {name}: handshake failed: {error}");
            }
        }
        let logs = server.logs.lock().unwrap().clone();
        for log in &logs {
            assert!(log.completed, "{name}");
            // Compare without echoing secrets on failure.
            assert!(
                log.auth_response == expected,
                "{name}: authentication response bytes differ from the reference \
                 (length {:?} vs {:?})",
                log.auth_response.as_ref().map(Vec::len),
                expected.as_ref().map(Vec::len)
            );
        }
        server.assert_no_handler_panics();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn authentication_failures_are_classified_and_never_leak_the_password() {
    let mut auth_error = b"E".to_vec();
    let body = b"SFATAL\0C28P01\0Mpassword authentication failed for user admin\0\0";
    auth_error.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
    auth_error.extend_from_slice(body);
    let cases: Vec<(&str, Vec<u8>)> = vec![
        ("unsupported-type", auth_request(99, None)),
        ("kerberos-type", auth_request(2, None)),
        ("negative-type", auth_request(-1, None)),
        ("truncated-type", b"R\0\0".to_vec()),
        ("truncated-md5-salt", {
            let mut wire = auth_request(5, None);
            wire.push(0x01);
            wire
        }),
        ("truncated-sha256-salt", auth_request(6, None)),
        ("unexpected-byte", b"Q".to_vec()),
        ("error-response", auth_error),
    ];
    for (name, raw) in cases {
        let server = MockServer::start(
            HandshakeScript {
                auth: AuthScript::Raw(raw),
                ..Default::default()
            },
            |session| session.serve_all(|_| select_one()),
        );
        for (engine, result) in connect_all(&server).await {
            let error = result
                .err()
                .unwrap_or_else(|| panic!("{engine} {name}: handshake succeeded"));
            assert_no_secret(&error);
            match name {
                "error-response" => {
                    assert!(matches!(error, NzError::Database(_)), "{engine}: {error:?}")
                }
                "unsupported-type" | "kerberos-type" | "negative-type" | "unexpected-byte" => {
                    assert!(
                        matches!(error, NzError::Protocol(_)),
                        "{engine} {name}: {error:?}"
                    )
                }
                _ => assert!(
                    matches!(error, NzError::Closed(_) | NzError::Io(_)),
                    "{engine} {name}: {error:?}"
                ),
            }
        }
    }
}

#[test]
fn connection_config_debug_never_prints_the_password() {
    let config = mock_config(5480);
    assert!(!format!("{config:?}").contains(MOCK_PASSWORD));
}
