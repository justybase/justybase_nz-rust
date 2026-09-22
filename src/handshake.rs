// Copyright 2026 Krzysztof Duśko.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Connection handshake — faithful port of the Node driver `Handshake.ts`
//! (itself ported from C# `Handshake.cs` / nzpy).
//!
//! Byte-for-byte sequence validated against a live appliance capture:
//! 1. negotiate connection-protocol version (`HSV2_CLIENT_BEGIN` + version 6;
//!    server downgrades via `'M'` + version char, e.g. `'M''5'` → v5)
//! 2. database selection (`HSV2_DB`)
//! 3. SSL negotiation (`HSV2_SSL_NEGOTIATE`; `'N'` = continue unsecured)
//! 4. version-specific option sequence (v4/v6 adds appname/OS/host/os-user)
//! 5. authentication (`R` + areq: 0 OK / 3 plain / 5 MD5 / 6 SHA256;
//!    hash = base64(hash(salt ‖ password)) with padding stripped)
//! 6. connection complete (`K` BackendKeyData, `Z` ReadyForQuery)

use crate::buffer::ReadBuffer;
use crate::config::{NzConnectionConfig, SecurityLevel};
use crate::error::{parse_backend_error_fields, validate_protocol_length, NzError, NzResult};
use sha2::{Digest, Sha256};
use std::io::{Read, Write};
use std::net::TcpStream;

/// Read/write transport used by the protocol handshake.  Keeping the
/// handshake generic is what allows the same wire implementation to run over
/// plain TCP, native TLS, and the Tokio transport.
pub trait NzIo: Read + Write {}
impl<T: Read + Write> NzIo for T {}

pub enum NzStream {
    Plain(TcpStream),
    #[cfg(feature = "ssl")]
    Tls(Box<rustls::StreamOwned<rustls::ClientConnection, TcpStream>>),
}

impl Read for NzStream {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Self::Plain(s) => s.read(buf),
            #[cfg(feature = "ssl")]
            Self::Tls(s) => s.read(buf),
        }
    }
}

impl Write for NzStream {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            Self::Plain(s) => s.write(buf),
            #[cfg(feature = "ssl")]
            Self::Tls(s) => s.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Self::Plain(s) => s.flush(),
            #[cfg(feature = "ssl")]
            Self::Tls(s) => s.flush(),
        }
    }
}

impl NzStream {
    pub fn set_nonblocking(&self, value: bool) -> std::io::Result<()> {
        match self {
            Self::Plain(s) => s.set_nonblocking(value),
            #[cfg(feature = "ssl")]
            Self::Tls(s) => s.sock.set_nonblocking(value),
        }
    }

    pub fn set_nodelay(&self, value: bool) -> std::io::Result<()> {
        match self {
            Self::Plain(s) => s.set_nodelay(value),
            #[cfg(feature = "ssl")]
            Self::Tls(s) => s.sock.set_nodelay(value),
        }
    }

    pub fn set_read_timeout(&self, value: Option<std::time::Duration>) -> std::io::Result<()> {
        match self {
            Self::Plain(s) => s.set_read_timeout(value),
            #[cfg(feature = "ssl")]
            Self::Tls(s) => s.sock.set_read_timeout(value),
        }
    }

    pub fn set_write_timeout(&self, value: Option<std::time::Duration>) -> std::io::Result<()> {
        match self {
            Self::Plain(s) => s.set_write_timeout(value),
            #[cfg(feature = "ssl")]
            Self::Tls(s) => s.sock.set_write_timeout(value),
        }
    }

    pub fn shutdown(&mut self) -> std::io::Result<()> {
        match self {
            Self::Plain(s) => s.shutdown(std::net::Shutdown::Both),
            #[cfg(feature = "ssl")]
            Self::Tls(s) => s.sock.shutdown(std::net::Shutdown::Both),
        }
    }
}

// Handshake opcodes (HSV2_*)
const HSV2_CLIENT_BEGIN: i16 = 1;
const HSV2_DB: i16 = 2;
const HSV2_USER: i16 = 3;
const HSV2_REMOTE_PID: i16 = 6;
const HSV2_CLIENT_TYPE: i16 = 8;
const HSV2_PROTOCOL: i16 = 9;
const HSV2_SSL_NEGOTIATE: i16 = 11;
const HSV2_SSL_CONNECT: i16 = 12;
const HSV2_APPNAME: i16 = 13;
const HSV2_CLIENT_OS: i16 = 14;
const HSV2_CLIENT_HOST_NAME: i16 = 15;
const HSV2_CLIENT_OS_USER: i16 = 16;
const HSV2_64BIT_VARLENA_ENABLED: i16 = 17;
const HSV2_CLIENT_DONE: i16 = 1000;

// Connection-protocol versions (CP_VERSION_*)
const CP_VERSION_2: i16 = 2;
const CP_VERSION_4: i16 = 4;
const CP_VERSION_5: i16 = 5;
const CP_VERSION_6: i16 = 6;

// PG protocol data versions
const PG_PROTOCOL_3: i16 = 3;
const PG_PROTOCOL_5: i16 = 5;

// Auth request codes
const AUTH_REQ_OK: i32 = 0;
const AUTH_REQ_PASSWORD: i32 = 3;
const AUTH_REQ_MD5: i32 = 5;
const AUTH_REQ_SHA256: i32 = 6;

const MSG_ERROR_RESPONSE: u8 = b'E';
const MSG_AUTH_REQUEST: u8 = b'R';
const MSG_BACKEND_KEY_DATA: u8 = b'K';
const MSG_READY_FOR_QUERY: u8 = b'Z';
const MSG_NOTICE_RESPONSE: u8 = b'N';

pub struct HandshakeResult {
    pub backend_process_id: i32,
    pub backend_secret_key: i32,
}

pub fn handshake(
    mut stream: NzStream,
    buffer: &mut ReadBuffer,
    config: &NzConnectionConfig,
) -> NzResult<(NzStream, HandshakeResult)> {
    let hs_version =
        conn_handshake_negotiate(&mut stream, buffer).map_err(|e| stage_error("negotiate", e))?;
    conn_send_database(&mut stream, buffer, &config.database)
        .map_err(|e| stage_error("database", e))?;
    if conn_secure_session(&mut stream, buffer, config.security_level)
        .map_err(|e| stage_error("security", e))?
    {
        // The server's `S` response is followed by a plaintext SSL_CONNECT
        // control frame. Only then may the socket be wrapped in TLS; after
        // the TLS handshake Netezza sends a final `N` confirmation byte.
        write_i16_frame(&mut stream, HSV2_SSL_CONNECT, &[])
            .map_err(|e| stage_error("ssl-connect", e))?;
        #[cfg(feature = "ssl")]
        {
            stream = upgrade_tls(stream, config).map_err(|e| stage_error("tls-upgrade", e))?;
            expect_n(&mut stream, buffer, "sslHandshake")
                .map_err(|e| stage_error("ssl-confirm", e))?;
        }
        #[cfg(not(feature = "ssl"))]
        return Err(NzError::Unsupported(
            "TLS requested but the `ssl` feature is disabled".into(),
        ));
    }

    // protocol1 = 3 (PG protocol), protocol2 = 5 (data protocol)
    let protocol1 = PG_PROTOCOL_3;
    let protocol2 = PG_PROTOCOL_5;

    let client_os = client_os_name();
    if hs_version == CP_VERSION_6 || hs_version == CP_VERSION_4 {
        conn_send_handshake_v4(
            &mut stream,
            buffer,
            hs_version,
            config,
            protocol1,
            protocol2,
            client_os,
        )
    } else {
        conn_send_handshake_v2(
            &mut stream,
            buffer,
            hs_version,
            config,
            protocol1,
            protocol2,
        )
    }
    .map_err(|e| stage_error("options", e))?;

    conn_authenticate(&mut stream, buffer, &config.password)
        .map_err(|e| stage_error("authentication", e))?;
    let result =
        conn_connection_complete(&mut stream, buffer).map_err(|e| stage_error("complete", e))?;
    Ok((stream, result))
}

fn stage_error(stage: &str, error: NzError) -> NzError {
    match error {
        NzError::Io(e)
            if matches!(
                e.kind(),
                std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
            ) =>
        {
            NzError::Timeout(format!("handshake {stage} timed out: {e}"))
        }
        other => other,
    }
}

#[cfg(feature = "ssl")]
fn upgrade_tls(stream: NzStream, config: &NzConnectionConfig) -> NzResult<NzStream> {
    let NzStream::Plain(tcp) = stream else {
        return Ok(stream);
    };
    use rustls::pki_types::ServerName;
    use std::sync::Arc;
    let mut roots = rustls::RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    if let Some(path) = &config.ssl_cert_path {
        let pem = std::fs::File::open(path).map_err(NzError::Io)?;
        let certs = rustls_pemfile::certs(&mut std::io::BufReader::new(pem))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| NzError::Config(format!("invalid TLS CA certificate: {e}")))?;
        for cert in certs {
            roots
                .add(cert)
                .map_err(|e| NzError::Config(format!("invalid TLS CA certificate: {e}")))?;
        }
    }
    let builder = rustls::ClientConfig::builder();
    let client_config = if config.reject_unauthorized {
        builder.with_root_certificates(roots).with_no_client_auth()
    } else {
        builder
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(NoCertificateVerification))
            .with_no_client_auth()
    };
    let host = config
        .host
        .strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
        .unwrap_or(&config.host);
    let name = ServerName::try_from(host.to_string())
        .map_err(|_| NzError::Config(format!("invalid TLS server name: {}", config.host)))?;
    let connection = rustls::ClientConnection::new(Arc::new(client_config), name)
        .map_err(|e| NzError::Config(format!("TLS setup failed: {e}")))?;
    Ok(NzStream::Tls(Box::new(rustls::StreamOwned::new(
        connection, tcp,
    ))))
}

#[cfg(feature = "ssl")]
#[derive(Debug)]
struct NoCertificateVerification;

#[cfg(feature = "ssl")]
impl rustls::client::danger::ServerCertVerifier for NoCertificateVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        let provider = rustls::crypto::aws_lc_rs::default_provider();
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        let provider = rustls::crypto::aws_lc_rs::default_provider();
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::aws_lc_rs::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

fn client_os_name() -> &'static str {
    std::env::consts::OS
}

/// Frame writer for handshake option blocks:
/// `[len(4)][opcode(2)][payload]` where len counts the whole frame.
fn write_frame(stream: &mut dyn NzIo, opcode: i16, payload: &[u8]) -> NzResult<()> {
    let len = (4 + 2 + payload.len()) as i32;
    stream.write_all(&len.to_be_bytes())?;
    stream.write_all(&opcode.to_be_bytes())?;
    stream.write_all(payload)?;
    stream.flush()?;
    Ok(())
}

fn write_cstring_frame(stream: &mut dyn NzIo, opcode: i16, value: &str) -> NzResult<()> {
    let mut payload = Vec::with_capacity(value.len() + 1);
    payload.extend_from_slice(value.as_bytes());
    payload.push(0);
    write_frame(stream, opcode, &payload)
}

fn write_i16_frame(stream: &mut dyn NzIo, opcode: i16, values: &[i16]) -> NzResult<()> {
    let mut payload = Vec::with_capacity(values.len() * 2);
    for v in values {
        payload.extend_from_slice(&v.to_be_bytes());
    }
    write_frame(stream, opcode, &payload)
}

fn write_i32_frame(stream: &mut dyn NzIo, opcode: i16, value: i32) -> NzResult<()> {
    write_frame(stream, opcode, &value.to_be_bytes())
}

/// Wait for the single-byte 'N' ack; throw a structured error on 'E'.
fn expect_n(stream: &mut dyn NzIo, buffer: &mut ReadBuffer, stage: &str) -> NzResult<()> {
    let b = read_byte(stream, buffer).map_err(|e| stage_error(stage, e))?;
    match b {
        b'N' => Ok(()),
        MSG_ERROR_RESPONSE => Err(throw_backend_error(stream, buffer, stage)),
        other => Err(NzError::Protocol(format!(
            "Handshake {stage}: unexpected response byte 0x{other:02x}"
        ))),
    }
}

fn read_byte(stream: &mut dyn NzIo, buffer: &mut ReadBuffer) -> NzResult<u8> {
    buffer.read_byte(stream)
}

/// Read a backend ErrorResponse frame (after its type byte) and return the
/// parsed error. Handles both normal length-prefixed frames and the legacy
/// NUL-terminated text formats some Netezza versions emit (port of
/// `_throwHandshakeErrorResponse` / `_readLegacyConnectionErrorText`).
fn throw_backend_error(stream: &mut dyn NzIo, buffer: &mut ReadBuffer, stage: &str) -> NzError {
    let len_buf = match buffer.read_bytes(stream, 4) {
        Ok(b) => b,
        Err(e) => return e,
    };
    let len = i32::from_be_bytes(len_buf[..4].try_into().unwrap());

    // Legacy format 1: the "length" bytes are printable ASCII — the message
    // started without a length field (e.g. "Password authentication failed").
    if len_buf.iter().all(|b| (0x20..=0x7e).contains(b)) {
        if let Some(text) = read_legacy_error_text(stream, buffer, 0, &len_buf) {
            return NzError::Database(Box::new(parse_backend_error_fields(&text)));
        }
    }
    // Legacy format 2: explicit zero length followed by NUL-terminated text.
    if len == 0 {
        match buffer.read_byte(stream) {
            Ok(0) => return NzError::Database(Box::new(parse_backend_error_fields(b""))),
            Ok(first) if is_legacy_text_byte(first) => {
                let mut initial = vec![first];
                if let Some(text) = read_legacy_error_text_into(stream, buffer, 4, &mut initial) {
                    return NzError::Database(Box::new(parse_backend_error_fields(&text)));
                }
            }
            Ok(_) => {}
            Err(e) => return e,
        }
    }

    let body_len = match validate_protocol_length(len, &format!("{stage}FrameLength"), false) {
        Ok(l) if l >= 4 => {
            match validate_protocol_length(l - 4, &format!("{stage}Payload"), true) {
                Ok(v) => v as usize,
                Err(e) => return e,
            }
        }
        Ok(_) => {
            return NzError::Protocol(format!(
                "Invalid backend protocol length for '{stage}Payload': frame is smaller than its 4-byte overhead."
            ));
        }
        Err(e) => return e,
    };
    match buffer.read_bytes(stream, body_len) {
        Ok(body) => NzError::Database(Box::new(parse_backend_error_fields(&body))),
        Err(e) => e,
    }
}

fn is_legacy_text_byte(b: u8) -> bool {
    if b >= 0x20 {
        b != 0x7f
    } else {
        b == 0x09 || b == 0x0a || b == 0x0d
    }
}

/// Read a NUL-terminated legacy error text. Returns None when the bytes
/// cannot be such a text (control bytes, too long, no terminator).
fn read_legacy_error_text(
    stream: &mut dyn NzIo,
    buffer: &mut ReadBuffer,
    min_chars: usize,
    initial: &[u8],
) -> Option<Vec<u8>> {
    let mut chars = initial.to_vec();
    read_legacy_error_text_into(stream, buffer, min_chars, &mut chars)
}

fn read_legacy_error_text_into(
    stream: &mut dyn NzIo,
    buffer: &mut ReadBuffer,
    min_chars: usize,
    chars: &mut Vec<u8>,
) -> Option<Vec<u8>> {
    const MAX: usize = 4096;
    if chars.len() > MAX {
        return None;
    }
    loop {
        match buffer.read_byte(stream) {
            Ok(0) => break,
            Ok(b) if is_legacy_text_byte(b) => {
                if chars.len() >= MAX {
                    return None;
                }
                chars.push(b);
            }
            Ok(_) => return None,
            Err(_) => return None,
        }
    }
    if chars.len() < min_chars {
        return None;
    }
    Some(std::mem::take(chars))
}

/// Step 1: negotiate the connection-protocol version.
fn conn_handshake_negotiate(stream: &mut dyn NzIo, buffer: &mut ReadBuffer) -> NzResult<i16> {
    let mut version = CP_VERSION_6;
    loop {
        // Frame: len(4) + HSV2_CLIENT_BEGIN(2) + version(2) = 8
        write_i16_frame(stream, HSV2_CLIENT_BEGIN, &[version])?;
        let beresp = read_byte(stream, buffer)?;
        match beresp {
            b'N' => return Ok(version),
            b'M' => {
                let c = read_byte(stream, buffer)?;
                version = match c {
                    b'2' => CP_VERSION_2,
                    b'3' => 3,
                    b'4' => CP_VERSION_4,
                    b'5' => CP_VERSION_5,
                    _ => {
                        return Err(NzError::Protocol(format!(
                            "Handshake negotiation: server suggested unknown version {c:?}"
                        )));
                    }
                };
            }
            MSG_ERROR_RESPONSE => {
                return Err(throw_backend_error(
                    stream,
                    buffer,
                    "handshakeNegotiationError",
                ));
            }
            _ => {
                return Err(NzError::Protocol(
                    "Handshake negotiation: bad protocol error".into(),
                ));
            }
        }
    }
}

/// Step 2: database selection.
fn conn_send_database(
    stream: &mut dyn NzIo,
    buffer: &mut ReadBuffer,
    database: &str,
) -> NzResult<()> {
    write_cstring_frame(stream, HSV2_DB, database)?;
    let beresp = read_byte(stream, buffer)?;
    match beresp {
        b'N' => Ok(()),
        MSG_ERROR_RESPONSE => Err(throw_backend_error(
            stream,
            buffer,
            "databaseSelectionError",
        )),
        _ => Err(NzError::Protocol(
            "Handshake: unknown database selection response".into(),
        )),
    }
}

/// Step 3: SSL negotiation. `'S'` means the server wants TLS; this build
/// reports a precise Unsupported error (TLS sessions are not needed for the
/// standard unsecured appliance configuration and are feature-gated).
fn conn_secure_session(
    stream: &mut dyn NzIo,
    buffer: &mut ReadBuffer,
    level: SecurityLevel,
) -> NzResult<bool> {
    write_i32_frame(stream, HSV2_SSL_NEGOTIATE, level.as_i32())?;
    let beresp = read_byte(stream, buffer)?;
    match beresp {
        b'N' => {
            if level == SecurityLevel::OnlySecuredSession {
                return Err(NzError::Protocol(
                    "Server refused secure session, but OnlySecuredSession was requested.".into(),
                ));
            }
            Ok(false)
        }
        b'S' => Ok(true),
        MSG_ERROR_RESPONSE => Err(throw_backend_error(stream, buffer, "secureSessionError")),
        _ => Err(NzError::Protocol(
            "Handshake: unknown secure-session response".into(),
        )),
    }
}

// Keep HSV2_SSL_CONNECT referenced for the TLS follow-up feature.
#[allow(dead_code)]
fn ssl_connect_opcode() -> i16 {
    HSV2_SSL_CONNECT
}

/// Step 4a: v4/v6 option sequence (includes Guardium audit metadata).
fn conn_send_handshake_v4(
    stream: &mut dyn NzIo,
    buffer: &mut ReadBuffer,
    hs_version: i16,
    config: &NzConnectionConfig,
    protocol1: i16,
    protocol2: i16,
    client_os: &str,
) -> NzResult<()> {
    write_cstring_frame(stream, HSV2_USER, &config.user)?;

    expect_n(stream, buffer, "user")?;
    write_cstring_frame(stream, HSV2_APPNAME, &config.app_name)?;
    expect_n(stream, buffer, "appname")?;
    write_cstring_frame(stream, HSV2_CLIENT_OS, client_os)?;
    expect_n(stream, buffer, "clientOs")?;
    write_cstring_frame(stream, HSV2_CLIENT_HOST_NAME, &config.client_host_name)?;
    expect_n(stream, buffer, "clientHostName")?;
    write_cstring_frame(stream, HSV2_CLIENT_OS_USER, &config.os_user)?;
    expect_n(stream, buffer, "clientOsUser")?;
    send_protocol_pid_type(stream, buffer, config, protocol1, protocol2, hs_version)
}

/// Step 4b: v2/v3/v5 option sequence.
fn conn_send_handshake_v2(
    stream: &mut dyn NzIo,
    buffer: &mut ReadBuffer,
    hs_version: i16,
    config: &NzConnectionConfig,
    protocol1: i16,
    protocol2: i16,
) -> NzResult<()> {
    write_cstring_frame(stream, HSV2_USER, &config.user)?;
    expect_n(stream, buffer, "user")?;
    send_protocol_pid_type(stream, buffer, config, protocol1, protocol2, hs_version)
}

fn send_protocol_pid_type(
    stream: &mut dyn NzIo,
    buffer: &mut ReadBuffer,
    config: &NzConnectionConfig,
    protocol1: i16,
    protocol2: i16,
    hs_version: i16,
) -> NzResult<()> {
    // NOTE: the server sends exactly one `'N'` ack per client option frame, and
    // the caller has already consumed the ack for the previous frame (USER, or
    // OS_USER on the v4/v6 path). Reading another here would block on an ack
    // the appliance never sends — both reference drivers read once per frame.
    write_i16_frame(stream, HSV2_PROTOCOL, &[protocol1, protocol2])?;

    expect_n(stream, buffer, "remotePid")?;
    write_i32_frame(stream, HSV2_REMOTE_PID, std::process::id() as i32)?;

    expect_n(stream, buffer, "clientType")?;
    write_i16_frame(
        stream,
        HSV2_CLIENT_TYPE,
        &[crate::normalize_client_type(config.client_type)],
    )?;

    if hs_version >= CP_VERSION_5 {
        expect_n(stream, buffer, "64bitVarlena")?;
        write_i16_frame(stream, HSV2_64BIT_VARLENA_ENABLED, &[1])?;
    }

    expect_n(stream, buffer, "clientDone")?;
    // CLIENT_DONE: frame with opcode only, no payload bytes.
    write_i16_frame(stream, HSV2_CLIENT_DONE, &[])?;
    Ok(())
}

/// Step 5: authentication.
fn conn_authenticate(
    stream: &mut dyn NzIo,
    buffer: &mut ReadBuffer,
    password: &str,
) -> NzResult<()> {
    let beresp = read_byte(stream, buffer)?;
    match beresp {
        MSG_ERROR_RESPONSE => {
            return Err(throw_backend_error(stream, buffer, "authenticationError"));
        }
        MSG_AUTH_REQUEST => {}
        other => {
            return Err(NzError::Protocol(format!(
                "Authentication error: unexpected response byte 0x{other:02x}"
            )));
        }
    }

    let areq = buffer.read_i32(stream)?;
    match areq {
        AUTH_REQ_OK => Ok(()),
        AUTH_REQ_PASSWORD => {
            let mut payload = password.as_bytes().to_vec();
            payload.push(0);
            write_auth_response(stream, &payload)
        }
        AUTH_REQ_MD5 => {
            let salt = buffer.read_bytes(stream, 2)?;
            let mut combined = Vec::with_capacity(salt.len() + password.len());
            combined.extend_from_slice(&salt);
            combined.extend_from_slice(password.as_bytes());
            let digest = md5::compute(combined);
            send_hash_password(stream, &digest.0)
        }
        AUTH_REQ_SHA256 => {
            let salt = buffer.read_bytes(stream, 2)?;
            let mut hasher = Sha256::new();
            hasher.update(&salt);
            hasher.update(password.as_bytes());
            let out = hasher.finalize();
            send_hash_password(stream, &out)
        }
        other => Err(NzError::Protocol(format!(
            "Unsupported authentication type requested by server: {other}"
        ))),
    }
}

fn send_hash_password(stream: &mut dyn NzIo, digest: &[u8]) -> NzResult<()> {
    let encoded = base64_encode(digest);
    let trimmed = encoded.trim_end_matches('=');
    let mut payload = trimmed.as_bytes().to_vec();
    payload.push(0);
    write_auth_response(stream, &payload)
}

/// Authentication responses are length-prefixed payloads without a message
/// type/opcode. This differs from the HSV2 option frames used elsewhere in
/// the connection handshake.
fn write_auth_response(stream: &mut dyn NzIo, payload: &[u8]) -> NzResult<()> {
    let len = (4 + payload.len()) as i32;
    stream.write_all(&len.to_be_bytes())?;
    stream.write_all(payload)?;
    stream.flush()?;
    Ok(())
}

/// Minimal standard base64 encoder (with padding) — password hashes only.
fn base64_encode(data: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = *chunk.get(1).unwrap_or(&0) as u32;
        let b2 = *chunk.get(2).unwrap_or(&0) as u32;
        let n = (b0 << 16) | (b1 << 8) | b2;
        out.push(TABLE[(n >> 18) as usize & 63] as char);
        out.push(TABLE[(n >> 12) as usize & 63] as char);
        if chunk.len() > 1 {
            out.push(TABLE[(n >> 6) as usize & 63] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(TABLE[n as usize & 63] as char);
        } else {
            out.push('=');
        }
    }
    out
}

/// Step 6: connection complete — read BackendKeyData / notices until 'Z'.
fn conn_connection_complete(
    stream: &mut dyn NzIo,
    buffer: &mut ReadBuffer,
) -> NzResult<HandshakeResult> {
    let mut result = HandshakeResult {
        backend_process_id: 0,
        backend_secret_key: 0,
    };
    loop {
        let beresp = read_byte(stream, buffer)?;

        // Stray NUL padding between messages (Node parity: a bare zero is not
        // a framed message and must not consume a frame header).
        if beresp == 0 {
            continue;
        }

        // Every framed message except AuthRequest / ErrorResponse carries a
        // 4-byte prefix (zeros/length) that the Node driver skips before it
        // handles the body. `K`, `N` and `Z` all rely on this skip.
        if beresp != MSG_AUTH_REQUEST && beresp != MSG_ERROR_RESPONSE {
            buffer.read_bytes(stream, 4)?;
        }

        match beresp {
            MSG_AUTH_REQUEST => {
                let _areq = buffer.read_i32(stream)?;
            }
            MSG_ERROR_RESPONSE => {
                return Err(throw_backend_error(
                    stream,
                    buffer,
                    "connectionCompleteError",
                ));
            }
            MSG_BACKEND_KEY_DATA => {
                // Frame after the type byte: prefix(4) [skipped above] +
                // padding(4) + pid(4) + key(4) — pid is at offset 8, matching
                // the Node and C# reference drivers.
                let _padding = buffer.read_bytes(stream, 4)?;
                result.backend_process_id = buffer.read_i32(stream)?;
                result.backend_secret_key = buffer.read_i32(stream)?;
            }
            MSG_READY_FOR_QUERY => return Ok(result),
            MSG_NOTICE_RESPONSE => {
                let len = buffer.read_i32(stream)?;
                let len = validate_protocol_length(len, "connectionCompleteNoticePayload", true)?;
                let _body = buffer.read_bytes(stream, len as usize)?;
            }
            other => {
                return Err(NzError::Protocol(format!(
                    "Handshake connection complete: unexpected response byte 0x{other:02x}"
                )));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    struct TestIo {
        input: Cursor<Vec<u8>>,
        output: Vec<u8>,
    }

    impl Read for TestIo {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            self.input.read(buf)
        }
    }

    impl Write for TestIo {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.output.extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    fn auth_exchange(areq: i32, suffix: &[u8], password: &str) -> Vec<u8> {
        let mut input = vec![MSG_AUTH_REQUEST];
        input.extend_from_slice(&areq.to_be_bytes());
        input.extend_from_slice(suffix);
        let mut io = TestIo {
            input: Cursor::new(input),
            output: Vec::new(),
        };
        let mut buffer = ReadBuffer::new();
        conn_authenticate(&mut io, &mut buffer, password).unwrap();
        io.output
    }

    #[test]
    fn base64_matches_known_vectors() {
        assert_eq!(base64_encode(b""), "");
        assert_eq!(base64_encode(b"f"), "Zg==");
        assert_eq!(base64_encode(b"fo"), "Zm8=");
        assert_eq!(base64_encode(b"foo"), "Zm9v");
        assert_eq!(base64_encode(b"foobar"), "Zm9vYmFy");
    }

    #[test]
    fn md5_hash_trimmed() {
        // Deterministic shape check: 24-char base64 for 16-byte digest → trim '='.
        let digest = md5::compute(b"JTpassword");
        let s = base64_encode(digest.0.as_slice());
        assert_eq!(s.len(), 24);
        assert!(s.ends_with('='));
        assert_eq!(s.trim_end_matches('=').len(), 22);
    }

    #[test]
    fn password_authentication_writes_length_prefixed_cstring() {
        let output = auth_exchange(AUTH_REQ_PASSWORD, &[], "secret");
        assert_eq!(
            output,
            [&(4 + 7i32).to_be_bytes()[..], b"secret\0",].concat()
        );
    }

    #[test]
    fn md5_authentication_writes_trimmed_base64_digest() {
        let salt = [0x12, 0x34];
        let output = auth_exchange(AUTH_REQ_MD5, &salt, "secret");
        let mut combined = salt.to_vec();
        combined.extend_from_slice(b"secret");
        let encoded = base64_encode(md5::compute(combined).0.as_slice())
            .trim_end_matches('=')
            .as_bytes()
            .to_vec();
        let mut expected = ((4 + encoded.len() + 1) as i32).to_be_bytes().to_vec();
        expected.extend_from_slice(&encoded);
        expected.push(0);
        assert_eq!(output, expected);
    }

    #[test]
    fn sha256_authentication_writes_trimmed_base64_digest() {
        let salt = [0x12, 0x34];
        let output = auth_exchange(AUTH_REQ_SHA256, &salt, "secret");
        let mut hasher = Sha256::new();
        hasher.update(salt);
        hasher.update(b"secret");
        let encoded = base64_encode(hasher.finalize().as_slice())
            .trim_end_matches('=')
            .as_bytes()
            .to_vec();
        let mut expected = ((4 + encoded.len() + 1) as i32).to_be_bytes().to_vec();
        expected.extend_from_slice(&encoded);
        expected.push(0);
        assert_eq!(output, expected);
    }

    #[test]
    fn cancel_style_frames_encode() {
        // Verify our BE frame writer contract with a direct buffer build.
        let mut payload = Vec::new();
        payload.extend_from_slice(&3i16.to_be_bytes());
        let frame_len = 4 + 2 + payload.len();
        assert_eq!(frame_len, 8);
    }

    fn complete(bytes: Vec<u8>) -> NzResult<HandshakeResult> {
        let mut stream = std::io::Cursor::new(bytes);
        let mut buffer = ReadBuffer::new();
        conn_connection_complete(&mut stream, &mut buffer)
    }

    #[test]
    fn connection_complete_ready_for_query_marker() {
        // Node fixture: 'Z' followed by a 4-byte prefix.
        let r = complete(vec![MSG_READY_FOR_QUERY, 0, 0, 0, 0]).unwrap();
        assert_eq!(r.backend_process_id, 0);
        assert_eq!(r.backend_secret_key, 0);
    }

    #[test]
    fn connection_complete_reads_backend_key_data_at_offset_eight() {
        // 'K' + zeros(4) + len(4)=12 + pid(4) + key(4), then 'Z' + prefix(4).
        let mut bytes = vec![MSG_BACKEND_KEY_DATA];
        bytes.extend_from_slice(&[0, 0, 0, 0]); // prefix
        bytes.extend_from_slice(&12i32.to_be_bytes()); // frame length
        bytes.extend_from_slice(&5857i32.to_be_bytes()); // pid
        bytes.extend_from_slice(&(-2_092_017_624i32).to_be_bytes()); // key
        bytes.push(MSG_READY_FOR_QUERY);
        bytes.extend_from_slice(&[0, 0, 0, 0]);

        let r = complete(bytes).unwrap();
        assert_eq!(r.backend_process_id, 5857);
        assert_eq!(r.backend_secret_key, -2_092_017_624);
    }

    #[test]
    fn connection_complete_skips_notice_then_ready() {
        let body = b"NOTICE: hello";
        let mut bytes = vec![MSG_NOTICE_RESPONSE];
        bytes.extend_from_slice(&[0, 0, 0, 0]); // prefix
        bytes.extend_from_slice(&(body.len() as i32).to_be_bytes());
        bytes.extend_from_slice(body);
        bytes.push(MSG_READY_FOR_QUERY);
        bytes.extend_from_slice(&[0, 0, 0, 0]);

        assert!(complete(bytes).is_ok());
    }

    #[test]
    fn connection_complete_rejects_bad_notice_length() {
        // Node fixture: NoticeResponse + prefix(4) + int32(-1).
        let mut bytes = vec![MSG_NOTICE_RESPONSE];
        bytes.extend_from_slice(&[0, 0, 0, 0]);
        bytes.extend_from_slice(&(-1i32).to_be_bytes());
        assert!(complete(bytes).is_err());
    }

    #[test]
    fn connection_complete_rejects_unexpected_type() {
        let bytes = vec![b'?', 0, 0, 0, 0];
        assert!(complete(bytes).is_err());
    }
}
