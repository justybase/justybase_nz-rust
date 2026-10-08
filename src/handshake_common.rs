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

//! Pure handshake decisions shared by the synchronous and asynchronous
//! protocol engines. Socket reads and writes stay with each engine.

use crate::error::{NzError, NzResult};
use sha2::{Digest, Sha256};

const HSV2_USER: i16 = 3;
const HSV2_REMOTE_PID: i16 = 6;
const HSV2_CLIENT_TYPE: i16 = 8;
const HSV2_PROTOCOL: i16 = 9;
const HSV2_APPNAME: i16 = 13;
const HSV2_CLIENT_OS: i16 = 14;
const HSV2_CLIENT_HOST_NAME: i16 = 15;
const HSV2_CLIENT_OS_USER: i16 = 16;
const HSV2_64BIT_VARLENA_ENABLED: i16 = 17;
const HSV2_CLIENT_DONE: i16 = 1000;

const AUTH_REQ_OK: i32 = 0;
const AUTH_REQ_PASSWORD: i32 = 3;
const AUTH_REQ_MD5: i32 = 5;
const AUTH_REQ_SHA256: i32 = 6;

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct HandshakeOption {
    pub opcode: i16,
    pub payload: Vec<u8>,
    pub ack_after: Option<&'static str>,
}

pub(crate) struct HandshakeClientInfo<'a> {
    pub user: &'a str,
    pub app_name: &'a str,
    pub client_os: &'a str,
    pub client_host_name: &'a str,
    pub os_user: &'a str,
    pub remote_pid: i32,
    pub client_type: i16,
}

/// Validate a server-requested connection-protocol downgrade.
pub(crate) fn validate_version_downgrade(current: i16, proposed: i16) -> NzResult<()> {
    if proposed >= current {
        return Err(NzError::Protocol(format!(
            "Handshake negotiation: server proposed version {proposed} after {current}"
        )));
    }
    Ok(())
}

/// Build the option frames and acknowledgement stages for a negotiated
/// connection-protocol version. Payload bytes are already in wire order.
pub(crate) fn option_plan(version: i16, client: HandshakeClientInfo<'_>) -> Vec<HandshakeOption> {
    let mut options = Vec::with_capacity(10);
    options.push(cstring_option(HSV2_USER, client.user, "user"));

    if version == 4 || version == 6 {
        options.push(cstring_option(HSV2_APPNAME, client.app_name, "appname"));
        options.push(cstring_option(HSV2_CLIENT_OS, client.client_os, "clientOs"));
        options.push(cstring_option(
            HSV2_CLIENT_HOST_NAME,
            client.client_host_name,
            "clientHostName",
        ));
        options.push(cstring_option(
            HSV2_CLIENT_OS_USER,
            client.os_user,
            "clientOsUser",
        ));
    }

    options.push(i16s_option(HSV2_PROTOCOL, &[3, 5], "remotePid"));
    options.push(i32_option(HSV2_REMOTE_PID, client.remote_pid, "clientType"));
    options.push(i16_option(
        HSV2_CLIENT_TYPE,
        crate::normalize_client_type(client.client_type),
        if version >= 5 {
            "64bitVarlena"
        } else {
            "clientDone"
        },
    ));
    if version >= 5 {
        options.push(i16_option(HSV2_64BIT_VARLENA_ENABLED, 1, "clientDone"));
    }
    options.push(HandshakeOption {
        opcode: HSV2_CLIENT_DONE,
        payload: Vec::new(),
        ack_after: None,
    });
    options
}

/// Number of salt bytes the transport must read before requesting an auth
/// response. Unknown methods are rejected before another socket read.
pub(crate) fn auth_salt_len(request: i32) -> NzResult<usize> {
    match request {
        AUTH_REQ_OK | AUTH_REQ_PASSWORD => Ok(0),
        AUTH_REQ_MD5 | AUTH_REQ_SHA256 => Ok(2),
        other => Err(NzError::Protocol(format!(
            "Unsupported authentication type requested by server: {other}"
        ))),
    }
}

/// Compute the response payload for an authentication request, including its
/// trailing NUL. `None` means the server accepted the credentials directly.
pub(crate) fn auth_response(
    request: i32,
    password: &str,
    salt: &[u8],
) -> NzResult<Option<Vec<u8>>> {
    match request {
        AUTH_REQ_OK => Ok(None),
        AUTH_REQ_PASSWORD => Ok(Some(cstring_payload(password))),
        AUTH_REQ_MD5 | AUTH_REQ_SHA256 => {
            if salt.len() != 2 {
                return Err(NzError::Protocol(format!(
                    "authentication salt must contain 2 bytes, received {}",
                    salt.len()
                )));
            }
            let mut digest = Vec::new();
            if request == AUTH_REQ_MD5 {
                let mut input = Vec::with_capacity(salt.len() + password.len());
                input.extend_from_slice(salt);
                input.extend_from_slice(password.as_bytes());
                digest.extend_from_slice(&md5::compute(input).0);
            } else {
                let mut hasher = Sha256::new();
                hasher.update(salt);
                hasher.update(password.as_bytes());
                digest.extend_from_slice(&hasher.finalize());
            }
            let mut response = base64_encode_unpadded(&digest).into_bytes();
            response.push(0);
            Ok(Some(response))
        }
        other => Err(NzError::Protocol(format!(
            "Unsupported authentication type requested by server: {other}"
        ))),
    }
}

/// Standard Base64 without `=` padding, as required by both Netezza engines.
pub(crate) fn base64_encode_unpadded(data: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = *chunk.get(1).unwrap_or(&0) as u32;
        let b2 = *chunk.get(2).unwrap_or(&0) as u32;
        let n = (b0 << 16) | (b1 << 8) | b2;
        out.push(TABLE[((n >> 18) & 63) as usize] as char);
        out.push(TABLE[((n >> 12) & 63) as usize] as char);
        if chunk.len() > 1 {
            out.push(TABLE[((n >> 6) & 63) as usize] as char);
        }
        if chunk.len() > 2 {
            out.push(TABLE[(n & 63) as usize] as char);
        }
    }
    out
}

fn cstring_option(opcode: i16, value: &str, ack_after: &'static str) -> HandshakeOption {
    HandshakeOption {
        opcode,
        payload: cstring_payload(value),
        ack_after: Some(ack_after),
    }
}

fn cstring_payload(value: &str) -> Vec<u8> {
    let mut payload = Vec::with_capacity(value.len() + 1);
    payload.extend_from_slice(value.as_bytes());
    payload.push(0);
    payload
}

fn i16_option(opcode: i16, value: i16, ack_after: &'static str) -> HandshakeOption {
    HandshakeOption {
        opcode,
        payload: value.to_be_bytes().to_vec(),
        ack_after: Some(ack_after),
    }
}

fn i16s_option(opcode: i16, values: &[i16], ack_after: &'static str) -> HandshakeOption {
    let mut payload = Vec::with_capacity(values.len() * 2);
    for value in values {
        payload.extend_from_slice(&value.to_be_bytes());
    }
    HandshakeOption {
        opcode,
        payload,
        ack_after: Some(ack_after),
    }
}

fn i32_option(opcode: i16, value: i32, ack_after: &'static str) -> HandshakeOption {
    HandshakeOption {
        opcode,
        payload: value.to_be_bytes().to_vec(),
        ack_after: Some(ack_after),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn downgrade_must_be_strictly_decreasing() {
        assert!(validate_version_downgrade(6, 5).is_ok());
        assert!(validate_version_downgrade(5, 4).is_ok());
        assert!(validate_version_downgrade(6, 6).is_err());
        assert!(validate_version_downgrade(4, 5).is_err());
    }

    #[test]
    fn option_plan_matches_each_version_family_and_wire_order() {
        let plan = |version| {
            option_plan(
                version,
                HandshakeClientInfo {
                    user: "user",
                    app_name: "app",
                    client_os: "linux",
                    client_host_name: "host",
                    os_user: "os-user",
                    remote_pid: 0x0102_0304,
                    client_type: 3,
                },
            )
        };
        let opcodes = |version| {
            plan(version)
                .into_iter()
                .map(|frame| frame.opcode)
                .collect::<Vec<_>>()
        };
        assert_eq!(opcodes(2), [3, 9, 6, 8, 1000]);
        assert_eq!(opcodes(3), [3, 9, 6, 8, 1000]);
        assert_eq!(opcodes(4), [3, 13, 14, 15, 16, 9, 6, 8, 1000]);
        assert_eq!(opcodes(5), [3, 9, 6, 8, 17, 1000]);
        assert_eq!(opcodes(6), [3, 13, 14, 15, 16, 9, 6, 8, 17, 1000]);

        let plan = plan(6);
        assert_eq!(plan[0].payload, b"user\0");
        assert_eq!(plan[1].payload, b"app\0");
        assert_eq!(plan[2].payload, b"linux\0");
        assert_eq!(plan[3].payload, b"host\0");
        assert_eq!(plan[4].payload, b"os-user\0");
        assert_eq!(plan[5].payload, [0, 3, 0, 5]);
        assert_eq!(plan[6].payload, 0x0102_0304i32.to_be_bytes());
        assert_eq!(plan[7].payload, 3i16.to_be_bytes());
        assert_eq!(plan[8].payload, 1i16.to_be_bytes());
        assert_eq!(plan[9].payload, []);
        assert_eq!(plan[5].ack_after, Some("remotePid"));
        assert_eq!(plan[7].ack_after, Some("64bitVarlena"));
        assert_eq!(plan[8].ack_after, Some("clientDone"));
        assert_eq!(plan[9].ack_after, None);
    }

    #[test]
    fn authentication_responses_use_unpadded_base64() {
        assert_eq!(base64_encode_unpadded(b""), "");
        assert_eq!(base64_encode_unpadded(b"f"), "Zg");
        assert_eq!(base64_encode_unpadded(b"fo"), "Zm8");
        assert_eq!(base64_encode_unpadded(b"foo"), "Zm9v");
        assert_eq!(base64_encode_unpadded(b"foobar"), "Zm9vYmFy");
        assert_eq!(auth_salt_len(AUTH_REQ_OK).unwrap(), 0);
        assert_eq!(auth_salt_len(AUTH_REQ_PASSWORD).unwrap(), 0);
        assert_eq!(auth_salt_len(AUTH_REQ_MD5).unwrap(), 2);
        assert_eq!(auth_salt_len(AUTH_REQ_SHA256).unwrap(), 2);
        assert!(auth_response(99, "secret", &[]).is_err());
        assert!(auth_response(AUTH_REQ_MD5, "secret", &[1]).is_err());

        assert_eq!(auth_response(AUTH_REQ_OK, "secret", &[]).unwrap(), None);
        assert_eq!(
            auth_response(AUTH_REQ_PASSWORD, "secret", &[]).unwrap(),
            Some(b"secret\0".to_vec())
        );
        let md5 = auth_response(AUTH_REQ_MD5, "secret", &[0x12, 0x34])
            .unwrap()
            .unwrap();
        assert_eq!(md5.len(), 23);
        assert_eq!(md5.last(), Some(&0));
        assert!(!md5.contains(&b'='));
        let sha256 = auth_response(AUTH_REQ_SHA256, "secret", &[0x12, 0x34])
            .unwrap()
            .unwrap();
        assert_eq!(sha256.len(), 44);
        assert_eq!(sha256.last(), Some(&0));
        assert!(!sha256.contains(&b'='));
    }
}
