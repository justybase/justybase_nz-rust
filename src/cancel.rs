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

//! Out-of-band query cancellation — port of the Node driver `cancel()`.
//!
//! Opens a fresh connection to the appliance and sends the PostgreSQL-style
//! 16-byte cancel request (magic 80877102 = 1234·65536 + 5678) carrying the
//! backend PID and secret key received during the handshake.

use crate::config::NzConnectionConfig;
use crate::error::{NzError, NzResult};
use std::io::Write;
use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;

/// Build the 16-byte cancel request packet.
pub fn build_cancel_packet(backend_process_id: i32, backend_secret_key: i32) -> [u8; 16] {
    let mut buf = [0u8; 16];
    buf[0..4].copy_from_slice(&16i32.to_be_bytes());
    buf[4..8].copy_from_slice(&80877102i32.to_be_bytes());
    buf[8..12].copy_from_slice(&backend_process_id.to_be_bytes());
    buf[12..16].copy_from_slice(&backend_secret_key.to_be_bytes());
    buf
}

/// Send a cancel request for the given backend key data. Fire-and-forget
/// semantics: success is reported when the packet was delivered.
pub fn send_cancel(
    config: &NzConnectionConfig,
    backend_process_id: i32,
    backend_secret_key: i32,
) -> NzResult<()> {
    if backend_process_id == 0 || backend_secret_key == 0 {
        return Ok(()); // nothing to cancel
    }
    let timeout = Duration::from_secs(config.connection_timeout.max(1));
    let addr_str = if config.host.contains(':') && !config.host.starts_with('[') {
        format!("[{}]:{}", config.host, config.port)
    } else {
        format!("{}:{}", config.host, config.port)
    };
    let mut last_err: Option<std::io::Error> = None;
    let mut connected: Option<TcpStream> = None;
    for addr in addr_str.to_socket_addrs().map_err(NzError::Io)? {
        match TcpStream::connect_timeout(&addr, timeout) {
            Ok(s) => {
                connected = Some(s);
                break;
            }
            Err(e) => last_err = Some(e),
        }
    }
    let mut stream = connected.ok_or_else(|| {
        last_err
            .map(NzError::Io)
            .unwrap_or_else(|| NzError::Closed(format!("cannot resolve {}", addr_str)))
    })?;
    stream.set_nodelay(true).ok();
    stream.set_write_timeout(Some(timeout)).ok();
    stream.set_read_timeout(Some(timeout)).ok();

    stream.write_all(&build_cancel_packet(backend_process_id, backend_secret_key))?;
    stream.flush().ok();
    // The server closes the socket after processing; a clean EOF or immediate
    // close are both fine outcomes (mirrors the Node driver's resolve-on-any).
    drop(stream);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn packet_layout() {
        let packet = build_cancel_packet(5857, -2092017624);
        assert_eq!(&packet[0..4], &16i32.to_be_bytes());
        assert_eq!(&packet[4..8], &80877102i32.to_be_bytes());
        assert_eq!(&packet[8..12], &5857i32.to_be_bytes());
        assert_eq!(&packet[12..16], &(-2092017624i32).to_be_bytes());
    }
}
