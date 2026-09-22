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

//! Growable stream read buffer — port of the Node driver's internal
//! `_intBuf` / `_ensureBufferCapacity` / `_ensureBufferData` machinery.
//!
//! Keeps unconsumed bytes compacted at the front; grows geometrically so a
//! lagging consumer can never corrupt the stream alignment.

use crate::error::{validate_protocol_length, NzError, NzResult, MAX_PROTOCOL_PAYLOAD};
use std::io::Read;

pub struct ReadBuffer {
    buf: Vec<u8>,
    start: usize,
    end: usize,
}

impl Default for ReadBuffer {
    fn default() -> Self {
        Self::new()
    }
}

impl ReadBuffer {
    pub fn new() -> Self {
        ReadBuffer {
            buf: vec![0u8; 65_536],
            start: 0,
            end: 0,
        }
    }

    #[inline]
    pub fn available(&self) -> usize {
        self.end - self.start
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.available() == 0
    }

    pub fn clear(&mut self) {
        self.start = 0;
        self.end = 0;
    }

    /// Discard leading NUL bytes (Netezza zero padding between messages).
    /// Returns how many were discarded.
    pub fn discard_leading_nulls(&mut self) -> usize {
        let mut n = 0;
        while self.start < self.end && self.buf[self.start] == 0 {
            self.start += 1;
            n += 1;
        }
        n
    }

    fn ensure_capacity(&mut self, needed: usize) {
        let remaining = self.end - self.start;
        if self.buf.len() - self.end >= needed {
            return;
        }
        if self.buf.len() - remaining >= needed {
            self.buf.copy_within(self.start..self.end, 0);
            self.start = 0;
            self.end = remaining;
            return;
        }
        let new_size = (self.buf.len() * 2).max(remaining + needed).max(65_536);
        let mut new_buf = vec![0u8; new_size];
        new_buf[..remaining].copy_from_slice(&self.buf[self.start..self.end]);
        self.buf = new_buf;
        self.start = 0;
        self.end = remaining;
    }

    /// Pull any data the OS already buffered from `stream` without blocking.
    pub fn pull_available<R: Read + ?Sized>(&mut self, stream: &mut R) -> NzResult<usize> {
        let mut pulled = 0;
        loop {
            self.ensure_capacity(4096);
            match stream.read(&mut self.buf[self.end..]) {
                Ok(0) => break,
                Ok(n) => {
                    self.end += n;
                    pulled += n;
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(e) => return Err(e.into()),
            }
        }
        Ok(pulled)
    }

    /// Ensure at least `n` bytes are buffered, reading from `stream` (blocking).
    pub fn ensure_data<R: Read + ?Sized>(&mut self, stream: &mut R, n: usize) -> NzResult<()> {
        check_len(n, "buffer read")?;
        if self.available() >= n {
            return Ok(());
        }
        if self.buf.len() - self.start < n {
            self.ensure_capacity(self.start + n);
        }
        while self.available() < n {
            match stream.read(&mut self.buf[self.end..]) {
                Ok(0) => {
                    return Err(NzError::Closed("Socket closed/ended during read".into()));
                }
                Ok(chunk) => {
                    self.end += chunk;
                }
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(e.into()),
            }
        }
        Ok(())
    }

    /// Consume `n` bytes (must already be buffered or readable).
    pub fn skip<R: Read + ?Sized>(&mut self, stream: &mut R, n: usize) -> NzResult<()> {
        check_len(n, "buffer skip")?;
        self.ensure_data(stream, n)?;
        self.start += n;
        Ok(())
    }

    pub fn read_byte<R: Read + ?Sized>(&mut self, stream: &mut R) -> NzResult<u8> {
        self.ensure_data(stream, 1)?;
        let b = self.buf[self.start];
        self.start += 1;
        Ok(b)
    }

    pub fn read_i32<R: Read + ?Sized>(&mut self, stream: &mut R) -> NzResult<i32> {
        self.ensure_data(stream, 4)?;
        let v = i32::from_be_bytes(self.buf[self.start..self.start + 4].try_into().unwrap());
        self.start += 4;
        Ok(v)
    }

    /// Read exactly `n` bytes into a fresh Vec (for payload frames).
    pub fn read_bytes<R: Read + ?Sized>(&mut self, stream: &mut R, n: usize) -> NzResult<Vec<u8>> {
        check_len(n, "buffer read")?;
        self.ensure_data(stream, n)?;
        let out = self.buf[self.start..self.start + n].to_vec();
        self.start += n;
        Ok(out)
    }

    /// Read exactly `n` bytes into a reusable destination buffer.
    ///
    /// DBOS rows are decoded one at a time. Reusing the destination avoids a
    /// heap allocation for every row while keeping the buffer ownership and
    /// lifetime rules explicit.
    pub fn read_bytes_into<R: Read + ?Sized>(
        &mut self,
        stream: &mut R,
        n: usize,
        out: &mut Vec<u8>,
    ) -> NzResult<()> {
        check_len(n, "buffer read")?;
        self.ensure_data(stream, n)?;
        out.clear();
        out.extend_from_slice(&self.buf[self.start..self.start + n]);
        self.start += n;
        Ok(())
    }

    /// Borrow `n` buffered bytes without consuming.
    pub fn peek_bytes<R: Read + ?Sized>(&mut self, stream: &mut R, n: usize) -> NzResult<&[u8]> {
        self.ensure_data(stream, n)?;
        Ok(&self.buf[self.start..self.start + n])
    }

    /// Parse a little-endian i64 at an absolute buffer offset (binary rows).
    pub fn read_le_i64_at(&self, offset: usize) -> i64 {
        i64::from_le_bytes(self.buf[offset..offset + 8].try_into().unwrap())
    }

    pub fn slice(&self) -> &[u8] {
        &self.buf[self.start..self.end]
    }

    /// Offset of `start` relative to the underlying allocation (binary parsing).
    pub fn start(&self) -> usize {
        self.start
    }

    pub fn advance(&mut self, n: usize) {
        self.start = (self.start + n).min(self.end);
    }
}

fn check_len(n: usize, field: &str) -> NzResult<()> {
    let v: i32 = n.try_into().map_err(|_| {
        NzError::Protocol(format!(
            "Invalid backend protocol length for '{field}': {n}; maximum supported payload is {} bytes. \
             The connection is no longer safe to reuse; reconnect is required.",
            MAX_PROTOCOL_PAYLOAD
        ))
    })?;
    validate_protocol_length(v, field, true)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn reads_across_chunks() {
        let mut rb = ReadBuffer::new();
        let mut data = Cursor::new(vec![1u8, 2, 3, 4, 5, 6, 7, 8]);
        assert_eq!(rb.read_byte(&mut data).unwrap(), 1);
        assert_eq!(rb.read_i32(&mut data).unwrap(), 0x02030405);
        assert_eq!(rb.read_byte(&mut data).unwrap(), 6);
    }

    #[test]
    fn skip_and_discard_nulls() {
        let mut rb = ReadBuffer::new();
        let mut data = Cursor::new(vec![0u8, 0, 0, 0, 0, 7]);
        assert_eq!(rb.discard_leading_nulls(), 0); // nothing buffered yet
        rb.ensure_data(&mut data, 1).unwrap();
        assert_eq!(rb.discard_leading_nulls(), 5);
        rb.pull_available(&mut data).unwrap();
        assert_eq!(rb.discard_leading_nulls(), 0);
        assert_eq!(rb.read_byte(&mut data).unwrap(), 7);
    }

    #[test]
    fn grows_for_large_payloads() {
        let mut rb = ReadBuffer::new();
        let payload: Vec<u8> = (0..300_000u32).map(|i| i as u8).collect();
        let mut data = Cursor::new(payload.clone());
        let out = rb.read_bytes(&mut data, payload.len()).unwrap();
        assert_eq!(out.len(), payload.len());
        assert_eq!(out[250_000], (250_000 % 256) as u8);
    }

    #[test]
    fn reuses_destination_for_payloads() {
        let mut rb = ReadBuffer::new();
        let mut data = Cursor::new(vec![1u8, 2, 3, 4, 5, 6]);
        let mut out = Vec::with_capacity(16);
        let capacity = out.capacity();
        rb.read_bytes_into(&mut data, 4, &mut out).unwrap();
        assert_eq!(out, [1, 2, 3, 4]);
        rb.read_bytes_into(&mut data, 2, &mut out).unwrap();
        assert_eq!(out, [5, 6]);
        assert_eq!(out.capacity(), capacity);
    }

    #[test]
    fn rejects_negative_or_huge_lengths() {
        let mut rb = ReadBuffer::new();
        let mut data = Cursor::new(vec![0u8; 4]);
        assert!(rb.read_bytes(&mut data, usize::MAX).is_err());
    }
}
