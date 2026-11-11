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

//! Per-result-set, per-column deduplication of decoded values.
//!
//! A result row often repeats the same short text, date, numeric or interval
//! across thousands of rows. The cache is keyed by the exact wire bytes of a
//! cell, so a hit returns the already decoded [`NzValue`] without decoding or
//! allocating. Values are immutable (`Arc<str>`, `Decimal`, compact temporal
//! types), which makes returning the same value safe.
//!
//! The cache is bounded per column, has a last-value fast path for grouped
//! rows, and disables itself for a column when the first probe finds almost no
//! repetition. It lives exactly as long as one result set's row metadata.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Mutex;

use crate::tuple_desc::ColumnDesc;
use crate::types::value::NzValue;

/// Default maximum number of distinct values cached for one column.
pub const DEFAULT_MAX_ENTRIES: usize = 2_048;
/// Values longer than this are not cached.
pub const DEFAULT_MAX_KEY_BYTES: usize = 64;
/// Number of column lookups before the hit-rate decision is made.
pub const DEFAULT_PROBE_LOOKUPS: usize = 8_192;
/// A column is disabled when fewer than 1/50 lookups (2%) hit.
const MIN_HIT_FRACTION_DENOMINATOR: usize = 50;

/// Aggregate value-cache counters for one result set.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ValueCacheStats {
    /// Cache lookups that returned a stored value.
    pub hits: u64,
    /// Cache lookups that had to decode the cell.
    pub misses: u64,
    /// Values admitted to a column cache.
    pub insertions: u64,
    /// Values rejected because they were too long or the column was full.
    pub rejected: u64,
    /// Number of columns that disabled themselves after the probe.
    pub disabled_columns: usize,
    /// Number of columns with a value cache.
    pub columns: usize,
    /// Values currently retained across all column caches.
    pub entries: usize,
}

#[derive(Debug)]
struct CacheState {
    enabled: AtomicBool,
    lookups: AtomicUsize,
    hits: AtomicUsize,
    probe_limit: usize,
}

impl CacheState {
    fn new(probe_limit: usize) -> Self {
        Self {
            enabled: AtomicBool::new(true),
            lookups: AtomicUsize::new(0),
            hits: AtomicUsize::new(0),
            probe_limit,
        }
    }

    fn enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    fn record(&self, hit: bool) {
        let lookups = self.lookups.fetch_add(1, Ordering::Relaxed) + 1;
        if hit {
            self.hits.fetch_add(1, Ordering::Relaxed);
        }
        if lookups == self.probe_limit {
            let hits = self.hits.load(Ordering::Relaxed);
            if hits * MIN_HIT_FRACTION_DENOMINATOR < lookups {
                self.enabled.store(false, Ordering::Relaxed);
            }
        }
    }
}

#[derive(Debug)]
struct ColumnCache {
    entries: HashMap<Box<[u8]>, NzValue>,
    last_key: Vec<u8>,
    last_value: Option<NzValue>,
    state: CacheState,
    max_entries: usize,
    max_key_bytes: usize,
    insertions: u64,
    rejected: u64,
}

impl ColumnCache {
    fn new(max_entries: usize, max_key_bytes: usize, probe_limit: usize) -> Self {
        Self {
            entries: HashMap::new(),
            last_key: Vec::new(),
            last_value: None,
            state: CacheState::new(probe_limit),
            max_entries,
            max_key_bytes,
            insertions: 0,
            rejected: 0,
        }
    }

    fn get(&mut self, key: &[u8]) -> Option<NzValue> {
        if !self.state.enabled() {
            return None;
        }
        if key.len() > self.max_key_bytes {
            self.state.record(false);
            return None;
        }
        if let Some(value) = &self.last_value {
            if self.last_key == key {
                self.state.record(true);
                return Some(value.clone());
            }
        }
        if let Some(value) = self.entries.get(key) {
            let value = value.clone();
            self.last_key.clear();
            self.last_key.extend_from_slice(key);
            self.last_value = Some(value.clone());
            self.state.record(true);
            return Some(value);
        }
        self.state.record(false);
        None
    }

    fn remember(&mut self, key: &[u8], value: &NzValue) {
        if !self.state.enabled() || key.len() > self.max_key_bytes {
            return;
        }
        self.last_key.clear();
        self.last_key.extend_from_slice(key);
        self.last_value = Some(value.clone());
    }

    fn insert(&mut self, key: &[u8], value: &NzValue) {
        if !self.state.enabled() {
            return;
        }
        if !is_cacheable_value(value) {
            return;
        }
        if key.len() > self.max_key_bytes {
            self.rejected = self.rejected.saturating_add(1);
            return;
        }
        if self.entries.contains_key(key) {
            self.remember(key, value);
            return;
        }
        if self.entries.len() >= self.max_entries {
            self.rejected = self.rejected.saturating_add(1);
            self.remember(key, value);
            return;
        }
        self.entries
            .insert(key.to_vec().into_boxed_slice(), value.clone());
        self.insertions = self.insertions.saturating_add(1);
        self.remember(key, value);
    }
}

/// Per-column caches shared by every row of one result set.
#[derive(Debug)]
pub(crate) struct ValueCaches {
    columns: Vec<Mutex<ColumnCache>>,
    cacheable: Vec<bool>,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl ValueCaches {
    pub(crate) fn new(columns: &[ColumnDesc]) -> Self {
        let cacheable: Vec<bool> = columns
            .iter()
            .map(|column| is_cacheable_type(column.type_oid))
            .collect();
        let columns = cacheable
            .iter()
            .map(|_| {
                Mutex::new(ColumnCache::new(
                    DEFAULT_MAX_ENTRIES,
                    DEFAULT_MAX_KEY_BYTES,
                    DEFAULT_PROBE_LOOKUPS,
                ))
            })
            .collect();
        Self {
            columns,
            cacheable,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    /// Return a cached value or run `decode`, remembering the result.
    pub(crate) fn get_or_decode<F>(
        &self,
        index: usize,
        key: &[u8],
        decode: F,
    ) -> crate::NzResult<NzValue>
    where
        F: FnOnce() -> crate::NzResult<NzValue>,
    {
        let Some(cacheable) = self.cacheable.get(index).copied() else {
            return decode();
        };
        if !cacheable {
            return decode();
        }
        let Some(cache) = self.columns.get(index) else {
            return decode();
        };
        {
            let mut cache = cache
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(value) = cache.get(key) {
                self.hits.fetch_add(1, Ordering::Relaxed);
                return Ok(value);
            }
        }
        self.misses.fetch_add(1, Ordering::Relaxed);
        let value = decode()?;
        let mut cache = cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        cache.insert(key, &value);
        Ok(value)
    }

    /// Snapshot the aggregate counters for this result set.
    pub(crate) fn stats(&self) -> ValueCacheStats {
        let mut disabled_columns = 0;
        let mut entries = 0;
        let mut local_insertions = 0;
        let mut local_rejected = 0;
        for cache in &self.columns {
            let cache = cache
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if !cache.state.enabled() {
                disabled_columns += 1;
            }
            entries += cache.entries.len();
            local_insertions += cache.insertions;
            local_rejected += cache.rejected;
        }
        ValueCacheStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            insertions: local_insertions,
            rejected: local_rejected,
            disabled_columns,
            columns: self.columns.len(),
            entries,
        }
    }
}

/// Values whose clone is cheap and whose decoded form is immutable.
fn is_cacheable_value(value: &NzValue) -> bool {
    matches!(
        value,
        NzValue::Text(_)
            | NzValue::Numeric(_)
            | NzValue::Date(_)
            | NzValue::Time(_)
            | NzValue::Timetz(_)
            | NzValue::Interval(_)
            | NzValue::Decimal(_)
    )
}

/// Column types that can produce an immutable, allocation-worthy value.
///
/// Integers, floats, booleans, raw binary and compact timestamps are excluded:
/// decoding them is cheaper than a cache lookup and they do not allocate.
fn is_cacheable_type(type_oid: i32) -> bool {
    !matches!(
        type_oid,
        16 // BOOL
            | 17 // BYTEA
            | 20 // INT8
            | 21 // INT2
            | 23 // INT4
            | 26 // OID
            | 28 // XID
            | 700 // FLOAT4
            | 701 // FLOAT8
            | 702 // ABSTIME (text path; compact NzTimestamp)
            | 1114 // TIMESTAMP
            | 1184 // TIMESTAMPTZ
            | 2568 // VARBINARY
            | 2569 // BINARY
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn desc(oid: i32) -> ColumnDesc {
        ColumnDesc {
            name: "c".into(),
            type_oid: oid,
            type_len: -1,
            type_mod: -1,
            format: 0,
        }
    }

    #[test]
    fn repeated_key_is_returned_from_last_hit() {
        let caches = ValueCaches::new(&[desc(1043)]);
        let first = caches
            .get_or_decode(0, b"alpha", || Ok(NzValue::Text("alpha".into())))
            .unwrap();
        let second = caches
            .get_or_decode(0, b"alpha", || panic!("decoded a cached value"))
            .unwrap();
        assert_eq!(first, second);
        let stats = caches.stats();
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.insertions, 1);
    }

    #[test]
    fn high_cardinality_disables_the_column() {
        let caches = ValueCaches::new(&[desc(1043)]);
        for i in 0..DEFAULT_PROBE_LOOKUPS {
            let key = i.to_le_bytes();
            let _ = caches
                .get_or_decode(0, &key, || Ok(NzValue::Text(i.to_string().into())))
                .unwrap();
        }
        let stats = caches.stats();
        assert_eq!(stats.disabled_columns, 1);
        assert_eq!(stats.hits, 0);
    }

    #[test]
    fn long_values_are_not_stored() {
        let caches = ValueCaches::new(&[desc(1043)]);
        let long = "x".repeat(DEFAULT_MAX_KEY_BYTES + 1);
        let _ = caches
            .get_or_decode(0, long.as_bytes(), || {
                Ok(NzValue::Text(long.as_str().into()))
            })
            .unwrap();
        assert_eq!(caches.stats().entries, 0);
        assert_eq!(caches.stats().rejected, 1);
    }

    #[test]
    fn non_allocating_types_bypass_the_cache() {
        let caches = ValueCaches::new(&[desc(23)]);
        let _ = caches
            .get_or_decode(0, b"1", || Ok(NzValue::Int4(1)))
            .unwrap();
        let stats = caches.stats();
        assert_eq!(stats.hits, 0);
        assert_eq!(stats.entries, 0);
    }

    #[test]
    fn disabled_column_stops_locking_after_probe() {
        let cache = ColumnCache::new(8, 8, 2);
        cache.state.record(false);
        cache.state.record(false);
        assert!(!cache.state.enabled());
    }
}
