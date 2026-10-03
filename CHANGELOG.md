# Changelog

All notable changes to `nz_rust` will be documented here.

## [0.2.1] - 2026-09-27

### Fixed

- Reconstructed metadata DDL now handles quoted and reserved identifiers,
  synonym targets, procedure comments, and external-table layouts.
- External-table compression and layout options are emitted in valid SQL form.

## [Unreleased]

## [0.3.2] - 2026-10-03

### Fixed

- Buffered `query()` rows recover the exact `NzNumeric` coefficient from the
  eagerly decoded value, fixing `NzNumeric` extraction for NUMERIC cells that
  decode as `Float8` (small-precision or integral values).

### Changed

- `query()` now decodes cells eagerly while draining the response, so a
  malformed cell fails the call instead of a later per-column read. Eager rows
  expose `RawValue::as_bytes()` only for byte-preserving cells (untrimmed text
  and binary payloads); other types must be read through the typed interface,
  and `try_get_raw_value` no longer returns the original NUMERIC/integer wire
  bytes.
- Released read-buffer high-water capacity after each drained response, added
  fast binary/text decode paths and a column-metadata cache, and added the
  opt-in `perf_fact200k` benchmark harness.

## [0.3.1] - 2026-09-30

### Fixed

- Corrected the repository links in crate metadata to
  `https://github.com/justybase/justybase_nz-rust`.

## [0.3.0] - 2026-09-30

### Fixed

- Long binary VARCHAR uses unsigned lengths; decoding errors never become NULL.
- Invalid descriptor fields, text scalar values, UTF-8, numeric metadata and
  connection URIs return checked errors instead of corrupt values or panics.
- SQL bindings validate numeric literals, missing/unused values and NUL;
  backslashes use Netezza-compatible `chr(92)` expressions.
- Signed intervals preserve negative months, microseconds and hours above 99.
- Native cancellation/close bypass backpressure and resume the pinned parser
  through ReadyForQuery, including a timeout in the middle of a TCP frame.
- Native pool slots are reserved through connect, cancellation and cleanup;
  dropped holders return or retire their connection without leaking capacity.
- Benchmark and compatibility example commands enable the required `compat`
  feature, and retired pool sessions finish shutdown before capacity is reused.

### Added

- A shared native transport for `Client` and `blocking::Client`, streaming
  blocking iterators, byte-bounded event queues and row batches.
- Exclusive RAII transaction guards, standard `Pool`/`blocking::Pool` APIs,
  borrowed pool streams, `QueryOptions`, `ConfigBuilder` and `TypeInfo`.
- Exact `NzNumeric` and numeric date/time/timestamp/timetz/interval getters.
- Native core metadata with shared SQL/decoders and a batched snapshot.
- Direct streaming text export, scoped import readers and explicit file policy.
- Deterministic malformed-input tests, TLS mocks and four-driver benchmark
  runners with allocation, CPU, first-row and RSS observations.

### Changed

- **Breaking:** 0.3 defaults to `Client`, `blocking::Client`, `Pool` and
  `blocking::Pool`. Legacy `NzConnection`, `NzCommand`, `NzDataReader`,
  `AsyncNzConnection`, `NzPool` and `AsyncNzPool` APIs now require the
  `compat` feature and are re-exported from `nz_rust::compat`. Enable
  `features = ["compat"]` during migration, then switch to the native APIs.
- Default statement timeout is disabled. `query_one` enforces exactly one row.
- `ToSql` requires `Debug + Sync`; binary SQL bindings are explicitly rejected.
- External filesystem access is opt-in. The global import registry requires
  `compat`; scoped readers are recommended.
- Execute/batch execution discards rows, and query row extraction moves data.
- Removed repeated replacement allocations of 64 KiB read buffers.

## [0.2.0] - 2026-09-27

### Added

- Extended catalog helpers for sequences, users, groups, query history,
  detailed columns, table keys, comments, owners and reconstructed DDL.
- Bounded synchronous and asynchronous reader sources for external-table
  imports.
- Exact `rust_decimal::Decimal` conversion and optional `chrono` conversions.
- Public-release metadata, package validation and GitHub Actions CI. The
  published crate excludes the editor example and benchmark tooling.

### Changed

- Aborting a streaming sink now cancels row delivery and drains the response
  through `ReadyForQuery`, preserving a reusable connection when draining
  succeeds.
- Metadata object arguments accept `[schema.]object`; database-qualified
  three-part names are rejected instead of silently resolving in the current
  database.
- The crate package and Rust import are named `nz_rust`; the C# compatibility
  benchmark and `nz-editor` example use their published NuGet and crates.io
  dependencies.

## [0.1.1] - 2026-09-22

### Changed

- Updated the runtime, cryptography, TLS, decimal and serialization dependencies
  to current stable releases supported by the Rust 1.87 MSRV.
- Refreshed the separate `nz-editor` lockfile and its spreadsheet dependency.

## [0.1.0] - Unreleased

Initial preview release of the pure-Rust IBM Netezza / PureData driver.
