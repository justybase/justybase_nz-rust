# Changelog

All notable changes to `nz_rust` will be documented here.

## [Unreleased]

### Fixed

- Native and legacy clients now defer invalid UTF-8 and malformed scalar
  errors from fully framed rows until the response reaches
  `ReadyForQuery`; those statement errors no longer retire a healthy session
  or cause a pool to replace it. Malformed row framing and layout still close
  the connection, and server errors take precedence.
- Native client: SHA-256 password authentication sent the base64 digest with
  `=` padding (45 bytes) instead of unpadded like the legacy engine, MD5 path
  and reference drivers.
- Native client: requests issued while the connection task was shutting down
  after a fatal error could wait forever; they now fail with `Closed`.
- Native client: each `RowStream` owns its batch block, so a partially read
  stream no longer stalls other streams on the same connection.
- Pools (`Pool`, `NzPool`) no longer hand out an idle session whose socket the
  server closed (or wrote unsolicited data to); checkout probes the socket and
  replaces the session. NUL padding after `ReadyForQuery` is tolerated.
- Legacy `NzConnection`: EOF or a transport error mid-response now closes the
  connection (previously `is_closed()` stayed `false` and `NzPool` could reuse
  it), and a command timeout inside a backend message retires the session
  instead of resynchronizing from the middle of a payload.
- Handshake version negotiation (both engines) now requires strictly
  decreasing downgrades, bounding the negotiation.
- `parse_row_description` no longer preallocates from the declared column
  count before checking the payload can hold it.
- The crate-level quick start (`Client::connect` returns a `Client`, not a
  `(Client, Connection)` tuple) and the legacy-module doc examples now compile
  in every feature set; CI runs doctests per feature set.

### Added

- Test infrastructure: scripted mock backend with deterministic TCP
  fragmentation, handshake/auth matrices, fault, cancel-race, stream-lifecycle,
  pool and external-table failure suites, golden wire fixtures captured from a
  real appliance, two new fuzz targets, property tests, `scripts/test-live.*`.
- Appliance-backed tests are `#[ignore]`d and fail (instead of silently
  passing) when run without `NZ_DEV_*` configuration.

## [0.3.3] - 2026-10-05

### Changed

- Default `NzConnectionConfig::client_type` is now JDBC
  (`ClientTypeId::SQL_JDBC = 3`) instead of Node (`15`), so sessions report
  as JDBC to the appliance for auditing and server-side feature gating.
  Override with `config.client_type`, `ConfigBuilder::client_type()` or the
  `?clientType=` / `?client_type=` connection-string parameter.

### Added

- `ConfigBuilder::client_type()` and the `clientType` / `client_type`
  connection-string parameter (numeric or named: `jdbc`, `odbc`, `node`,
  `dotnet`, `golang`, `python`, `oledb`, `sql`).
- Unit tests covering the JDBC default and the builder/URI override.

## [0.2.1] - 2026-09-27

### Fixed

- Reconstructed metadata DDL now handles quoted and reserved identifiers,
  synonym targets, procedure comments, and external-table layouts.
- External-table compression and layout options are emitted in valid SQL form.

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
