# Changelog

All notable changes to `nz_rust` will be documented here.

## [Unreleased]

### Changed

- `NzValue::Timestamp` now carries the typed `NzTimestamp` value
  (microseconds since 2000-01-01) instead of a preformatted `String`. Binary
  TIMESTAMP decoding no longer formats or allocates per row; use `Display` /
  `to_string()` for text or `NzTimestamp::microseconds()` for the numeric
  component. This is a breaking change for code that matched the old `String`
  payload.
- `Client::query_batches` now eagerly decodes every column into one shared
  per-batch arena. Batch rows no longer allocate an `Arc<RawRowData>`, payload
  `Bytes`, or per-cell `OnceLock`; memory is bounded by the existing 256-row /
  1 MiB batch limits. `try_values()` and typed getters read the predecoded
  values directly.
- The binary NUMERIC fast path returns an exact `Decimal` instead of routing
  small values through `f64`, matching the C# driver and avoiding a
  decimal/float round-trip per cell.

## [1.0.0-rc.1] - 2026-11-07

Release candidate. The public API is frozen for the `1.0` series; `1.0.0`
follows after the release-candidate soak. The Preview label remains until
then. See `COMPATIBILITY.md` for the appliance, encoding, type, feature and
semver matrix.

### Fixed

- `CHAR`, `VARCHAR` and `VAR_FIXED_CHAR` values on the binary (DBOS) path are
  now decoded with the configurable `char_varchar_encoding` (default Latin-1)
  instead of strict UTF-8. Netezza often stores Latin-1 bytes even when the
  session encoding is UTF-8; a stored `ó` (`0xF3`) previously failed the whole
  statement. `NCHAR`/`NVARCHAR` and the text path remain UTF-8. Configure with
  `CharEncoding`, `ConfigBuilder::char_varchar_encoding` or
  `?char_varchar_encoding=utf8`.
- Binary `INTERVAL` formatting now keeps days and the clock part as one signed
  duration (`-1 year -2 mons -3 days -04:05:06.789012` instead of folding days
  into hours), uses the server's singular/plural units, and omits a zero clock
  when days or months are present (`-3 days`, `1 year 3 days`).
- Binary `TIMETZ` rendering now matches the server text form
  (`+02` for whole-hour offsets, `+02:30` or `+02:30:15` otherwise), so binary
  and text results stay identical; the previous formatter could disagree on
  whole-hour zones.
- Native and legacy clients defer invalid UTF-8 and malformed scalar errors
  from fully framed rows until the response reaches `ReadyForQuery`; those
  statement errors no longer retire a healthy session or cause a pool to
  replace it. Malformed row framing and layout still close the connection, and
  server errors take precedence.
- Native client: SHA-256 password authentication sent the base64 digest with
  `=` padding (45 bytes) instead of unpadded like the legacy engine, MD5 path
  and reference drivers.
- Native client: requests issued while the connection task was shutting down
  after a fatal error could wait forever; they now fail with `Closed`.
- Native client: each `RowStream` owns its batch block, so a partially read
  stream no longer stalls other streams on the same connection.
- Pools (`Pool`, `NzPool`) no longer hand out an idle session whose socket the
  server closed (or wrote unsolicited data to); checkout probes the socket and
  replaces the session. NUL padding after `ReadyForQuery` is tolerated. Pool
  state locks recover from poisoning instead of aborting.
- Legacy `NzConnection`: EOF or a transport error mid-response now closes the
  connection (previously `is_closed()` stayed `false` and `NzPool` could reuse
  it), and a command timeout inside a backend message retires the session
  instead of resynchronizing from the middle of a payload.
- Handshake version negotiation (both engines) now requires strictly
  decreasing downgrades, bounding the negotiation.
- `parse_row_description` no longer preallocates from the declared column
  count before checking the payload can hold it.
- Windows host and user detection: `os_user` falls back to `USERNAME`, and
  `client_host_name` to `COMPUTERNAME`, so handshakes from Windows no longer
  report `unknown`.
- The crate-level quick start (`Client::connect` returns a `Client`, not a
  `(Client, Connection)` tuple) and the legacy-module doc examples now compile
  in every feature set; CI runs doctests per feature set.

### Added

- `CharEncoding` (`Latin1` default, `Utf8`), connection-string and builder
  support, and encoding-aware DBOS descriptors. Shared `NzValue`/`String`/
  `RawValue` decoding keeps JSON/JSONPATH UTF-8.
- `tests/encoding_matrix.rs`: Latin-1, UTF-8, NCHAR/NVARCHAR, every high byte
  and text-path coverage.
- Golden wire fixtures captured from a real appliance:
  `latin_varchar`, `text_encoding`, `metadata_core`, `ddl_reconstruction`,
  `temporal_boundaries`, `numeric_edges` and `error_details`, with
  `manifest.json` and replay under every TCP fragmentation pattern.
  `examples/capture_wire_fixtures.rs` gained `--suite`, a manifest, a DDL gate
  (`NZ_DEV_ALLOW_FIXTURE_DDL`) and a password/host secret scan.
- Offline catalog coverage in `tests/metadata_mock.rs` for the native and
  legacy metadata surfaces, including the newly completed native wrappers:
  `current_schema`, `sequences`, `users`, `groups`, `query_history`,
  `table_sizes`, `sessions`, `functions`, `synonyms`, `constraints`,
  `all_distribution_keys`, `organize_keys`, `object_details`, `table_owner`,
  `search_objects_detailed` and `objects`.
- DDL reconstruction fixtures and replay for tables (columns, defaults,
  distribution, organize, keys, comments), views and synonyms.
- Numeric edge coverage (38-digit values, negative fractional values), temporal
  boundaries (0001-01-01 to 9999-12-31, TIMETZ, INTERVAL) and structured error
  fields with session recovery.
- Documentation: `#![warn(missing_docs)]` on the public API and
  `#![forbid(unsafe_code)]`, with internal wire modules exempted explicitly.
  `README.md` gained a dedicated character-encoding section, native and legacy
  metadata examples, and the coverage/`cargo-deny` checks. `QUALITY_REPORT.md`
  now reports `1.0.0-rc.1` validation and labels `0.3.0` benchmarks as
  historical.
- `COMPATIBILITY.md`: appliance/encoding/type/feature matrix, known
  representation notes and the `1.0` semver/API-freeze policy.
- `scripts/test-live-matrix.sh` and `.ps1`: run the self-contained
  qualification suite against the primary `NZ_DEV_*` appliance and an optional
  second `NZ_DEV2_*` appliance, writing a credential-free report under
  `target/live-matrix/`.
- CI: Windows and macOS test jobs, a `cargo-deny` advisories/licenses/bans/
  sources job with `deny.toml`, a 70% line-coverage gate, and package plus
  `cargo publish --dry-run` verification.
- Test infrastructure: scripted mock backend with deterministic TCP
  fragmentation, handshake/auth matrices, fault, cancel-race, stream-lifecycle,
  pool and external-table failure suites, golden wire fixtures captured from a
  real appliance, two new fuzz targets, property tests, `scripts/test-live.*`.
- Appliance-backed tests are `#[ignore]`d and fail (instead of silently
  passing) when run without `NZ_DEV_*` configuration.

### Changed

- The crate version is `1.0.0-rc.1`. The public API of the native client,
  blocking facade, pools, metadata, configuration, errors and exports is
  frozen for the `1.0` series; see `COMPATIBILITY.md`.
- `README.md` documents the release-candidate status, encoding configuration,
  native and legacy metadata examples, the two-appliance matrix, the
  coverage/`cargo-deny` checks and the API stability policy.

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
