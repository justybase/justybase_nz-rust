# Compatibility and support matrix

This document records the appliance, encoding, type and API surfaces that
`nz_rust` 1.0.0-rc.1 claims to support, plus the rules that make the public API
stable from this release onward.

## Validation baseline

| Item | Value |
| --- | --- |
| Driver version | `1.0.0-rc.1` |
| Primary appliance | IBM Netezza `Release 11.2.2.1 [Build 20]` |
| Session encoding | `SHOW NZ_ENCODING` → `UTF8` |
| Stored `CHAR`/`VARCHAR` bytes | database encoding, observed as Latin-1/CP1250-compatible single bytes |
| `NCHAR`/`NVARCHAR` bytes | UTF-8 |
| Rust MSRV | `1.87` |
| Operating systems in CI | Linux, Windows, macOS |
| Live suites | `live_qualification` (self-contained temp tables), `live_driver`, `live_integration`, opt-in `live_stress`, `live_admin` |

A second appliance can be exercised with the matrix script described below.
The driver does not hard-code the 11.2 baseline: it only requires the Netezza
simple-query protocol, but behavior on releases other than the baseline is
reported as tested only after `scripts/test-live-matrix` has been run against
that appliance.

## Character encoding

Netezza has two different text channels, and the driver treats them separately.

| SQL type | Binary (DBOS) path | Text path |
| --- | --- | --- |
| `CHAR`, `VARCHAR`, `VAR_FIXED_CHAR` | decoded with `char_varchar_encoding` (default **Latin-1**) | UTF-8 as rendered by the server |
| `NCHAR`, `NVARCHAR` | UTF-8 | UTF-8 |
| `JSON`, `JSONPATH` | UTF-8 | UTF-8 |

The default matches the Node driver and `nzpy` (`char_varchar_encoding=latin`).
It is required on appliances that store Latin-1 bytes even when the session
encoding is UTF-8; decoding those bytes as UTF-8 used to fail the statement.

Configure it explicitly when the database really stores UTF-8 in
`CHAR`/`VARCHAR`:

```rust
use nz_rust::{CharEncoding, NzConnectionConfig};

let mut config = NzConnectionConfig::new("nz-host", "JUST_DATA", "admin", "secret");
config.char_varchar_encoding = CharEncoding::Utf8;
```

```text
nz://admin:secret@nz-host:5480/JUST_DATA?char_varchar_encoding=utf8
```

Accepted spellings: `latin`, `latin1`, `latin-1`, `iso-8859-1`, `utf8`,
`utf-8`. Latin-1 decoding is lossless for every byte value (`0x80..=0xFF` map to
`U+0080..=U+00FF`), but characters outside Latin-1 (for example most Polish
letters) cannot be represented in `VARCHAR`; store them in `NVARCHAR`. A
borrowed `&str` cannot hold a transcoded value, so non-ASCII Latin-1 values must
be read through `String`, `NzValue` or `try_values`; the borrowed
`try_get_raw_typed::<_, &str>` path returns a clear error instead of corrupting
the value.

## Type coverage

| SQL family | Binary decoding | Text decoding | Typed getters |
| --- | --- | --- | --- |
| `BOOLEAN` | yes | yes | `bool` |
| `BYTEINT`, `SMALLINT`, `INTEGER`, `BIGINT` | yes | yes | `i16`, `i32`, `i64`, unsigned getters |
| `REAL`, `DOUBLE PRECISION` | yes | yes | `f32`, `f64` |
| `NUMERIC(p,s)` up to 38 digits | yes, exact | yes | `NzNumeric`, `rust_decimal::Decimal` within its range, canonical text |
| `DATE`, `TIME`, `TIMESTAMP` | yes | yes | canonical text, numeric temporal components |
| `TIMETZ` | yes, server-compatible `±HH[:MM[:SS]]` | yes | canonical text, `NzTimetz` |
| `INTERVAL` | yes, months + signed microseconds | yes | canonical text, `NzInterval` |
| `CHAR`, `VARCHAR`, `VAR_FIXED_CHAR` | yes, configurable encoding | yes | `String`, borrowed `&str` for representable bytes |
| `NCHAR`, `NVARCHAR` | yes, UTF-8 | yes | `String`, borrowed `&str` |
| `JSON`, `JSONPATH` | yes, UTF-8 | yes | `String` |
| `VARBINARY` / `BYTEA` | yes | yes | `Vec<u8>` |
| Arrays, GIS, interval-typed variants outside the above | not claimed | not claimed | — |

Known representation notes:

- Binary `CHAR` trims trailing spaces; the text path preserves them
  (`"ab   "`).
- DATE/TIMESTAMP values use the Netezza epoch (2000-01-01) and are rendered
  without a time zone.
- Legacy Netezza 11.2 errors may arrive as raw text without SQLSTATE,
  `detail` or `hint`; `NzDatabaseError.raw` always retains the original text.
- Wide NUMERIC values that do not fit `rust_decimal::Decimal` stay exact in
  `NzValue::Numeric(String)`.

## Authentication and transport

| Capability | Status |
| --- | --- |
| Plain password | supported |
| MD5 password | supported |
| SHA-256 password (unpadded base64 digest) | supported |
| Unsecured session | supported |
| TLS (`ssl` feature) | supported; `sslmode` / `securityLevel` selectable |
| Cancellation (out-of-band packet) | supported by native, blocking, pool and compat clients |
| External-table file transfer | opt-in through `ExternalFilePolicy`; disabled by default |

## Feature flags

| Feature | Effect |
| --- | --- |
| *(default)* | native Tokio client, blocking facade, pools, metadata |
| `compat` | legacy `NzConnection`, `NzCommand`, `NzDataReader`, `AsyncNzConnection`, `NzPool`, `AsyncNzPool` and compatibility aliases |
| `ssl` | Rustls-based TLS transport |
| `chrono` | optional `chrono` conversions for temporal values |

## Public API stability and semver policy

Starting with `1.0.0-rc.1`, the following surface is frozen for the `1.0`
series; release candidates may still correct API mistakes before `1.0.0`.
After `1.0.0`, changes follow Cargo's interpretation of Semantic Versioning:

- **Patch releases** (`1.0.x`) contain bug, security and documentation fixes
  only. They do not remove or change public items, method signatures, feature
  names or documented behavior.
- **Minor releases** (`1.x.0`) may add public items, trait implementations,
  feature flags and variants of non-exhaustive enums. Breaking changes require
  a major release (`2.0.0`).
- **Release candidates** (`1.0.0-rc.N`) may still correct API mistakes before
  `1.0.0`. Each RC documents every change in `CHANGELOG.md`; applications
  should pin the exact prerelease while evaluating.
- The `compat` feature is a migration surface, not a second stability tier: its
  items follow the same semver rules as the native API.
- Internal protocol modules (`messages`, `buffer`, `handshake`, `tuple_desc`)
  are published for integration and fixture tooling, but are exempt from the
  documentation requirement. Their fields may track the appliance wire format
  and change in minor releases; use the safe row/metadata APIs for application
  code.
- `#[non_exhaustive]` may be added to public enums in a minor release; matching
  code must include a wildcard arm.
- Deprecations are announced in `CHANGELOG.md` and the item documentation, and
  are kept for at least one minor release before removal.
- The minimum supported Rust version is `1.87`. Raising it is a minor-release
  change and is called out in `CHANGELOG.md`.
- Wire fixtures (`tests/fixtures/wire`) are test data, not API; they may be
  regenerated when a new appliance capture adds coverage.

The freeze covers the items re-exported at the crate root plus the public
modules `blocking`, `config`, `error`, `export`, `metadata`, `native_async`,
`pool`, `types` and (with `compat`) `connection`, `asynchronous`,
`async_pool`. `cargo doc` treats missing documentation on this surface as a
warning, and CI builds docs with `-D warnings`.

## Live two-appliance matrix

`scripts/test-live-matrix.sh` (and `.ps1`) runs `live_qualification` against
the primary `NZ_DEV_*` appliance and, when configured, a second `NZ_DEV2_*`
appliance. Credentials are read only from the environment and are never written
to the report.

```bash
# Primary appliance (required)
export NZ_DEV_HOST=... NZ_DEV_USER=... NZ_DEV_PASSWORD=... NZ_DEV_DB=...
# Second appliance or server version (optional)
export NZ_DEV2_HOST=... NZ_DEV2_USER=... NZ_DEV2_PASSWORD=... NZ_DEV2_DB=...

scripts/test-live-matrix.sh
scripts/test-live-matrix.sh --require-two   # fail if NZ_DEV2_* is absent
```

The report is written to `target/live-matrix/report.txt` and is excluded from
the published crate. A matrix run is required before tagging `1.0.0` and is
recommended for every appliance patch level.

## Reporting a compatibility problem

Please include:

- driver version (`nz_rust`), Rust version and feature flags;
- appliance model, `SELECT version()`, and `SHOW NZ_ENCODING`;
- the SQL statement with literals replaced by parameters;
- whether the same statement succeeds through `nzpy`, the Node driver or ODBC;
- a sanitized `NzError` (including `raw` for legacy errors) and, when
  possible, a wire fixture captured with
  `examples/capture_wire_fixtures.rs`.

Never include credentials, private host names or production result data.
