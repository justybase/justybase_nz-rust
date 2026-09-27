# Changelog

All notable changes to `nz_rust` will be documented here.

## [Unreleased]

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
