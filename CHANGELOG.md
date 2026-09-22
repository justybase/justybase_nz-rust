# Changelog

All notable changes to `nz_rust` will be documented here.

## [0.1.1] - 2026-09-22

### Changed

- Updated the runtime, cryptography, TLS, decimal and serialization dependencies
  to current stable releases supported by the Rust 1.87 MSRV.
- Refreshed the separate `nz-editor` lockfile and its spreadsheet dependency.

## [Unreleased]

### Added

- Public-release metadata, package validation and GitHub Actions CI.
- Published-crate packaging excludes the editor example and benchmark tooling.

### Changed

- The crate package and Rust import are named `nz_rust`.
- The C# compatibility benchmark consumes `JustyBase.NetezzaDriver` from NuGet.
- The `nz-editor` example consumes `justybase-spreadsheet` from crates.io.

## [0.1.0] - Unreleased

Initial preview release of the pure-Rust IBM Netezza / PureData driver.
