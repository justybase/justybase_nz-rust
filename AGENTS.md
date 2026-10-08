# Repository Guidelines

## Project Structure & Module Organization

This repository is a Rust 2021 crate for the IBM Netezza/PureData wire
protocol. Driver implementation lives in `src/`; protocol handling is split
among connection, handshake, message, buffer, reader, metadata, parameter,
pool, and type modules. Integration tests are in `tests/`: `mock_server.rs`
exercises the protocol without an appliance, while `live_driver.rs` and
`live_integration.rs` cover appliance-backed behavior. Runnable examples are
under `examples/`, including `dump_to_txt.rs`. Use `README.md` for public API
and compatibility details.

## Build, Test, and Development Commands

Run these checks from the repository root:

```bash
cargo fmt --all -- --check
cargo test -p nz_rust --all-targets
cargo clippy -p nz_rust --all-targets --all-features -- -D warnings
cargo test --workspace --all-targets --offline
```

The example can be smoke-tested without a database:

```bash
cargo run -p nz_rust --features compat --example dump_to_txt -- --demo --out demo.txt
```

Live tests are `#[ignore]`d and need `NZ_DEV_HOST`, `NZ_DEV_USER`,
`NZ_DEV_PASSWORD` and `NZ_DEV_DB` (or `NZ_DEV_DATABASE`); `NZ_DEV_PORT` is
optional. A missing variable fails the run. Run them serially with
`scripts/test-live.sh` (`--qualification`, `--stress`, `--capture` modes;
`scripts/test-live.ps1` on Windows), or:

```bash
cargo test -p nz_rust --features compat \
  --test live_qualification --test live_driver --test live_integration -- \
  --ignored --nocapture --test-threads=1
```

`tests/support` holds the scripted mock backend (fragmentation, handshake and
auth scripts, cancel counting); prefer it for new offline protocol tests.
`tests/fixtures/wire` holds real appliance responses captured by
`examples/capture_wire_fixtures.rs`.

The `perf_fact200k` harness is also opt-in: set `NZ_RUN_REPLAY_PERF=1` to
replay a recorded fixture (override its path with `NZ_REPLAY_FIXTURE`, or place
it at `tests/fixtures/fact200k.nzreplay.gz`), and/or `NZ_RUN_PERF_TESTS=1` with
the `NZ_DEV_*` variables for the live appliance variant.

## Coding Style & Naming Conventions

Format Rust with rustfmt and keep code idiomatic for the Rust 2021 edition:
`snake_case` for functions and tests, `CamelCase` for types, and uppercase
constants. Keep protocol parsing bounded and explicit, preserve public API
compatibility, and add focused documentation for new public items.

## Testing Guidelines

Every behavior change should include an appropriate unit, mock-server, or
integration test. Keep default tests appliance-independent; live suites are
opt-in through `NZ_RUN_LIVE_TESTS=1`. Use descriptive test names such as
`handshake_and_query_round_trip_against_mock_server`.

## Security & Configuration

Never commit Netezza credentials, connection URIs containing passwords, or
generated result files. Use environment variables or a secret manager for
local and CI configuration. Treat SQL parameters as values, not identifiers;
validate or whitelist dynamic identifiers separately.

## Commits and Pull Requests

The checkout does not contain readable Git history, so an established commit
prefix convention could not be verified. Prefer small, imperative, focused
commits. Pull requests should explain the behavior change, identify any
protocol or compatibility impact, list validation commands, and call out
whether live-appliance testing was run or skipped.
