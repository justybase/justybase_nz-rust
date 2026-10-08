# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

`nz_rust` is a pure-Rust driver for the IBM Netezza simple-query wire protocol (single crate, MSRV 1.87). `AGENTS.md` holds the repo guidelines (style, security, PR expectations) — follow it as well.

## Commands

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-targets --all-features --locked     # offline; LIVE tests show as ignored
cargo test --features compat --test wire_faults                  # one test binary
cargo test --features compat --test wire_faults protocol_fault_marks   # one test
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features --locked
cargo check --manifest-path fuzz/Cargo.toml --locked             # fuzz crate is a separate workspace
```

CI also runs the feature matrix (`""`, `ssl`, `chrono`, `compat`, `ssl,chrono,compat` with `--no-default-features`) and an MSRV 1.87 `cargo check`. Most integration tests need `--features compat` (declared via `required-features` in `Cargo.toml` — a new test binary using legacy types needs a `[[test]]` entry too).

### LIVE (appliance) tests

GitHub CI has no appliance and never will; LIVE tests are `#[ignore]`d and run only locally:

```bash
scripts/test-live.sh [--qualification | --stress | --capture | --all]   # .ps1 on Windows
```

Needs `NZ_DEV_HOST`, `NZ_DEV_USER`, `NZ_DEV_PASSWORD`, `NZ_DEV_DB` (or `NZ_DEV_DATABASE`), optional `NZ_DEV_PORT`; missing vars fail the run (`tests/live_support`). `live_qualification` uses only TEMP tables; tests whose ignore reason mentions `JUST_DATA` need the sample schema. `--capture` regenerates `tests/fixtures/wire/*.bin` (format in its README); review the diff before committing.

## Architecture

Two independent protocol engines coexist; a fix in one usually needs a matching look at the other:

- **Native async** (`src/native_async.rs`): `Client` + background `Connection` task (tokio-postgres style). Requests go through an mpsc queue to `run_connection`; `drive_operation` handles timeout/cancel/abandoned-stream with a generation counter in `Control`, and a connection is reusable only after `Ok`/`Database` errors (any other error ends the task and closes the client). `Pool`/`blocking::Client`/`blocking::Pool` (`async_pool.rs`, `blocking.rs`) are built on it. Has its own handshake (`AsyncSession::handshake`).
- **Legacy/compat** (`connection.rs` `NzConnection`, `reader.rs`, `pool.rs` `NzPool`, `asynchronous.rs`): blocking socket engine with its own handshake in `handshake.rs`, only public with the `compat` feature. Poisoning is explicit (`mark_protocol_fault` / `mark_faulted_after`, `protocol_sync_required`, `frame_in_progress`).

Shared pieces: `tuple_desc.rs` (text `T` RowDescription, binary `X` DBOS descriptor and `Y` row decoding), `types/` (text/binary value decoding, NUMERIC, temporal), `params.rs` (client-side `$n`/`:name`/`?` substitution — there are no server-side prepared statements), `messages.rs` (message codes, statement splitting, transaction tracking), `error.rs` (`NzError`, `validate_protocol_length`, `MAX_PROTOCOL_PAYLOAD`), `buffer.rs` (`ReadBuffer`), `external.rs` (external-table file policy).

Wire facts worth knowing: backend frames are `[type][4 skipped bytes][i32 BE length][payload]` (`Z` has no length; DBOS `Y` has an extra reserved word); the appliance pads with NULs after `Z` and sends a `P…"blank"` pseudo-message before `T`; text NULL bitmap is MSB-first with set = present, DBOS bitmap is LSB-first from byte 2 indexed by physical field with set = NULL. A framing/length fault must retire the connection (a pool must never reuse it); an `ErrorResponse` followed by `Z` is a normal `NzError::Database` and keeps the session.

## Testing infrastructure

Offline protocol tests use `tests/support/mod.rs`: byte-exact frame encoders (text, DBOS), `Chunking` fragmentation plans (whole/1/2/3/4/7 bytes/seeded), and `MockServer` (scripted handshake versions and auth, per-connection handler `Session`, `accepted` counter and `cancels` board to prove physical-connection identity and cancel packets). Prefer it for new tests; use `wait_until` only for flags the driver publishes asynchronously (e.g. `Client::is_closed()` right after an error). `tests/allocation_bounds.rs` installs a per-thread tracking global allocator. `fuzz/fuzz_targets/*` have mirrored bodies in `tests/fuzz_regressions.rs` — keep them in sync. Soak new concurrency tests on one core (`taskset -c 0 cargo test …`) to expose scheduling assumptions.

Encoding caveat for live work: the test appliance's `VARCHAR` is Latin (non-UTF-8); use `NVARCHAR` for Unicode in fixtures/tests.
