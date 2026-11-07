# Driver quality report

## Current release: `1.0.0-rc.1`

`1.0.0-rc.1` freezes the public API for the `1.0` series. It remains preview
software until `1.0.0` has soaked in controlled use; the support policy and the
appliance/type/encoding/feature matrix are in
[COMPATIBILITY.md](COMPATIBILITY.md). The integration guide is
[README.md](README.md), and the complete change list is
[CHANGELOG.md](CHANGELOG.md).

This release was validated against Netezza
`Release 11.2.2.1 [Build 20]`. No credentials or connection strings are stored
in this report or in the repository.

### Correctness changes in this release

| Finding reproduced before changes | Implemented correction |
| --- | --- |
| Binary `CHAR`/`VARCHAR` values containing Latin-1 bytes (for example `ó` = `0xF3`) decoded as invalid UTF-8 | Encoding-aware DBOS field decoding; Latin-1 is the default, configurable with `char_varchar_encoding` / `CharEncoding` |
| Borrowed `&str` getters could silently mangle non-ASCII Latin-1 data | Borrowed getters return an error for transcoded values; `String` and `NzValue` materialize the decoded text |
| Binary `INTERVAL` folded days into hours and used non-server unit spellings | Signed day/clock components are preserved, units match the server, and a zero clock is omitted when days or months are present |
| Binary `TIMETZ` always rendered `±HH:MM` | Offsets now match the server text form: `+02`, `+02:30` or `+02:30:15` |
| Native metadata stopped at the core catalog helpers | Completed `AsyncMetadata`: current schema, sequences, users, groups, query history, table sizes, sessions, functions, synonyms, constraints, distribution/organize keys, object details, owners, search and DDL reconstruction |
| Netezza 11.2 errors could arrive without SQLSTATE/detail/hint | Structured errors preserve the original message in `NzDatabaseError.raw` and keep the session usable when the backend can resynchronize |
| A poisoned pool lock could hide the original panic | Pool lock poisoning is recovered explicitly in the sync and async pools |
| Windows session identity was incomplete | `os_user` / `client_host_name` fallbacks were added |

### Validation for this release

| Check | Result |
| --- | --- |
| `cargo fmt --all -- --check` | pass |
| `cargo test -p nz_rust --all-targets --all-features --locked` | pass |
| `cargo test -p nz_rust --doc --all-features --locked` | 4/4 pass |
| `cargo clippy -p nz_rust --all-targets --all-features --locked -- -D warnings` | pass |
| `cargo doc --no-deps --all-features --locked` with `RUSTDOCFLAGS=-D warnings` | pass |
| Feature matrix (`default`, `ssl`, `chrono`, `compat`, all) | pass |
| `cargo deny check advisories licenses bans sources` | pass |
| Line-coverage gate (70%) | 73.84% lines, 71.63% regions, 63.98% functions |
| `cargo package` / `cargo publish --dry-run` | pass — 108 files, 1.8 MiB |
| Library unit tests | 155/155 |
| Encoding matrix | 6/6 |
| Metadata mock (native + legacy) | 2/2 |
| Golden wire replay | 17/17 across 15 fixtures |
| Pool mock tests | 14/14 |
| Live qualification | 13/13 |
| Live driver | 14/14 |
| Live integration | 45/45 |
| Live two-appliance matrix | primary pass; secondary skipped because `NZ_DEV2_*` was not configured |

Golden fixtures in `tests/fixtures/wire` are real appliance responses to
synthetic statements. They are replayed offline by `tests/wire_replay.rs` and
cover encoding, native and legacy metadata, DDL reconstruction, temporal and
numeric edges, and structured errors. The capture tool scans statements and
responses for the configured password and host before writing a fixture.

### Known limitations

- The release candidate has been exercised against one appliance build. The
  secondary matrix is implemented and documented but requires `NZ_DEV2_*`; a
  two-appliance run is required before tagging `1.0.0`.
- Binary `CHAR`/`VARCHAR` text defaults to Latin-1. Store characters outside
  Latin-1 in `NVARCHAR`, or set `char_varchar_encoding=utf8` when the database
  truly stores UTF-8.
- Some Netezza 11.2 errors arrive as legacy raw text without SQLSTATE, detail
  or hint; use `NzDatabaseError.raw` for the original message.
- No universal performance claim is made for the release candidate. The
  benchmark numbers later in this document are historical `0.3.0` data.

### Reproducing the validation

The commands are listed in [README.md](README.md#testing-and-validation) and
[CONTRIBUTING.md](CONTRIBUTING.md). Live suites require `NZ_DEV_*`; the matrix
runner additionally supports `NZ_DEV2_*`. Reports are written below `target/`
and must not be committed.

## Historical report: 0.3.0

> This appendix is retained for project history. It describes version `0.3.0`
> only; its benchmark numbers, test counts and reference-driver results are not
> claims about `1.0.0-rc.1`.

### Scope and implementation status

This report covers version 0.3.0. The existing `AGENTS.md` was preserved.

All reproduced critical defects described below have fixes and regression
coverage. The Rust API migration, native async metadata helpers, producer-side
batching, regression suites, repeat performance runs and instrumented fuzz
campaign are complete.

### Correctness findings and fixes

| Finding reproduced before changes | Implemented correction |
| --- | --- |
| Binary VARCHAR lengths 32,766, 32,767 and 40,000 became NULL in typed rows | Unsigned length decoding, validated bounds and checked per-cell decoding |
| Decode errors silently became SQL NULL | `try_values()` and typed getters propagate errors; `values()` fails explicitly |
| Negative month/microsecond intervals and durations over 24 hours were formatted incorrectly | Signed component handling and numeric temporal getters |
| Numeric strings could inject SQL; temporal strings were not escaped | Strict numeric grammar, safe literal construction, NUL/binding validation |
| A dropped async pool lease permanently consumed capacity | Owned permits, cancellation-safe reservations and bounded rollback on return |
| Closing a paused native stream could hang | Independent close/cancel control and bounded parser cleanup |
| `query_one` accepted multiple rows | Exactly-one semantics; zero-or-one has `query_opt` |
| Unicode URIs and malformed DBOS descriptors could panic | Checked URI slicing, descriptor validation and generated malformed-input tests |

Additional changes validate text scalar values and UTF-8, retain unknown binary
fields as bytes, keep OIDs distinct from DBOS codes, and validate NUMERIC
precision/scale before arithmetic. Backslashes follow observed Netezza literal
semantics: string parameters use `chr(92)` when necessary. Unsupported binary
SQL parameters now return an error instead of generating invalid SQL.

### API, lifecycle and security

The primary APIs are `Client`, `blocking::Client`, `Pool` and `blocking::Pool`.
The primary blocking implementation uses the native Tokio protocol engine.
Buffered and streamed native queries share response handling. Exclusive
transaction guards serialize clones; dropped guards roll back. Pool streams
and transactions borrow their lease. Cancellation retains partially read
frames and drains the session before reuse; failed cleanup closes it.

Stream events expose metadata, result boundaries, notices, rows and command
completion. Queues limit retained payloads to 8 MiB and 256 events; batches
limit delivery to 256 rows/1 MiB, allowing one oversized row. Notice history
has separate count/byte limits and reports dropped entries. Borrowed fields,
`NzNumeric` and numeric temporal getters avoid unnecessary materialization.

Filesystem transfers default to disabled. Directory policy resolves symlinks;
it assumes an independently trusted local directory because check/open is not
atomic. Scoped import readers belong to one operation; the global registry
requires `compat`. Configuration debug output redacts passwords. Query timeouts
default to disabled; native connect and cleanup deadlines are 10 and 5 seconds.

### Performance measurements (0.3.0)

Baseline: Git HEAD `eb9dd61e9e20b626450558042c3b9709b96cac3d` (0.2.3),
extracted read-only into `/tmp`. The final modified release measurements used
the same instrumented runner, appliance and workload: 100,000 rows, seven
samples and one warmup; scalar queries used 100 repetitions. The baseline
snapshot has five samples, so p50 changes are directional rather than a
strictly paired statistical comparison. Allocation bytes and peak RSS below
are final modified-run averages and maxima.

| Materialized scenario | Baseline p50 ms (5 samples) | Modified p50 ms (7 samples) | Change | Allocated bytes avg | Peak RSS KiB max |
| --- | ---: | ---: | ---: | ---: | ---: |
| Text scalar | 276.92 | 290.46 | +4.89% | 457,700 | 3,004 |
| Binary fixed width | 479.54 | 473.53 | -1.25% | 87,277,742 | 88,340 |
| Binary variable width | 581.70 | 554.53 | -4.67% | 93,075,983 | 112,040 |
| Binary numeric heavy | 566.71 | 544.13 | -3.98% | 67,662,419 | 132,396 |
| Numeric low precision | 483.14 | 466.46 | -3.45% | 46,888,388 | 132,396 |
| Numeric high precision | 481.57 | 481.12 | -0.09% | 43,687,904 | 146,324 |
| Numeric single high precision | 424.33 | 390.48 | -7.98% | 40,487,332 | 172,104 |

No materialized p50 case regressed by more than 5% in this repeated run; text
scalar was close to that limit. It does not establish a universal throughput
gain. A separate final `execute_stream` run measured p50 values of 260.31 ms
(text), 407.68 ms (fixed width), 519.69 ms (variable width), 462.73 ms
(numeric heavy), 431.91 ms (numeric low), 441.06 ms (numeric high) and
408.64 ms (single high precision). Its peak RSS maxima were 3,000; 19,092;
35,440; 68,176; 68,176; 82,196 and 107,976 KiB respectively. The streaming
figures describe another API mode and are not compared directly to the
materialized baseline. Reports are in ignored `target/quality-final-rust-7samples.json`
and `target/quality-final-stream-7samples.json`.

The first five-sample snapshot recorded 55–99.6% fewer allocated bytes in its
paired comparison. The final run records allocation bytes for repeatability,
but its allocation counter was noisy in some multi-row samples, so those
earlier percentage reductions should not be treated as a refreshed paired
claim. Removing repeated empty 64 KiB buffer allocations and lazy cell
decoding remains supported by the allocation profile and targeted tests.

NUMERIC decoder replay used 2,000,000 iterations and seven samples. Results
were identical; per-case timing changes ranged from -1.2% to +2.4%, within
the proposed 5% regression limit. This measures the public compatibility
decoder, not every new exact-numeric getter.

The shared manifest ran again after the three reference-driver changes against
all four local drivers using 10,000 rows, one warmup and three samples. Latest
p50 (ms) for fixed-width rows was C# 101.73, Node 105.62, Python 108.50 and
Rust 112.87; variable-width was C# 129.72, Node 133.32, Python 131.80 and Rust
139.25. Rust's text scalar workload measured 264.52 ms. These short live runs
include appliance, network and runtime effects. Comparison with the earlier
run shifted even on workloads untouched by the corresponding code changes,
so those differences are directional and cannot establish causal speedups or
regressions. Reports, including CPU and peak RSS where supported, are in
ignored `target/netezza-cross-benchmark/`; the earlier snapshot is in
`target/quality-cross-driver/`. Final Rust reports are
`target/quality-final-buffered.json` and `target/quality-final-stream.json`.

Reproduce with `benchmarks/netezza_cross_driver/run.sh` and
`numeric_replay.sh`; see the benchmark README for environment controls.

### Validation (0.3.0)

The all-feature all-target regression run passed 131 unit tests, 29
compatibility tests, 24 mock protocol tests and all other enabled test targets.
The default-feature all-target run also passed 131 unit, 1 decoder performance,
3 generated-input and 1 typed-value tests. Separately, both opt-in suites ran
against appliance `Release 11.2.2.1 [Build 20]` with `NZ_RUN_LIVE_TESTS=1` and
`NZ_DEV_*`: all 14 driver and 45 integration tests passed, including producer
batching, async and legacy DDL parity, import/export, cancellation, exact values,
blocking lifecycle and pool cleanup. Credentials were not recorded. Final
formatting, Clippy with warnings denied, rustdoc with warnings denied and
`git diff --check` pass. Earlier validation also passed an offline
all-target/all-feature check using Rust 1.87.0. The TLS fixture key is
disposable test data, not a credential.

### Rust plan completion (0.3.0)

The native async metadata API now reconstructs table, view, procedure,
external-table and synonym DDL. Bulk table/view/procedure operations batch
catalog reads. The old public API is opt-in behind `compat`; the README,
changelog and examples document the migration and 0.3 breaking change.
`query_batches` groups rows at the protocol producer under row, byte and
channel limits, including a singleton path for one oversized row. Unit,
mock-server and live tests cover grouping and first-result-set behavior.

The nightly libFuzzer protocol campaign completed 301 seconds and 2,064,458
instrumented runs with 1,531 coverage counters and 3,435 feature edges, using
92 MiB peak RSS. It found no crash. `ASAN_OPTIONS=detect_leaks=0` was needed
because LeakSanitizer cannot operate under this environment's ptrace policy;
AddressSanitizer and coverage instrumentation remained enabled. Further tuning
is evidence-driven: the final measurements show no repeatable materialized p50
regression above 5%, but do not support a general latency-improvement claim. A
dedicated native batch benchmark and paired allocation baseline remain useful
follow-up measurements.

### Reference-driver implementation and validation (0.3.0)

The authorized follow-up work is implemented in all three reference drivers.
The changes preserve their public APIs except for the additive C# pool
validation interval. Rust's documented 0.3 API migration remains the only
planned breaking change.

| Driver | Implemented change | Evidence and limits |
| --- | --- | --- |
| C# | Variable-field offsets are precomputed in one bounded pass per row; wide fixed strings use cleared `ArrayPool<char>` scratch; reused row slots clear stale references; pool probes are async, with an optional idle validation interval (default `0` retains validation on every checkout). The interval never suppresses closed/expired checks. | Offset microbenchmark: at 64 fields 2.854 us → 154.86 ns (~18x), at 256 fields 58.36 us → 657.50 ns (~89x), zero managed allocations. Synthetic NativeAOT benchmark, not end-to-end throughput. Final unit suite: 80 passed. Integration suite with Python reference path: 137 passed, 1 skipped. |
| Node | Batch cache drains by head index and is released when exhausted; contiguous text DataRow parsing uses a synchronous `Buffer` view with owned-copy fallback for fragmented frames; external export waits for `drain` and propagates stream errors. | Unit suite: 211 passed. Full suite: 477 passed, 1 skipped. The first run found one stale live assertion that omitted the database qualifier already emitted by DDL reconstruction; the assertion now checks the fully qualified output. |
| Python | The global fixed-size pool is capped at 32 buffers (8 MiB at 256 KiB each) and reports retained bytes/count; DBOS parsing consumes short-lived memoryviews, including fragmented payloads backed by one owned buffer. | Unit suite: 206 passed, 1 skipped. C-extension and pure-Python parity: 110 passed; C extension is enabled. `mypy` succeeds; Pyright reports 0 errors and 232 pre-existing warnings. Live C/Python parity and contract integration: 116 passed. |

Python's buffer cap intentionally bounds process-retained memory; after the
cap is full, released buffers are collected instead of cached. Node zero-copy
views are limited to synchronous parsing so subsequent buffer reuse cannot
change returned rows. Stream export errors now fail the operation instead of
being silently treated as successful completion.
