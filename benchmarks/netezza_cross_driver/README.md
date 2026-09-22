# Cross-driver Netezza benchmark

This benchmark runs the same SQL scenarios against the same live appliance
with the C#, Node and Rust drivers. It measures an already-open connection and
includes query execution, protocol decoding and scalar value access.

It is deliberately split into text-protocol scalar decoding, fixed-width
binary rows, variable-width binary strings and several binary numeric
precision profiles. A single winner is not assumed: the report ranks drivers
per profile, because C# and Rust expose different native representations for
temporal and decimal values.

The benchmark reads:

- `NZ_DEV_HOST`
- `NZ_DEV_PORT` (default `5480`)
- `NZ_DEV_USER` (default `admin`)
- `NZ_DEV_PASSWORD`
- `NZ_DEV_DB` or `NZ_DEV_DATABASE` (default `JUST_DATA`)

Optional controls are `NZ_BENCH_ROWS`, `NZ_BENCH_SAMPLES`,
`NZ_BENCH_WARMUP`, `NZ_BENCH_TEXT_REPETITIONS`, `NZ_BENCH_SOURCE_TABLE` and
`NZ_BENCH_OUTPUT_DIR`. Set `NZ_BENCH_SCENARIOS` to use a focused scenario file,
such as `numeric_scenarios.json`. `NZ_BENCH_CSHARP_STRING_POOL=0` disables the C#
reference driver's normal short-string pool for an additional allocation
profile. `NZ_BENCH_RUST_BIN` can point to a prebuilt Rust runner when the
surrounding workspace has unrelated packages unavailable in the local Cargo
index.

The Node driver must already be built (`npm run build` in
`justybase_netezza_node_driver`). The runner loads `dist/cjs` by default;
override it with `NZ_NODE_DRIVER_ENTRY` when using another build. Run from
this repository:

```bash
NZ_DEV_HOST=... NZ_DEV_PORT=5480 NZ_DEV_USER=... NZ_DEV_PASSWORD=... \
  benchmarks/netezza_cross_driver/run.sh
```

Results are JSON files under `target/netezza-cross-benchmark`. The tabular
summary sorts each scenario by average latency and also reports p50/p95 and
throughput. The rows/cells and SQL are identical for all three runners; warmup
and measured samples are recorded in every output.

## Numeric decoder replay

The live benchmark includes network and appliance costs. To isolate the
numeric conversion itself, run the deterministic replay:

```bash
NZ_BENCH_NUMERIC_ITERATIONS=200000 \
NZ_BENCH_NUMERIC_SAMPLES=5 \
  benchmarks/netezza_cross_driver/numeric_replay.sh
```

The replay feeds the same encoded Netezza numeric words to the Rust and Node
conversion functions. It does not open a database connection. The C# NuGet
package intentionally does not expose its internal numeric conversion helper,
so the C# runner participates in the live compatibility benchmark rather than
this decoder-only replay. Results are written to
`target/netezza-cross-benchmark/*-numeric-replay.json`.

This is a decoder microbenchmark, not a replacement for the live comparison.
The Rust replay calls the public conversion function so it can be compared
directly with the Node reference conversion function. The live streaming path
returns a fixed-width `rust_decimal::Decimal` for representable values and
reuses the exact-decimal `String` buffer only for the wider fallback; therefore
the replay explains arithmetic and representation costs, while the live
benchmark remains authoritative for end-to-end performance.

For the release comparison used during development:

```bash
NZ_BENCH_ROWS=10000 NZ_BENCH_SAMPLES=5 NZ_BENCH_WARMUP=1 \
NZ_BENCH_TEXT_REPETITIONS=100 \
NZ_DEV_HOST=... NZ_DEV_PORT=5480 NZ_DEV_USER=... NZ_DEV_PASSWORD=... \
  benchmarks/netezza_cross_driver/run.sh
```

The C# reference keeps its default per-column short-string pool enabled. That
is part of its normal driver behavior and is therefore retained in the primary
comparison; an apples-to-apples allocation study should also publish a second
run with that feature disabled rather than silently changing defaults.

Run only the numeric precision matrix with:

```bash
NZ_BENCH_SCENARIOS="$PWD/benchmarks/netezza_cross_driver/numeric_scenarios.json" \
NZ_BENCH_RUST_BIN="$PWD/target/release/netezza-bench" \
  benchmarks/netezza_cross_driver/run.sh
```
