#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
OUTPUT_DIR="${NZ_BENCH_OUTPUT_DIR:-$ROOT_DIR/target/netezza-cross-benchmark}"
mkdir -p "$OUTPUT_DIR"

export NZ_BENCH_SCENARIOS="${NZ_BENCH_SCENARIOS:-$ROOT_DIR/benchmarks/netezza_cross_driver/scenarios.json}"

if [[ -n "${NZ_BENCH_RUST_BIN:-}" ]]; then
    "$NZ_BENCH_RUST_BIN" --output "$OUTPUT_DIR/rust.json"
else
    cargo run --release -p netezza-bench -- --output "$OUTPUT_DIR/rust.json"
fi
node "$ROOT_DIR/benchmarks/netezza_cross_driver/node_runner.js" "$OUTPUT_DIR/node.json"
dotnet run -c Release --project "$ROOT_DIR/benchmarks/netezza_cross_driver/csharp/CrossDriverBenchmark.csproj" -- --output "$OUTPUT_DIR/csharp.json"
node "$ROOT_DIR/benchmarks/netezza_cross_driver/report.js" \
    "$OUTPUT_DIR/csharp.json" "$OUTPUT_DIR/node.json" "$OUTPUT_DIR/rust.json"
