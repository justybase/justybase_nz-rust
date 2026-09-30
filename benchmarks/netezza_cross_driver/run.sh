#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
OUTPUT_DIR="${NZ_BENCH_OUTPUT_DIR:-$ROOT_DIR/target/netezza-cross-benchmark}"
mkdir -p "$OUTPUT_DIR"
cd "$ROOT_DIR"
if [[ -z "${NZ_BENCH_CSHARP_PROJECT:-}" ]]; then
    export NZ_BENCH_CSHARP_PROJECT="$(python3 "$ROOT_DIR/benchmarks/netezza_cross_driver/prepare_csharp_reference.py" "${NZ_CSHARP_DRIVER_ROOT:-$ROOT_DIR/../JustyBase.NetezzaDriver}" "$OUTPUT_DIR/csharp-reference")"
fi

export NZ_BENCH_SCENARIOS="${NZ_BENCH_SCENARIOS:-$ROOT_DIR/benchmarks/netezza_cross_driver/scenarios.json}"

if [[ -n "${NZ_BENCH_RUST_BIN:-}" ]]; then
    "$NZ_BENCH_RUST_BIN" --output "$OUTPUT_DIR/rust.json"
else
    cargo run --release -p nz_rust --features compat \
        --example netezza_bench -- --output "$OUTPUT_DIR/rust.json"
fi
node "$ROOT_DIR/benchmarks/netezza_cross_driver/node_runner.js" "$OUTPUT_DIR/node.json"
dotnet run -c Release --project "$ROOT_DIR/benchmarks/netezza_cross_driver/csharp/CrossDriverBenchmark.csproj" -- --output "$OUTPUT_DIR/csharp.json"
python3 "$ROOT_DIR/benchmarks/netezza_cross_driver/python_runner.py" "$OUTPUT_DIR/python.json"
node "$ROOT_DIR/benchmarks/netezza_cross_driver/report.js" \
    "$OUTPUT_DIR/csharp.json" "$OUTPUT_DIR/node.json" "$OUTPUT_DIR/rust.json" "$OUTPUT_DIR/python.json"
