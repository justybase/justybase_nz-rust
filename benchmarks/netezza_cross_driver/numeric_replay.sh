#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
OUTPUT_DIR="${NZ_BENCH_OUTPUT_DIR:-$ROOT_DIR/target/netezza-cross-benchmark}"
ITERATIONS="${NZ_BENCH_NUMERIC_ITERATIONS:-200000}"
SAMPLES="${NZ_BENCH_NUMERIC_SAMPLES:-5}"

mkdir -p "$OUTPUT_DIR"

if [[ -n "${NZ_BENCH_RUST_BIN:-}" ]]; then
  NZ_BENCH_NUMERIC_ITERATIONS="$ITERATIONS" NZ_BENCH_NUMERIC_SAMPLES="$SAMPLES" \
    "$NZ_BENCH_RUST_BIN" --numeric-replay \
      --output "$OUTPUT_DIR/rust-numeric-replay.json"
else
  NZ_BENCH_NUMERIC_ITERATIONS="$ITERATIONS" NZ_BENCH_NUMERIC_SAMPLES="$SAMPLES" \
    cargo run --release -p netezza-bench --offline -- \
      --numeric-replay --output "$OUTPUT_DIR/rust-numeric-replay.json"
fi

NZ_BENCH_NUMERIC_ITERATIONS="$ITERATIONS" NZ_BENCH_NUMERIC_SAMPLES="$SAMPLES" \
  node "$ROOT_DIR/benchmarks/netezza_cross_driver/numeric_replay.js" \
    "$OUTPUT_DIR/node-numeric-replay.json"

node "$ROOT_DIR/benchmarks/netezza_cross_driver/numeric_replay_report.js" \
  "$OUTPUT_DIR/node-numeric-replay.json" \
  "$OUTPUT_DIR/rust-numeric-replay.json"
