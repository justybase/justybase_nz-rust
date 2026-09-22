#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
MANIFEST="${NZ_COMPAT_MANIFEST:-$ROOT_DIR/benchmarks/netezza_cross_driver/compatibility_cases.json}"
OUTPUT_DIR="${NZ_COMPAT_OUTPUT_DIR:-$ROOT_DIR/target/netezza-cross-compatibility/core}"
CSHARP_PROJECT="$ROOT_DIR/benchmarks/netezza_cross_driver/csharp/CrossDriverBenchmark.csproj"

mkdir -p "$OUTPUT_DIR"
export NZ_COMPAT_MANIFEST="$MANIFEST"

dotnet run -c Release --project "$CSHARP_PROJECT" -- \
    --compat --manifest "$MANIFEST" --output "$OUTPUT_DIR/csharp.json"
cargo run --example cross_driver_compat -- \
    --manifest "$MANIFEST" --output "$OUTPUT_DIR/rust.json"
cargo run --example cross_driver_compat -- \
    --compare "$OUTPUT_DIR/rust.json" "$OUTPUT_DIR/csharp.json" \
    --report "$OUTPUT_DIR/diff.json"
