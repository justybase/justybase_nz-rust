#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
OUTPUT_DIR="${NZ_COMPAT_OUTPUT_DIR:-$ROOT_DIR/target/netezza-cross-compatibility/full}"
SOURCE="${NZ_CSHARP_REFERENCE_QUERIES:-$ROOT_DIR/../justybase_netezza_node_driver/tests/helpers/referenceQueries.js}"
MANIFEST="$OUTPUT_DIR/compatibility_full.json"

mkdir -p "$OUTPUT_DIR"
python3 "$ROOT_DIR/benchmarks/netezza_cross_driver/build_full_compat_manifest.py" \
    "$ROOT_DIR/benchmarks/netezza_cross_driver/compatibility_cases.json" \
    "$SOURCE" "$MANIFEST"

NZ_COMPAT_MANIFEST="$MANIFEST" \
NZ_COMPAT_OUTPUT_DIR="$OUTPUT_DIR" \
    "$ROOT_DIR/benchmarks/netezza_cross_driver/run_core.sh"
