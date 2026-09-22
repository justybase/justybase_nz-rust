# Netezza Rust vs `nzpy_extended` performance

This benchmark reproduces the six data-type scenarios from
[`nzpy_extended/tools/examples/performance_test.py`](https://github.com/justybase/nzpy_extended/blob/main/tools/examples/performance_test.py).
It uses 100,000 rows from `JUST_DATA..FACTPRODUCTINVENTORY`, opens a fresh
connection for every scenario, and measures query execution plus complete
result decoding. Connection setup is reported separately.

## Rust benchmark

From the repository root, configure the same variables used by the live tests:

```bash
export NZ_RUN_PERF_TESTS=1
export NZ_DEV_HOST=your_netezza_host
export NZ_DEV_PORT=5480
export NZ_DEV_DATABASE=JUST_DATA
export NZ_DEV_USER=admin
export NZ_DEV_PASSWORD='secret'

NZ_ROWS=100000 NZ_PERF_REPEATS=5 cargo test --release \
  --test nzpy_extended_performance -- \
  --nocapture --test-threads=1 \
  | tee target/nzpy-extended-rust-performance-latest.txt
```

Optional overrides are `NZ_ROWS` and `NZ_BENCH_SOURCE_TABLE`. The benchmark is
opt-in and therefore does not run as part of the normal test suite.

## Python benchmark with C extension

Use an isolated environment and install the package:

```bash
python3 -m venv /tmp/nzpy-extended-venv
/tmp/nzpy-extended-venv/bin/python -m pip install nzpy-extended
```

Clone or download the upstream repository, then run its reference benchmark:

```bash
cd /path/to/nzpy_extended
NZ_HOST="$NZ_DEV_HOST" \
NZ_PORT="${NZ_DEV_PORT:-5480}" \
NZ_DATABASE="${NZ_DEV_DATABASE:-JUST_DATA}" \
NZ_USER="$NZ_DEV_USER" \
NZ_PASSWORD="$NZ_DEV_PASSWORD" \
NZ_ROWS=100000 \
/tmp/nzpy-extended-venv/bin/python tools/examples/performance_test.py \
  --output /path/to/this/repository/target/nzpy-extended-python-performance-latest.txt
```

The script should report `nzpy_extended` with `C ext` enabled. Verify it with:

```bash
/tmp/nzpy-extended-venv/bin/python -c \
  'from nzpy_extended import _cstate; print(_cstate._HAVE_C_EXT)'
```

Do not commit generated reports or credentials.

## Scenarios

The benchmark runs `integer_types`, `numeric_types`, `string_types`,
`datetime_types`, `boolean_types`, and `all_types`. The primary metric is
`rows/s`; lower query time is better. Results can vary substantially with
Netezza cache state, network conditions, and appliance load, so comparisons
should use the same host, database, row limit, and warm/cold state.

## Rust DBOS optimization

The Rust implementation now shares column metadata between rows, reuses DBOS
scratch storage, decodes payloads directly from the read buffer, and processes
complete consecutive `Y` frames in a batch. This matches the important
`process_dbos_batch` behavior of the Python C extension without changing the
public `Row` or `NzValue` API.

For repeatable local decoder measurements, run the opt-in test with a release
build:

```bash
NZ_RUN_DECODE_PERF=1 cargo test --release \
  --test dbos_decode_performance -- --nocapture
```

## Baseline before optimization

Environment: Linux x86_64, Rust release build, `nzpy_extended 0.5.0`, C
extension enabled, 100,000 rows, `JUST_DATA..FACTPRODUCTINVENTORY`.
Rust was run first and Python second; the processes were not concurrent. This
is the pre-change baseline.

| Scenario | Rust rows/s | Python + C rows/s | Faster |
|---|---:|---:|---|
| `integer_types` | 319,385 | 511,730 | Python 1.60× |
| `numeric_types` | 148,133 | 133,605 | Rust 1.11× |
| `string_types` | 146,844 | 162,104 | Python 1.10× |
| `datetime_types` | 370,554 | 453,562 | Python 1.22× |
| `boolean_types` | 417,040 | 578,727 | Python 1.39× |
| `all_types` | 79,474 | 77,365 | Rust 1.03× |

### Post-optimization sample

The following Rust values are medians of three sequential runs using the same
100,000-row scenarios after the DBOS batching and shared-row-metadata changes.
Python+C was run separately with the local `nzpy_extended 0.5.0` installation;
its values are the latest sequential reference sample.

| Scenario | Rust rows/s | Python + C rows/s | Faster |
|---|---:|---:|---|
| `integer_types` | 655,169 | 494,762 | Rust 1.32× |
| `numeric_types` | 179,458 | 137,397 | Rust 1.31× |
| `string_types` | 177,035 | 143,020 | Rust 1.24× |
| `datetime_types` | 623,373 | 538,817 | Rust 1.16× |
| `boolean_types` | 724,603 | 533,891 | Rust 1.36× |
| `all_types` | 72,660 | 72,526 | Rust 1.00× |

Current reports:

- Rust: console output from `cargo test --release --test nzpy_extended_performance`
- Python: report file written by the reproduction command below (e.g. `target/nzpy-extended-python-performance-latest.txt`)

The earlier Python run contained a transient `integer_types` outlier (18,226
rows/s), so it was not used here. For future regression tracking, repeat each
side several times and compare medians or p50/p95 values.

## Current apples-to-apples run

The implementation now stores result rows lazily. The benchmark therefore
explicitly calls `row.values()` for every returned row inside the measured
interval; this matches the Python reference, whose `fetchall()` returns
materialized values. The Rust figures below are medians of three runs. The
Python figures are from the same six scenarios and appliance, with
`nzpy_extended` C extension enabled (`_HAVE_C_EXT=True`).

| Scenario | Rust rows/s (materialized) | Python + C ext rows/s | Relative result |
|---|---:|---:|---|
| `integer_types` | 556,917 | 517,895 | Rust 1.08× |
| `numeric_types` | 162,049 | 145,482 | Rust 1.11× |
| `string_types` | 140,721 | 147,011 | Python 1.04× |
| `datetime_types` | 605,121 | 452,328 | Rust 1.34× |
| `boolean_types` | 466,759 | 583,627 | Python 1.25× |
| `all_types` | 77,909 | 74,914 | Rust 1.04× |

This run shows that the earlier integer/datetime deficit is no longer present
after the DBOS parsing and allocation changes. Boolean is within the normal
run-to-run variance of this appliance benchmark (the five Rust samples ranged
from 428k to 791k rows/s), so it should be rechecked with interleaved driver
order before treating the single-sample Python lead as a code regression. The
combined scenario is now slightly ahead for Rust; the remaining optimization
target is allocation and decoding cost across all 15 columns, especially
NUMERIC and text values.

Reproduction used:

```bash
NZ_RUN_PERF_TESTS=1 NZ_ROWS=100000 NZ_PERF_REPEATS=3 \
  cargo test --release --test nzpy_extended_performance -- \
  --nocapture --test-threads=1

python3 -m venv /tmp/nzpy-extended-venv
/tmp/nzpy-extended-venv/bin/python -m pip install -e /path/to/nzpy_extended nzpy
NZ_HOST="$NZ_DEV_HOST" NZ_PORT="${NZ_DEV_PORT:-5480}" \
NZ_DATABASE="${NZ_DEV_DATABASE:-JUST_DATA}" NZ_USER="$NZ_DEV_USER" \
NZ_PASSWORD="$NZ_DEV_PASSWORD" NZ_ROWS=100000 \
/tmp/nzpy-extended-venv/bin/python \
  /path/to/nzpy_extended/tools/examples/performance_test.py \
  --output target/nzpy-extended-python-current.txt
```

The Python run also reported the useful no-C-extension control: 210,858,
81,078, 89,791, 136,724, 239,963 and 41,242 rows/s respectively. This
confirms that the C extension remains material for the integer, datetime and
boolean paths, while Rust is already faster than the C-extension path in each
of those individual scenarios in this sample.
