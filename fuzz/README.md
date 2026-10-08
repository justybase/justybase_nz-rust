# Protocol fuzzing

This independent cargo-fuzz package has three targets:

* `protocol` — URI, text-row metadata, DBOS descriptor/row, numeric metadata
  and scalar parsers.
* `wire_frames` — text rows (both entry points must agree), RowDescription,
  DBOS descriptors/rows, error/notice fields, length validation.
* `sql_lexer` — `split_statements`, `parse_transaction_state`,
  `substitute_parameters`, `substitute_bound_parameters` (deterministic,
  bounded output).

Inputs are capped to focus on parsing rather than oversized allocations.
Panics and violated invariants fail the target; ordinary protocol errors are
expected. `tests/fuzz_regressions.rs` runs the same invariants on mutated valid
frames and seeded inputs in every normal `cargo test`.

Install `cargo-fuzz` and a nightly toolchain, then run from the repository root:

```bash
cargo +nightly fuzz run protocol -- -max_len=65536 -max_total_time=300
cargo +nightly fuzz run wire_frames -- -max_len=65536 -max_total_time=300
cargo +nightly fuzz run sql_lexer -- -max_len=16384 -max_total_time=300
```

Keep minimized reproductions as ordinary regression tests. Corpus, artifacts
and build output are ignored. To check the harness without a fuzz campaign:

```bash
cargo check --manifest-path fuzz/Cargo.toml --offline
```
