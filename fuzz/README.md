# Protocol fuzzing

This independent cargo-fuzz package exercises URI, text-row metadata, DBOS
descriptor/row, numeric metadata and scalar parsers. Inputs are capped at
64 KiB to focus on parsing rather than oversized allocations. Panics fail
the target; ordinary protocol errors are expected.

Install `cargo-fuzz` and a nightly toolchain, then run from the repository root:

```bash
cargo +nightly fuzz run protocol -- -max_len=65536 -max_total_time=300
```

Keep minimized reproductions as ordinary regression tests. Corpus, artifacts
and build output are ignored. To check the harness without a fuzz campaign:

```bash
cargo check --manifest-path fuzz/Cargo.toml --offline
```
