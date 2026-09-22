# Contributing to `nz_rust`

## Local checks

Run these commands from the repository root before opening a pull request:

```bash
cargo fmt --all -- --check
cargo test -p nz_rust --all-targets
cargo clippy -p nz_rust --all-targets --all-features -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps
```

The default test suite is appliance-independent. Live tests require
`NZ_RUN_LIVE_TESTS=1`, `NZ_DEV_HOST`, `NZ_DEV_USER` and `NZ_DEV_PASSWORD`;
never commit those values.

The C# compatibility benchmark also requires a live Netezza appliance and is
not part of GitHub Actions CI.

The declared minimum supported Rust version is 1.87.

## Pull requests

Keep changes focused, preserve protocol and public API compatibility, and add
unit or mock-server coverage for behavior changes. Include the Rust version,
feature flags, appliance version when relevant, and a sanitized reproduction.

Do not include credentials, private connection details or generated result
files in commits.
