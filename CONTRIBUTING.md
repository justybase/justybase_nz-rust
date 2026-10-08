# Contributing to `nz_rust`

## Local checks

Run these commands from the repository root before opening a pull request:

```bash
cargo fmt --all -- --check
cargo test -p nz_rust --all-targets
cargo clippy -p nz_rust --all-targets --all-features -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps
```

The default test suite is appliance-independent: appliance-backed tests are
`#[ignore]`d. Run them explicitly with `scripts/test-live.sh` (or `.ps1`),
which requires `NZ_DEV_HOST`, `NZ_DEV_USER`, `NZ_DEV_PASSWORD` and
`NZ_DEV_DB`/`NZ_DEV_DATABASE`; a missing variable fails the run. Never commit
those values. GitHub Actions never has appliance access, so every behavior
change needs a unit, mock-server or replay test that runs offline; add LIVE
coverage in addition where the appliance's behavior matters.

The C# compatibility benchmark also requires a live Netezza appliance and is
not part of GitHub Actions CI.

The declared minimum supported Rust version is 1.87.

## Pull requests

Keep changes focused, preserve protocol and public API compatibility, and add
unit or mock-server coverage for behavior changes. Include the Rust version,
feature flags, appliance version when relevant, and a sanitized reproduction.

Do not include credentials, private connection details or generated result
files in commits.
