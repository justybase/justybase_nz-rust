# Security policy

Do not report credentials, private connection strings or exploitable protocol
details in a public issue. Once the GitHub repository is online, use its
private security advisory channel for vulnerability reports.

Please include the affected version, Rust toolchain, feature flags and a
minimal sanitized reproduction. Secrets accidentally committed to a branch or
history must be revoked immediately; removing them from Git does not revoke
them.

## Supported versions

| Version | Status |
| --- | --- |
| `1.0.0-rc.3` | Current release candidate; security fixes are released as new prerelease versions |
| `1.0.0-rc.2` and earlier | Superseded; upgrade to the current candidate |
| `0.x` | Unsupported |

Security fixes are documented in `CHANGELOG.md`. Release candidates are never
republished: a fix for a published RC is released as the next RC so users can
tell the builds apart.

## Known advisory exception

RUSTSEC-2026-0543 marks all `chrono` versions as unmaintained and has no safe
upgrade. `chrono` is an optional, off-by-default dependency of the `chrono`
feature. `deny.toml` records an explicit, justified exception; the project
tracks replacing the conversion helpers and will remove the exception when a
safe alternative is available.
