# Golden wire fixtures

Byte-exact server→client responses captured from a real Netezza appliance for
**synthetic** statements by `examples/capture_wire_fixtures.rs`, replayed
offline by `tests/wire_replay.rs`.

## Format

```
"NZWIRE01"                      8 bytes magic
u32 LE record count
record × count:
    u32 LE sql length,  sql bytes (UTF-8)
    u32 LE response length, response bytes
```

A response starts at the first byte after the statement was sent and ends
with the `ReadyForQuery` message (`Z` + 4 bytes); between records the
appliance's NUL padding is omitted. Records are meant to be replayed in order
on one session (later statements may use TEMP tables created by earlier ones).

## What is — and is not — in them

* Included: query-phase messages (`P` pseudo-message, `T`, `D`, `X`, `Y`, `C`,
  `N`, `E`, `Z`) for the statements listed in `fixtures()`.
* Never included: anything client→server (so no credentials), the handshake
  and authentication exchange, `BackendKeyData`, host/user/database names. The
  capture tool scans each response for the configured user, password and host
  and refuses to write a fixture containing one.

## Regenerating

```
scripts/test-live.sh --capture        # needs NZ_DEV_* and a plaintext session
```

Review the diff before committing: the statements and expected values in
`tests/wire_replay.rs` are hand-written from the SQL literals, so a changed
response is either an appliance behaviour change or a driver bug.
