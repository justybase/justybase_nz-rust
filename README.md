# `nz_rust`

Pure Rust client driver for IBM Netezza and PureData System for Analytics.

[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![CI](https://github.com/justybase/justybase_netezza_driver_rust/actions/workflows/ci.yml/badge.svg)](https://github.com/justybase/justybase_netezza_driver_rust/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/nz_rust.svg)](https://crates.io/crates/nz_rust)
[![docs.rs](https://docs.rs/nz_rust/badge.svg)](https://docs.rs/nz_rust)

> [!WARNING]
> **Preview software.** This crate is under active development and its public
> API, protocol coverage and behavior may change between releases. It is ready
> for evaluation, integration work and controlled internal use; validate it
> against your Netezza environment before using it in production.

## What it is

`nz_rust` is a dependency-light Rust driver for the Netezza simple-query
protocol. It provides a synchronous client, a Tokio-compatible async facade,
connection pools, catalog metadata, cancellation and bounded-memory result
streaming.

The project is intended for Rust applications, workbench integrations and
tools that need direct Netezza connectivity without an ODBC bridge.

The crate implements the Netezza simple-query protocol, including the
handshake and authentication flow, text and binary result formats, Netezza
numeric decoding, catalog metadata, transactions, cancellation, connection
pools and bounded row streaming. Its compatibility surface was checked against
the C# `JustyBase.NetezzaDriver` and the Node/TypeScript
`@justybase/netezza-driver`.

## Status and support expectations

The protocol and metadata surface is covered by unit tests, mock-server tests
and opt-in live Netezza integration tests. The current implementation includes
the following areas:

| Area | Status | Notes |
| --- | --- | --- |
| Synchronous queries and commands | Available | Buffered and streaming APIs |
| Native Tokio client | Available | `Client`/`Connection` follow tokio-postgres' split driver model |
| Legacy async facade | Available | `AsyncNzConnection` uses Tokio's blocking pool for compatibility |
| Connection pooling | Available | Sync and async pools |
| Transactions and cancellation | Available | Recreate a connection after an unsynchronized protocol failure |
| Catalog metadata | Available | Schemas, tables, columns and additional catalog helpers |
| TLS | Opt-in | Enable the `ssl` feature |
| Netezza appliance coverage | Preview | Verify behavior against your appliance and server version |

The compatibility APIs `query` and `execute_reader` buffer result sets. For
large results, use `execute_stream` or the async streaming facade described
below.

The project does not currently promise long-term API stability or complete
coverage of every Netezza server feature. Missing or appliance-specific
behavior should be reported as an issue with the server version, SQL example
and a sanitized error or protocol trace.

## Contents

- [Installation](#installation)
- [Quick start](#quick-start)
- [Connection strings](#connection-strings)
- [Queries, values and parameters](#querying-values-and-parameters)
- [Streaming](#bounded-streaming)
- [Async and pooling](#async-and-pooling-apis)
- [Metadata](#metadata-and-catalog-operations)
- [External-table transfer](#external-table-transfer)
- [Errors and connection lifecycle](#errors-and-connection-lifecycle)
- [Security and compatibility](#security-and-compatibility)
- [Testing](#testing-and-validation)
- [Compatibility notes](#compatibility-notes)
- [Contributing](#contributing-and-support)
- [License](#license)

## Installation

For a published release, add the crate to `Cargo.toml`:

```toml
[dependencies]
nz_rust = "0.1"
```

When using the workspace checkout:

```toml
[dependencies]
nz_rust = { path = "../justybase_netezza_driver_rust" }
```

TLS support is opt-in:

```toml
[dependencies]
nz_rust = { version = "0.1", features = ["ssl"] }
```

The default build has no TLS dependency. The `ssl` feature enables Rustls and
the CA bundle support used by secured Netezza sessions.

## Quick start

The following example connects to a Netezza database and reads a small result
set. Replace the example values with configuration from your environment or
secret manager.

```rust,no_run
use nz_rust::{NzConnection, NzConnectionConfig};

fn main() -> nz_rust::NzResult<()> {
    let config = NzConnectionConfig::new(
        "nz.example.internal",
        "JUST_DATA",
        "admin",
        "secret",
    );
    let mut connection = NzConnection::connect(&config)?;

    let result = connection.query("SELECT 1 AS one, 'hello' AS message", &[])?;
    for row in result.rows() {
        let number: i32 = row.try_get(0)?;
        let message: String = row.try_get(1)?;
        println!("{number}: {message}");
    }

    connection.close();
    Ok(())
}
```

`NzConnection::connect` performs TCP setup, the Netezza handshake and
authentication. `NzConnection::close` is idempotent; dropping a connection
also releases its socket.

`NzConnectionConfig::new` uses port `5480` and the default timeout values. For
full control, construct `NzConnectionConfig` directly and set fields such as
`command_timeout`, `connection_timeout`, `security_level`, `app_name` and
`client_type`.

## Connection strings

Both `netezza://` and `nz://` schemes are accepted, case-insensitively:

```rust,no_run
use nz_rust::NzConnection;

let mut connection = NzConnection::connect_with_str(
    "netezza://admin:secret@nz.example.internal:5480/JUST_DATA?sslmode=require",
)?;
# connection.close();
# Ok::<(), nz_rust::NzError>(())
```

Supported connection-string details include percent-decoding, query options
such as `sslmode`, `securityLevel`, `commandTimeout` and
`connectionTimeout`, and bracketed IPv6 hosts:

```text
nz://user:password@[2001:db8::10]:5480/JUST_DATA?sslmode=verify-full
```

Do not put credentials in source control. Prefer environment variables or a
secret manager in applications and CI.

## Querying, values and parameters

`NzValue` is the compatibility value model returned by the driver. It includes
null, boolean, signed integer, floating-point, exact numeric, text, temporal
and binary variants. NUMERIC values that fit the fixed-width
`rust_decimal::Decimal` representation use `NzValue::Decimal`; wider Netezza
values use the lossless `NzValue::Numeric(String)` fallback. Temporal values
remain canonical owned strings.

Rows can be read by ordinal or case-insensitive column name and extracted with
the checked `FromSql` API:

```rust,no_run
use nz_rust::{NzConnection, NzConnectionConfig};

# fn run(mut connection: NzConnection) -> nz_rust::NzResult<()> {
let rows = connection.query_rows(
    "SELECT product_id, product_name FROM inventory WHERE product_id = $1",
    &[&42i32],
)?;
for row in rows {
    let id: i32 = row.try_get(0)?;
    let name: String = row.try_get("product_name")?;
    println!("{id}: {name}");
}
# Ok(())
# }
```

The `ToSql` trait is implemented for the common Rust scalar types, `String`,
byte slices, `NzValue` and `Option<T>`. `query_one`, `query_opt`, `execute`,
`batch_execute`, `begin_transaction`, `commit`, `rollback` and
`transaction` are available on `NzConnection`.

Netezza's simple-query path does not provide a server-side bind/prepare
message. Parameters are therefore escaped and substituted into the SQL text
by the driver. Values are protected by the driver's literal escaping, but
parameters must not be used as SQL identifiers; validate or whitelist table,
column and database identifiers separately.

For C#/ADO.NET-style named or positional parameters, use `NzCommand` and
`NzParameter`:

```rust,no_run
use nz_rust::{NzConnection, NzValue};

# fn run(connection: &mut NzConnection) -> nz_rust::NzResult<()> {
let mut command = connection.create_command(
    "SELECT * FROM inventory WHERE product_id = :product_id",
    vec![],
);
command = command.add_named_parameter("product_id", NzValue::Int4(42));
connection.execute_command(&mut command)?;
println!("rows affected: {}", command.records_affected);
# Ok(())
# }
```

## Bounded streaming

`execute_stream` decodes one row at a time and invokes a user-provided
`QueryStreamSink` before reading the next backend message. The connection does
not accumulate the full result set. Override `on_values` when the consumer can
process borrowed values immediately; the default `on_values` implementation
creates an owned `Row` for compatibility.

```rust,no_run
use nz_rust::{ColumnDesc, NzConnection, NzResult, NzValue, QueryStreamSink};

struct Counter {
    rows: u64,
}

impl QueryStreamSink for Counter {
    fn on_columns(
        &mut self,
        _result_set: usize,
        _columns: &[ColumnDesc],
        _nullability: Option<&[bool]>,
    ) -> NzResult<()> {
        Ok(())
    }

    fn on_row(&mut self, _result_set: usize, row: nz_rust::Row) -> NzResult<()> {
        self.rows += 1;
        let _ = row;
        Ok(())
    }

    fn on_values(
        &mut self,
        _result_set: usize,
        _columns: &[ColumnDesc],
        values: &[NzValue],
    ) -> NzResult<()> {
        self.rows += 1;
        let _ = values;
        Ok(())
    }
}

# fn run(mut connection: NzConnection) -> NzResult<()> {
let mut counter = Counter { rows: 0 };
let summary = connection.execute_stream(
    "SELECT * FROM large_table",
    &[],
    &mut counter,
)?;
println!("{} rows", counter.rows);
println!("{} result sets", summary.result_sets.len());
# Ok(())
# }
```

The sink may apply backpressure by blocking or returning an error. A sink
error or protocol error makes the connection unsafe to reuse; reconnect before
issuing another command. This prevents an aborted consumer from accidentally
reading the remainder of an old response as a new query response.

`StreamSummary` reports result-set metadata, row counts, affected rows and
server notices. Multiple result sets are delivered in order.

## Async and pooling APIs

`Client` and `Connection` provide the native Tokio API. `connect` returns both
objects, just like `tokio-postgres`; the caller runs the connection future and
uses the cloneable client for serialized queries. `query_stream` exposes a
bounded `RowStream` with backpressure. The compatibility `AsyncNzConnection`
API remains available for cancellation, metadata and existing applications;
it moves the legacy blocking protocol work to Tokio's blocking pool.

```rust,no_run
use nz_rust::{connect, NzConnectionConfig};

# async fn run() -> nz_rust::NzResult<()> {
let config = NzConnectionConfig::new("nz-host", "JUST_DATA", "admin", "secret");
let (client, connection) = connect(&config).await?;
let driver = tokio::spawn(connection);
let row = client.query_one("SELECT 1", &[]).await?;
let value: i32 = row.try_get(0)?;
assert_eq!(value, 1);
client.close().await?;
driver.await.map_err(|e| nz_rust::NzError::Closed(e.to_string()))??;
# Ok(())
# }
```

```rust,no_run
use nz_rust::{AsyncNzConnection, NzConnectionConfig};

# async fn run() -> nz_rust::NzResult<()> {
let config = NzConnectionConfig::new("nz-host", "JUST_DATA", "admin", "secret");
let connection = AsyncNzConnection::connect(&config).await?;
let row = connection.query_one("SELECT 1", &[]).await?;
let value: i32 = row.try_get(0)?;
assert_eq!(value, 1);
# Ok(())
# }
```

Use `query_with_timeout`, `execute_with_timeout` or
`execute_stream_with_timeout` when a command needs a per-call wall-clock
budget. The deadline covers sending, response decoding and streaming fetches;
on timeout the driver sends an out-of-band cancel and drains the abandoned
response before the next command. `AsyncNzConnection::cancel()` can be awaited
from another Tokio task while a query is running and keeps the same session
usable when the server returns `ReadyForQuery`.

Use `NzPool` for synchronous applications and `AsyncNzPool` for Tokio
applications. Both support maximum and minimum connection counts, checkout
timeouts, idle/lifetime/use rotation and rollback-on-release. SQL errors keep a
drained session; transport, timeout and protocol errors retire it. An open
transaction is rolled back before a connection is returned to the idle queue
when `rollback_on_release` is enabled (the default).

## Metadata and catalog operations

Catalog helpers are available through a short-lived mutable metadata view:

```rust,no_run
use nz_rust::{NzConnection, NzConnectionConfig};

# fn run(mut connection: NzConnection) -> nz_rust::NzResult<()> {
let schemas = connection.metadata().schemas()?;
let tables = connection.metadata().tables(Some("ADMIN"), Some("FACT%"))?;
let columns = connection.metadata().columns("FACTPRODUCTINVENTORY", Some("ADMIN"))?;
println!("{} schemas, {} tables, {} columns", schemas.len(), tables.len(), columns.len());
# Ok(())
# }
```

The metadata surface includes schemas, databases, tables, columns, views,
procedures, functions, synonyms, constraints, distribution keys, organize
keys, table sizes, sessions, object details and object search. Use
`change_database` to switch catalogs without reconnecting when no transaction
is active.

## External-table transfer

The crate also exposes the Netezza external-table import/export framing used by
the workbench. Register import data with `register_import_data`, execute the
corresponding external-table command, and unregister it when the transfer is
complete. Invalid buffer sizes are reported as errors; they are not silently
replaced with a default.

## Errors and connection lifecycle

All fallible operations return `NzResult<T>`. `NzError` distinguishes
configuration, transport, timeout, database and protocol failures.

- `NzError::Database` contains structured backend diagnostics and SQLSTATE
  when supplied by Netezza.
- `NzError::Protocol` means framing or sequencing was invalid and the
  connection must be recreated.
- `NzError::Timeout` means the command timeout path attempted out-of-band
  cancellation; recreate the connection if the server or transport cannot be
  confirmed synchronized.

The driver drains normal database errors through `ReadyForQuery`, allowing a
pool to retain the session after a SQL error. It retires sessions that cannot
be safely synchronized.

## Security and compatibility

- Do not commit passwords or connection strings containing credentials. Use
  environment variables or a secret manager.
- SQL parameters are treated as values. They must not be used for table,
  column or database identifiers; validate or whitelist dynamic identifiers
  separately.
- TLS is opt-in through the `ssl` feature. Use certificate verification in
  secured environments and test the selected `sslmode` against your appliance.
- The simple-query protocol performs client-side parameter substitution; it is
  not a server-side prepared-statement API.
- Performance depends on the appliance, network, query shape and value types.
  No universal benchmark claim is made for this preview release.

## Testing and validation

Offline/unit validation from the workspace root:

```bash
cargo fmt --all -- --check
cargo test -p nz_rust --all-targets
cargo clippy -p nz_rust --all-targets --all-features -- -D warnings
cargo test --workspace --all-targets --offline
```

Live tests use `NZ_DEV_HOST`, `NZ_DEV_PORT`, `NZ_DEV_USER` and
`NZ_DEV_PASSWORD`:

```bash
NZ_RUN_LIVE_TESTS=1 \
  cargo test -p nz_rust --test live_driver --test live_integration -- \
  --nocapture --test-threads=1
```

The live suites are opt-in because they require access to a Netezza appliance.
The mock-server and unit suites are the default appliance-independent checks.

The `examples/nz-editor` TUI is intentionally excluded from the published
crate. It is a separate local package and uses the published
`justybase-spreadsheet` crate from crates.io. The cross-driver benchmark is
also repository tooling; its C# runner uses the `JustyBase.NetezzaDriver`
NuGet package to compare live compatibility and correctness, while its Node
runner requires the separately built Node driver. These comparisons require a
real Netezza appliance and are not run in GitHub Actions.

### Rust/C# result compatibility

The shared compatibility manifest is
`benchmarks/netezza_cross_driver/compatibility_cases.json`. It compares column
names, result-set shape, NULLs, canonical values, affected rows and selected
type categories. Floating-point cases use an explicit tolerance; errors are
compared by outcome rather than localized message text.

Run the deterministic/core live comparison with the C# reference driver:

```bash
NZ_DEV_HOST=... NZ_DEV_USER=... NZ_DEV_PASSWORD=... \
  benchmarks/netezza_cross_driver/run_core.sh
```

Run the extended comparison by importing the existing C# reference query
corpus. The importer only reads the JavaScript source; it does not run Node or
Node tests:

```bash
NZ_DEV_HOST=... NZ_DEV_USER=... NZ_DEV_PASSWORD=... \
  benchmarks/netezza_cross_driver/run_full.sh
```

Set `NZ_CSHARP_REFERENCE_QUERIES` when the reference corpus is stored at a
different path. Reports are written below `target/netezza-cross-compatibility`
and must not be committed. The report separates documented C# representation
differences (for example, wide NUMERIC rounding) from unexpected mismatches;
only unexpected mismatches make the command fail.

The C# benchmark project restores `JustyBase.NetezzaDriver` from NuGet. It is
not a dependency of `nz_rust` and is not included in the crates.io package.

## License

Licensed under the [Apache License, Version 2.0](LICENSE).

## Contributing and support

Bug reports, compatibility findings and focused pull requests are welcome.
Please include the Rust version, Netezza/PureData server version, relevant
feature flags and a sanitized reproduction. Never include credentials or
private connection details.

## Compatibility notes

- The simple-query protocol performs client-side parameter substitution; it is
  not a server-side prepared-statement API.
- `query` and `execute_reader` retain all result rows in memory.
- `execute_stream` is the bounded-memory API and is the preferred choice for
  large exports, grids and result stores.
- `Client` uses native Tokio socket I/O; `AsyncNzConnection` is retained as a
  blocking-pool compatibility facade.
- The crate does not impose `chrono` or `time`; it uses `rust_decimal` for
  exact NUMERIC values within its fixed-width range and retains a string
  fallback for wider Netezza values.
