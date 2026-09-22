# Binary NUMERIC performance analysis

## Question

The original cross-driver benchmark showed Rust behind C# on
`binary-numeric-heavy`:

| Driver | Average latency for 10,000 rows |
| --- | ---: |
| C# | 118.51 ms |
| Rust | 126.10 ms |
| Node | 135.89 ms |

The gap is 7.59 ms, or approximately 6.4% relative to Rust. This is the
largest absolute and relative Rust deficit in the original binary profiles.

## What the query exercises

The profile has four numeric columns:

- two `NUMERIC(30,6)` values, which must remain precision-preserving;
- one `NUMERIC(10,4)` value;
- one `NUMERIC(18,2)` value.

The result is consumed once per cell. Connection establishment is excluded,
but query execution, socket reads, protocol parsing, binary row decoding and
value materialization are included.

## Representation difference

The reference C# path decodes a numeric into a `decimal` and stores it in a
reused `RowValue` struct. The struct has typed storage for numeric, temporal
and primitive values. The large values in this benchmark fit within the .NET
`decimal` range, so C# does not need to produce decimal text for them.

The Rust path now exposes one public `NzValue` enum with two exact numeric
representations. Values that fit `rust_decimal::Decimal` use the fixed-width
`NzValue::Decimal` variant; values outside its 96-bit coefficient or scale
range retain the lossless `NzValue::Numeric(String)` fallback. The streaming
decoder reuses the fallback string only when it is needed.

The relevant paths are therefore conceptually:

```text
C#:    binary words -> decimal -> RowValue.decimalValue
Rust:  binary words -> i128 -> Decimal (common case)
                         \-> decimal text -> Numeric (wide fallback)
```

This is a data-model difference, not evidence that the Rust compiler or TCP
implementation is intrinsically slower.

## Precision matrix

The live benchmark was extended with three focused profiles. The post-change
run used the same live connection, 10,000 rows, one warm-up and five measured
samples. Values below are average milliseconds:

| Profile | C# | Rust | Node | Rust deficit vs C# |
| --- | ---: | ---: | ---: | ---: |
| Mixed numeric, original | **122.35** | 127.00 | 141.91 | 3.8% |
| Low precision only | 110.68 | **110.50** | 114.03 | Rust wins by 0.2% |
| High precision only | **115.81** | 124.11 | 134.76 | 7.2% |
| Single high precision cell | **103.58** | 110.57 | 117.30 | 6.8% |

The low-precision result improved to parity after the Decimal fast path. The
Rust decoder now returns `f64` only when the value and scale round-trip cleanly;
otherwise it returns a fixed-width Decimal, preserving values such as `3.1400`
without materializing decimal text. The high-precision live gap remains because
the database read, row materialization and public enum handling are still part
of the measurement; the decoder-only replay below shows that the arithmetic
itself is no longer the bottleneck for Decimal-representable values.

The remaining high-precision live gap is therefore not caused by converting the
value to decimal text. It points to the surrounding row/materialization path,
which is still measurably cheaper in the C# driver.

## Decoder-only replay

The deterministic replay uses the same encoded numeric word layout and calls
the three implementations without a database connection. A 200,000 iteration
run with five samples produced the following approximate average nanoseconds
per conversion:

| Case | C# | Rust | Node |
| --- | ---: | ---: | ---: |
| `NUMERIC(10,4)` | 16.81 ns | 110.88 ns | 72.80 ns |
| `NUMERIC(10,4)` trailing zero | 23.14 ns | 130.01 ns | 63.96 ns |
| `NUMERIC(30,6)` | 23.41 ns | 18.31 ns | 295.72 ns |
| high precision negative | 44.78 ns | 18.12 ns | 352.45 ns |

These numbers are diagnostic, not an end-to-end ranking. C# returns a
value-type `decimal`; Rust's replay calls the public allocating conversion
function. The live streaming path reuses the Rust exact-decimal String, so the
replay isolates conversion and representation work rather than claiming that
every live-row conversion allocates.

The replay explains two otherwise surprising details:

1. Rust's high-precision direct conversion is now faster than C# because the
   tested values fit in `rust_decimal` and the result is a 16-byte value with
   no decimal-string allocation.
2. The low-precision Rust replay still includes the compatibility decision
   between `f64` and Decimal; the live result is nevertheless at C# parity.
3. C# remains very fast on high precision because the tested values fit into
   its native `decimal`; Node pays the largest cost in its arbitrary-precision
   compatibility path.

## Conclusions

Before the change, the C# advantage was caused by three combined effects:

1. native `decimal` materialization instead of exact decimal text;
2. a compact typed row representation instead of a universal enum with
   string-backed temporal/numeric variants;
3. a simpler low-precision acceptance path without Rust's string round-trip
   check.

The short-string pool is not involved in the numeric profiles. The previous
control run with the C# pool disabled also left C# ahead on binary profiles.

## `rust_decimal` implementation and limits

`rust_decimal` is a credible candidate for a follow-up experiment. Its
representation is similar to .NET `decimal`: a fixed-width integer mantissa,
scale and sign. The current documentation describes a 96-bit mantissa and a
limit of roughly 28 significant decimal digits. See the
[`rust_decimal` documentation](https://docs.rs/rust_decimal/latest/rust_decimal/).

That fits the two `NUMERIC(30,6)` values in this benchmark because they have
26 significant digits. The driver now uses this hybrid design:

```text
precision <= 28 and value fits Decimal -> fixed-width Decimal
otherwise                              -> exact decimal text/big-decimal path
```

The public `NzValue::Decimal` variant is intentionally explicit. Consumers
that need the original text can use `to_display_string`, `to_node_canonical` or
`FromSql<String>`; consumers that need exact arithmetic can match on Decimal
and avoid reparsing text. Values beyond the Decimal range still use
`NzValue::Numeric(String)`, so NUMERIC(38) remains lossless.

## Constraints and next target

It would be unsafe to replace either exact representation with `f64`: that
would lose precision. The public value model now deliberately exposes Decimal
for the common exact range and retains the string fallback for wider values.

The remaining end-to-end optimization target is row materialization and public
value access, not replacing Decimal with an imprecise `f64`. Any future change
must retain the hybrid fallback and the exact-value tests.
