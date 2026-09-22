//! Deterministic DBOS decoder microbenchmark.
//!
//! This is opt-in because it is timing-sensitive and intentionally performs a
//! large number of allocations. Run it in release mode with
//! `NZ_RUN_DECODE_PERF=1`.

use nz_rust::messages::nz_type;
use nz_rust::types::value::NzValue;
use nz_rust::DbosTupleDesc;
use std::env;
use std::hint::black_box;
use std::time::Instant;

fn fixed_desc(types: &[i32], sizes: &[i32]) -> DbosTupleDesc {
    let mut offset = 2i32;
    let mut offsets = Vec::with_capacity(types.len());
    let mut fixed_sizes = Vec::with_capacity(types.len());
    for &size in sizes {
        offsets.push(offset);
        fixed_sizes.push(size);
        offset += size;
    }
    DbosTupleDesc {
        nulls_allowed: 0,
        fixed_fields_size: offset,
        num_fields: types.len(),
        field_type: types.to_vec(),
        field_size: sizes.to_vec(),
        field_true_size: sizes.to_vec(),
        field_offset: offsets,
        field_phys_field: vec![0; types.len()],
        field_null_allowed: vec![false; types.len()],
        field_null_byte_offset: vec![0; types.len()],
        field_null_bit_mask: vec![0; types.len()],
        field_fixed_size: fixed_sizes,
        ..Default::default()
    }
}

fn integer_case() -> (DbosTupleDesc, Vec<u8>) {
    let desc = fixed_desc(
        &[
            nz_type::NZ_TYPE_INT,
            nz_type::NZ_TYPE_INT8,
            nz_type::NZ_TYPE_INT2,
            nz_type::NZ_TYPE_INT1,
        ],
        &[4, 8, 2, 1],
    );
    let mut row = vec![0, 0];
    row.extend_from_slice(&123i32.to_le_bytes());
    row.extend_from_slice(&456i64.to_le_bytes());
    row.extend_from_slice(&789i16.to_le_bytes());
    row.push(12);
    (desc, row)
}

fn boolean_case() -> (DbosTupleDesc, Vec<u8>) {
    let desc = fixed_desc(&[nz_type::NZ_TYPE_BOOL, nz_type::NZ_TYPE_BOOL], &[1, 1]);
    (desc, vec![0, 0, 1, 0])
}

fn datetime_case() -> (DbosTupleDesc, Vec<u8>) {
    let desc = fixed_desc(
        &[
            nz_type::NZ_TYPE_DATE,
            nz_type::NZ_TYPE_TIME,
            nz_type::NZ_TYPE_TIMESTAMP,
        ],
        &[4, 8, 8],
    );
    let mut row = vec![0, 0];
    row.extend_from_slice(&9_000i32.to_le_bytes());
    row.extend_from_slice(&45_678_901i64.to_le_bytes());
    row.extend_from_slice(&789_465_600_123_456i64.to_le_bytes());
    (desc, row)
}

fn checksum(values: &[NzValue]) -> u64 {
    values.iter().fold(0u64, |sum, value| match value {
        NzValue::Bool(value) => sum + u64::from(*value),
        NzValue::Int2(value) => sum.wrapping_add(*value as u64),
        NzValue::Int4(value) => sum.wrapping_add(*value as u64),
        NzValue::Int8(value) => sum.wrapping_add(*value as u64),
        NzValue::Date(value) | NzValue::Time(value) | NzValue::Timestamp(value) => {
            sum.wrapping_add(value.len() as u64)
        }
        _ => sum,
    })
}

fn rows_per_second(rows: usize, elapsed: std::time::Duration) -> f64 {
    rows as f64 / elapsed.as_secs_f64()
}

#[test]
fn dbos_decoder_performance() {
    if env::var("NZ_RUN_DECODE_PERF").ok().as_deref() != Some("1") {
        eprintln!("skipping: set NZ_RUN_DECODE_PERF=1 to run the decoder benchmark");
        return;
    }

    let iterations: usize = env::var("NZ_DECODE_ROWS")
        .unwrap_or_else(|_| "200000".into())
        .parse()
        .expect("NZ_DECODE_ROWS must be an integer");

    println!("Rust DBOS decoder benchmark rows={iterations}");
    for (name, (desc, row)) in [
        ("integer", integer_case()),
        ("boolean", boolean_case()),
        ("datetime", datetime_case()),
    ] {
        let start = Instant::now();
        let mut total = 0u64;
        for _ in 0..iterations {
            let values = desc.parse_row(black_box(&row)).unwrap();
            total = total.wrapping_add(checksum(&values));
            black_box(&values);
        }
        let fresh_elapsed = start.elapsed();

        let start = Instant::now();
        let mut values = Vec::with_capacity(desc.num_fields);
        let mut var_starts = Vec::new();
        for _ in 0..iterations {
            desc.parse_row_into_with_scratch(black_box(&row), &mut values, &mut var_starts)
                .unwrap();
            total = total.wrapping_add(checksum(&values));
            black_box(&values);
        }
        let reused_elapsed = start.elapsed();

        println!(
            "{name:<8} fresh={:>12.0} rows/s reused={:>12.0} rows/s checksum={total}",
            rows_per_second(iterations, fresh_elapsed),
            rows_per_second(iterations, reused_elapsed),
        );
    }
}
