//! Deterministic malformed-input checks; no database or external fuzz runtime needed.
use nz_rust::{parse_connection_string, DbosTupleDesc};
fn next(seed: &mut u64) -> u64 {
    *seed ^= *seed << 13;
    *seed ^= *seed >> 7;
    *seed ^= *seed << 17;
    *seed
}
#[test]
fn descriptor_and_uri_parsers_do_not_panic_on_generated_inputs() {
    let mut seed = 0x536ad879c41u64;
    for length in 0..512 {
        let bytes: Vec<u8> = (0..length).map(|_| next(&mut seed) as u8).collect();
        if let Ok(descriptor) = DbosTupleDesc::parse(&bytes, None) {
            let _ = descriptor.parse_row(&bytes);
        }
        let text = String::from_utf8_lossy(&bytes);
        let _ = parse_connection_string(&text);
    }
    // Exercise complete, otherwise valid descriptor headers, mutating one word.
    let mut base = vec![0; 80];
    for (offset, value) in [
        (16, 1i32),
        (24, 7),
        (28, 7),
        (32, 1),
        (36, 3),
        (40, 4),
        (44, 4),
        (48, 3),
        (64, 1),
    ] {
        base[offset..offset + 4].copy_from_slice(&value.to_be_bytes());
    }
    for offset in (0..80).step_by(4) {
        for value in [i32::MIN, -1, 0, 1, 2, 100_000, i32::MAX] {
            let mut bytes = base.clone();
            bytes[offset..offset + 4].copy_from_slice(&value.to_be_bytes());
            if let Ok(descriptor) = DbosTupleDesc::parse(&bytes, None) {
                for length in 0..32 {
                    let _ = descriptor.parse_row(&vec![0; length]);
                }
            }
        }
    }
}
#[test]
fn numeric_metadata_is_bounded_before_arithmetic_or_allocation() {
    for (precision, scale, words) in [
        (38, -1, 4),
        (38, i32::MAX, 4),
        (38, 0, i32::MAX),
        (-1, 0, 4),
        (39, 0, 4),
    ] {
        assert!(
            nz_rust::types::numeric::get_cs_numeric(&[0; 20], precision, scale, words).is_err()
        );
    }
}

#[test]
fn malformed_text_scalars_and_present_lengths_are_not_values_or_null() {
    for (value, oid) in [
        ("garbage", 16),
        ("garbage", 23),
        ("999999999999999999999999", 20),
        ("x", 1700),
    ] {
        assert!(nz_rust::types::text::try_parse_text_value(value, oid, -1).is_err());
    }
    let column = nz_rust::ColumnDesc {
        name: "v".into(),
        type_oid: 1043,
        type_len: -1,
        type_mod: -1,
        format: 0,
    };
    for length in [-1i32, 0, 1, 2, 3] {
        let mut bytes = vec![0x80];
        bytes.extend_from_slice(&length.to_be_bytes());
        assert!(
            nz_rust::types::text::parse_text_data_row(&bytes, std::slice::from_ref(&column))
                .is_err()
        );
    }
}
