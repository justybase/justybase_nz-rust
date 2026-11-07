//! Character encoding across the binary (DBOS) and text result paths.
//!
//! Netezza stores `CHAR`/`VARCHAR` in the database encoding (commonly Latin-1)
//! but sends `NCHAR`/`NVARCHAR` as UTF-8. The decoder must not conflate the
//! two: a stored `ó` is one byte (`0xF3`) on the binary path, while an
//! `NVARCHAR` `ó` is `0xC3 0xB3`.

use nz_rust::messages::nz_type;
use nz_rust::types::value::NzValue;
use nz_rust::{CharEncoding, ColumnDesc, DbosTupleDesc};

/// Layout used by every case below:
///
/// `[2 bitmap][i32 ID][CHAR(4)][VARCHAR][NVARCHAR]`
fn descriptor(encoding: CharEncoding) -> DbosTupleDesc {
    DbosTupleDesc {
        nulls_allowed: 0,
        num_fields: 4,
        num_fixed_fields: 2,
        num_varying_fields: 2,
        fixed_fields_size: 10,
        field_type: vec![
            nz_type::NZ_TYPE_INT,
            nz_type::NZ_TYPE_CHAR,
            nz_type::NZ_TYPE_VARCHAR,
            nz_type::NZ_TYPE_NVARCHAR,
        ],
        field_size: vec![4, 4, 64, 64],
        field_true_size: vec![4, 4, 64, 64],
        field_offset: vec![2, 6, 0, 1],
        field_fixed_size: vec![4, 4, 0, 0],
        field_null_byte_offset: vec![0; 4],
        field_null_bit_mask: vec![0; 4],
        char_varchar_encoding: encoding,
        ..Default::default()
    }
}

fn row(char_bytes: &[u8], varchar_bytes: &[u8], nvarchar_text: &str) -> Vec<u8> {
    assert_eq!(char_bytes.len(), 4);
    let mut row = vec![0, 0];
    row.extend_from_slice(&7i32.to_le_bytes());
    row.extend_from_slice(char_bytes);

    let mut push_varying = |bytes: &[u8]| {
        row.extend_from_slice(&((bytes.len() + 2) as u16).to_le_bytes());
        row.extend_from_slice(bytes);
        if !(bytes.len() + 2).is_multiple_of(2) {
            row.push(0);
        }
    };
    push_varying(varchar_bytes);
    push_varying(nvarchar_text.as_bytes());
    row
}

#[test]
fn latin1_char_and_varchar_decode_from_a_dbos_row() {
    // "ó   " in Latin-1 — one byte for ó in CHAR.
    let bytes = row(
        &[0xF3, b' ', b' ', b' '],
        &[b'c', b'a', b'f', 0xE9, b' ', 0xF3],
        "Zażółć gęślą jaźń",
    );
    let values = descriptor(CharEncoding::Latin1).parse_row(&bytes).unwrap();

    assert_eq!(values[0], NzValue::Int4(7));
    assert_eq!(values[1], NzValue::Text("ó".into()));
    assert_eq!(values[2], NzValue::Text("café ó".into()));
    assert_eq!(values[3], NzValue::Text("Zażółć gęślą jaźń".into()));
}

#[test]
fn utf8_encoding_decodes_utf8_char_and_varchar() {
    let bytes = row("ó  ".as_bytes(), "Zażółć".as_bytes(), "Zażółć gęślą jaźń");
    let values = descriptor(CharEncoding::Utf8).parse_row(&bytes).unwrap();

    assert_eq!(values[1], NzValue::Text("ó".into()));
    assert_eq!(values[2], NzValue::Text("Zażółć".into()));
    assert_eq!(values[3], NzValue::Text("Zażółć gęślą jaźń".into()));
}

#[test]
fn utf8_encoding_rejects_latin1_style_bytes_in_varchar() {
    let bytes = row(b"X   ", &[0xF3], "ó");
    let error = descriptor(CharEncoding::Utf8)
        .parse_row(&bytes)
        .unwrap_err();
    let message = error.to_string();
    assert!(message.contains("invalid UTF-8 field 2"), "{message}");
}

#[test]
fn nchar_and_nvarchar_stay_utf8_regardless_of_the_char_encoding() {
    // Latin-1 must not be applied to NVARCHAR/NCHAR: a UTF-8 ó has two bytes
    // and would otherwise decode as "Ã³".
    let bytes = row(b"X   ", b"plain", "ó");
    let values = descriptor(CharEncoding::Latin1).parse_row(&bytes).unwrap();
    assert_eq!(values[3], NzValue::Text("ó".into()));

    let mut desc = descriptor(CharEncoding::Latin1);
    desc.field_type[2] = nz_type::NZ_TYPE_NCHAR;
    desc.field_size[2] = 4;
    desc.num_varying_fields = 2;
    let bytes = row(b"X   ", b"plain", "Zażółć");
    let values = desc.parse_row(&bytes).unwrap();
    assert_eq!(values[2], NzValue::Text("plain".into()));
    assert_eq!(values[3], NzValue::Text("Zażółć".into()));
}

#[test]
fn latin1_decoding_is_lossless_for_every_high_byte() {
    let mut expected = String::new();
    let mut bytes = Vec::new();
    for byte in 0x80u8..=0xFF {
        expected.push(byte as char);
        bytes.push(byte);
    }
    let bytes = row(&[0xF3, 0xE9, 0xBF, 0xB3], &bytes, "ó");
    let values = descriptor(CharEncoding::Latin1).parse_row(&bytes).unwrap();
    assert_eq!(values[1], NzValue::Text("óé¿³".into()));
    assert_eq!(values[2], NzValue::Text(expected));
}

#[test]
fn text_path_values_remain_utf8() {
    // The text protocol already renders values in the negotiated session
    // encoding; applying Latin-1 there would corrupt UTF-8.
    let columns = vec![
        ColumnDesc {
            name: "V".into(),
            type_oid: 1043,
            type_len: -1,
            type_mod: -1,
            format: 0,
        },
        ColumnDesc {
            name: "N".into(),
            type_oid: 2530,
            type_len: -1,
            type_mod: -1,
            format: 0,
        },
    ];
    let mut payload = vec![0b1100_0000];
    for value in ["ó", "Zażółć gęślą jaźń"] {
        let bytes = value.as_bytes();
        payload.extend_from_slice(&((bytes.len() + 4) as i32).to_be_bytes());
        payload.extend_from_slice(bytes);
    }
    let values = nz_rust::types::text::parse_text_data_row(&payload, &columns).unwrap();
    assert_eq!(values[0], NzValue::Text("ó".into()));
    assert_eq!(values[1], NzValue::Text("Zażółć gęślą jaźń".into()));
}
