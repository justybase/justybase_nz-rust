#![no_main]
//! Backend message payload parsers: text rows, DBOS descriptors and rows,
//! error/notice fields, length validation. Invariants: no panic, no
//! out-of-bounds access, no allocation driven by declared lengths, and the
//! two text-row entry points agree.
use libfuzzer_sys::fuzz_target;
use nz_rust::buffer::ReadBuffer;
use nz_rust::error::{parse_backend_error_fields, validate_protocol_length};
use nz_rust::tuple_desc::{parse_row_description, ColumnDesc};
use nz_rust::types::text::{parse_text_data_row, parse_text_data_row_into};
use nz_rust::DbosTupleDesc;
use std::io::Cursor;

const OIDS: [i32; 10] = [16, 20, 21, 23, 25, 700, 701, 1043, 1082, 1700];

fuzz_target!(|data: &[u8]| {
    if data.len() > 65_536 || data.is_empty() {
        return;
    }

    // Text path: the first byte picks the column count and types.
    let columns: Vec<ColumnDesc> = (0..(data[0] as usize % 40))
        .map(|i| ColumnDesc {
            name: format!("c{i}"),
            type_oid: OIDS[(data[0] as usize + i) % OIDS.len()],
            type_len: -1,
            type_mod: -1,
            format: 0,
        })
        .collect();
    let payload = &data[1..];
    let eager = parse_text_data_row(payload, &columns);
    let mut reused = vec![nz_rust::NzValue::Null; 3];
    let into = parse_text_data_row_into(payload, &columns, &mut reused);
    assert_eq!(eager.is_ok(), into.is_ok());
    if let Ok(values) = eager {
        assert_eq!(values, reused);
        assert_eq!(values.len(), columns.len());
    }

    // RowDescription and DBOS descriptor/row.
    let _ = parse_row_description(data);
    let split = (data[0] as usize).min(data.len());
    if let Ok(descriptor) = DbosTupleDesc::parse(&data[..split], None) {
        let _ = descriptor.parse_row(&data[split..]);
    }
    if let Ok(descriptor) = DbosTupleDesc::parse(payload, None) {
        let mut out = Vec::new();
        let _ = descriptor.parse_row_into(&data[..data.len().min(64)], &mut out);
    }

    // Error / notice fields and CommandComplete text.
    let fields = parse_backend_error_fields(data);
    assert_eq!(fields.raw, parse_backend_error_fields(data).raw);
    if let Ok(text) = std::str::from_utf8(data) {
        let _ = nz_rust::messages::parse_command_complete_rows(text);
    }

    // Length fields: every i32 prefix is validated before any allocation.
    if data.len() >= 4 {
        let declared = i32::from_be_bytes(data[..4].try_into().unwrap());
        if let Ok(length) = validate_protocol_length(declared, "fuzz", true) {
            let mut reader = Cursor::new(data[4..].to_vec());
            let mut buffer = ReadBuffer::new();
            let _ = buffer.read_bytes(&mut reader, length as usize);
        }
    }
});
