#![no_main]
use libfuzzer_sys::fuzz_target;
use nz_rust::{parse_connection_string, DbosTupleDesc};

fuzz_target!(|data: &[u8]| {
    if data.len() > 65_536 {
        return;
    }
    let _ = nz_rust::tuple_desc::parse_row_description(data);
    let split = data.first().copied().unwrap_or(0) as usize;
    let split = split.min(data.len());
    if let Ok(desc) = DbosTupleDesc::parse(&data[..split], None) {
        let _ = desc.parse_row(&data[split..]);
    }
    if let Ok(text) = std::str::from_utf8(data) {
        let _ = parse_connection_string(text);
        let _ = text.parse::<nz_rust::NzNumeric>();
        for oid in [16, 20, 23, 26, 700, 701, 1700] {
            let _ = nz_rust::types::text::try_parse_text_value(text, oid, -1);
        }
    }
    if data.len() >= 12 {
        let word = |start| i32::from_le_bytes(data[start..start + 4].try_into().unwrap());
        let _ = nz_rust::types::numeric::get_cs_numeric(&data[12..], word(0), word(4), word(8));
    }
});
