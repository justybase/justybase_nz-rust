//! Deterministic replay of the fuzz-target bodies (`fuzz/fuzz_targets/*.rs`)
//! in normal test runs: valid frames with every byte mutated, seeded random
//! inputs, and a directory of committed seeds. Keeps the fuzz invariants (no
//! panic, bounded work, deterministic results) enforced in CI without a
//! nightly toolchain. The harness functions mirror the fuzz targets; keep them
//! in sync when changing either.

mod support;

use nz_rust::buffer::ReadBuffer;
use nz_rust::error::{parse_backend_error_fields, validate_protocol_length};
use nz_rust::messages::{parse_transaction_state, split_statements};
use nz_rust::params::substitute_parameters;
use nz_rust::tuple_desc::{parse_row_description, ColumnDesc};
use nz_rust::types::text::{parse_text_data_row, parse_text_data_row_into};
use nz_rust::{substitute_bound_parameters, DbosTupleDesc, NzParameter, NzValue};
use std::io::Cursor;
use support::*;

const OIDS: [i32; 10] = [16, 20, 21, 23, 25, 700, 701, 1043, 1082, 1700];

fn wire_frames(data: &[u8]) {
    if data.len() > 65_536 || data.is_empty() {
        return;
    }
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
    let mut reused = vec![NzValue::Null; 3];
    let into = parse_text_data_row_into(payload, &columns, &mut reused);
    assert_eq!(eager.is_ok(), into.is_ok());
    if let Ok(values) = eager {
        assert_eq!(values, reused);
        assert_eq!(values.len(), columns.len());
    }
    let _ = parse_row_description(data);
    let split = (data[0] as usize).min(data.len());
    if let Ok(descriptor) = DbosTupleDesc::parse(&data[..split], None) {
        let _ = descriptor.parse_row(&data[split..]);
    }
    if let Ok(descriptor) = DbosTupleDesc::parse(payload, None) {
        let mut out = Vec::new();
        let _ = descriptor.parse_row_into(&data[..data.len().min(64)], &mut out);
    }
    let fields = parse_backend_error_fields(data);
    assert_eq!(fields.raw, parse_backend_error_fields(data).raw);
    if let Ok(text) = std::str::from_utf8(data) {
        let _ = nz_rust::messages::parse_command_complete_rows(text);
    }
    if data.len() >= 4 {
        let declared = i32::from_be_bytes(data[..4].try_into().unwrap());
        if let Ok(length) = validate_protocol_length(declared, "fuzz", true) {
            let mut reader = Cursor::new(data[4..].to_vec());
            let mut buffer = ReadBuffer::new();
            let _ = buffer.read_bytes(&mut reader, length as usize);
        }
    }
}

fn sql_lexer(data: &[u8]) {
    if data.len() > 16_384 {
        return;
    }
    let Ok(sql) = std::str::from_utf8(data) else {
        return;
    };
    assert_eq!(split_statements(sql), split_statements(sql));
    assert_eq!(parse_transaction_state(sql), parse_transaction_state(sql));
    let values = [
        NzValue::Null,
        NzValue::Int4(-7),
        NzValue::Text(sql.chars().rev().collect()),
        NzValue::Text("it's \\ \"q\" ; -- /* $$".into()),
        NzValue::Bool(true),
    ];
    for count in 0..=values.len() {
        let positional = substitute_parameters(sql, &values[..count]);
        assert_eq!(positional, substitute_parameters(sql, &values[..count]));
        if let Ok(rendered) = &positional {
            assert!(rendered.len() <= sql.len() + count * (2 * sql.len() + 64) + 64);
        }
    }
    let mut bound: Vec<NzParameter> = values
        .iter()
        .cloned()
        .map(NzParameter::positional)
        .collect();
    bound.push(NzParameter::named("a", NzValue::Int4(1)));
    for count in 0..=bound.len() {
        assert_eq!(
            substitute_bound_parameters(sql, &bound[..count]),
            substitute_bound_parameters(sql, &bound[..count])
        );
    }
}

fn run_all(data: &[u8]) {
    wire_frames(data);
    sql_lexer(data);
}

/// Valid samples of every payload kind the targets parse.
fn valid_samples() -> Vec<Vec<u8>> {
    let layout = DbosLayout {
        kinds: vec![DbosKind::Int4, DbosKind::Varchar(16), DbosKind::Int4],
        phys: vec![0, 1, 2],
        nulls_allowed: true,
    };
    let mut samples = Vec::new();
    // First byte = column count for the text-row harness.
    let mut text = vec![5u8];
    text.extend(text_row_payload(&[
        Some(b"1"),
        None,
        Some(b"abc"),
        Some(b"2024-01-01"),
        None,
    ]));
    samples.push(text);
    samples.push(row_description_payload(&[
        ("A", OID_INT4, 4),
        ("B", OID_VARCHAR, -1),
    ]));
    samples.push(layout.descriptor_payload());
    samples.push(layout.row_payload(&[
        Some(DbosCell::Int4(1)),
        Some(DbosCell::Text("hi".into())),
        None,
    ]));
    samples.push(b"SERROR\0C42000\0Msyntax\0\0".to_vec());
    samples.push(b"SELECT 1 /* c */; INSERT INTO t VALUES ('a''b', $1, $2) -- x\n".to_vec());
    samples.push(b"BEGIN; COMMIT; $$ x; y $$; E'\\'; z'".to_vec());
    samples.push(b"INSERT 0 5\0".to_vec());
    samples
}

#[test]
fn mutated_valid_frames_never_break_the_parsers() {
    for sample in valid_samples() {
        run_all(&sample);
        for index in 0..sample.len() {
            for replacement in [0x00u8, 0xff, 0x7f, 0x80, sample[index] ^ 1] {
                let mut mutated = sample.clone();
                mutated[index] = replacement;
                run_all(&mutated);
            }
            // Truncation and splice at every offset.
            run_all(&sample[..index]);
            let mut spliced = sample[..index].to_vec();
            spliced.extend_from_slice(&sample[index..].repeat(2));
            run_all(&spliced);
        }
    }
}

#[test]
fn seeded_random_inputs_never_break_the_parsers() {
    let mut seed = 0xf022_5eed;
    for _ in 0..3_000 {
        let length = (next_random(&mut seed) % 300) as usize;
        let data: Vec<u8> = (0..length).map(|_| next_random(&mut seed) as u8).collect();
        run_all(&data);
        // Biased towards structure: small bytes and printable ASCII.
        let structured: Vec<u8> = (0..length)
            .map(|_| match next_random(&mut seed) % 4 {
                0 => 0,
                1 => (next_random(&mut seed) % 8) as u8,
                2 => {
                    const SYNTAX: &[u8] = b"'$;-/*\\\" \nabcSELECT";
                    SYNTAX[(next_random(&mut seed) % SYNTAX.len() as u64) as usize]
                }
                _ => next_random(&mut seed) as u8,
            })
            .collect();
        run_all(&structured);
    }
}

#[test]
fn committed_seed_files_never_break_the_parsers() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/fuzz_seeds");
    let mut count = 0;
    for entry in std::fs::read_dir(&dir).unwrap_or_else(|e| panic!("{}: {e}", dir.display())) {
        let path = entry.unwrap().path();
        if path.is_file() {
            run_all(&std::fs::read(&path).unwrap());
            count += 1;
        }
    }
    assert!(count > 0, "no seeds found in {}", dir.display());
}
