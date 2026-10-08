//! NULL-bitmap boundary qualification for the text (`D`) and binary DBOS
//! (`Y`) row formats.
//!
//! Text rows use an MSB-first bitmap where a *set* bit means "present"; DBOS
//! rows use an LSB-first bitmap starting at byte 2, indexed by the physical
//! field number, where a set bit means NULL. Off-by-one errors in either show
//! up at byte boundaries, so every column count around 8/16/32 is exercised
//! with exhaustive single-NULL and single-present patterns.

mod support;

use nz_rust::tuple_desc::{ColumnDesc, DbosTupleDesc};
use nz_rust::types::text::{parse_text_data_row, parse_text_data_row_into};
use nz_rust::NzValue;
use support::*;

const COLUMN_COUNTS: [usize; 9] = [7, 8, 9, 15, 16, 17, 31, 32, 33];

/// `true` = present (non-NULL).
fn patterns(n: usize) -> Vec<(String, Vec<bool>)> {
    let mut out = vec![
        ("all-present".to_string(), vec![true; n]),
        ("all-null".to_string(), vec![false; n]),
        (
            "alternating-null-first".to_string(),
            (0..n).map(|i| i % 2 == 1).collect(),
        ),
        (
            "alternating-present-first".to_string(),
            (0..n).map(|i| i % 2 == 0).collect(),
        ),
    ];
    for i in 0..n {
        let mut only_null = vec![true; n];
        only_null[i] = false;
        out.push((format!("only-null-at-{i}"), only_null));
        let mut only_present = vec![false; n];
        only_present[i] = true;
        out.push((format!("only-present-at-{i}"), only_present));
    }
    for (a, b) in [(6, 7), (7, 8), (14, 15), (15, 16), (30, 31), (31, 32)] {
        if b < n {
            let mut pair = vec![true; n];
            pair[a] = false;
            pair[b] = false;
            out.push((format!("null-pair-{a}-{b}"), pair));
        }
    }
    out
}

/// Every third column is VARCHAR so present cells have varying lengths.
fn text_columns(n: usize) -> Vec<ColumnDesc> {
    (0..n)
        .map(|i| ColumnDesc {
            name: format!("C{i}"),
            type_oid: if i % 3 == 2 { OID_VARCHAR } else { OID_INT4 },
            type_len: if i % 3 == 2 { -1 } else { 4 },
            type_mod: -1,
            format: 0,
        })
        .collect()
}

fn text_cell(i: usize) -> String {
    if i % 3 == 2 {
        "x".repeat(i % 5)
    } else {
        format!("{}", i as i32 * 1_000 - 7)
    }
}

fn expected_value(i: usize, present: bool) -> NzValue {
    match (present, i % 3 == 2) {
        (false, _) => NzValue::Null,
        (true, true) => NzValue::Text(text_cell(i)),
        (true, false) => NzValue::Int4(i as i32 * 1_000 - 7),
    }
}

#[test]
fn text_data_row_null_bitmap_boundaries() {
    let mut reused = Vec::new();
    for n in COLUMN_COUNTS {
        let columns = text_columns(n);
        for (name, mask) in patterns(n) {
            let cells: Vec<String> = (0..n).map(text_cell).collect();
            let encoded: Vec<Option<&[u8]>> = mask
                .iter()
                .zip(&cells)
                .map(|(present, cell)| present.then_some(cell.as_bytes()))
                .collect();
            let payload = text_row_payload(&encoded);
            assert_eq!(payload[..n.div_ceil(8)].len(), n.div_ceil(8));
            let expected: Vec<NzValue> = mask
                .iter()
                .enumerate()
                .map(|(i, present)| expected_value(i, *present))
                .collect();
            assert_eq!(
                parse_text_data_row(&payload, &columns).unwrap(),
                expected,
                "parse_text_data_row n={n} {name}"
            );
            parse_text_data_row_into(&payload, &columns, &mut reused).unwrap();
            assert_eq!(reused, expected, "parse_text_data_row_into n={n} {name}");

            // A bitmap one byte short must be rejected, never read past.
            if n.div_ceil(8) > 0 {
                let truncated = &payload[..n.div_ceil(8) - 1];
                assert!(parse_text_data_row(truncated, &columns).is_err());
            }
            // Dropping the last present cell's bytes must be detected.
            if mask.iter().any(|p| *p) {
                let short = &payload[..payload.len() - 1];
                let all_empty_tail = mask
                    .iter()
                    .enumerate()
                    .rev()
                    .find(|(_, p)| **p)
                    .is_some_and(|(i, _)| text_cell(i).is_empty());
                if !all_empty_tail {
                    assert!(
                        parse_text_data_row(short, &columns).is_err(),
                        "truncated row accepted n={n} {name}"
                    );
                }
            }
        }
    }
}

fn dbos_layout(n: usize, permutation: &str) -> DbosLayout {
    let kinds = (0..n)
        .map(|i| {
            if i % 3 == 2 {
                DbosKind::Varchar(16)
            } else {
                DbosKind::Int4
            }
        })
        .collect();
    let phys: Vec<i32> = match permutation {
        "identity" => (0..n as i32).collect(),
        "reversed" => (0..n as i32).rev().collect(),
        "rotated" => (0..n as i32).map(|i| (i + 3) % n as i32).collect(),
        other => panic!("unknown permutation {other}"),
    };
    DbosLayout {
        kinds,
        phys,
        nulls_allowed: true,
    }
}

fn dbos_cell(i: usize) -> DbosCell {
    if i % 3 == 2 {
        DbosCell::Text(text_cell(i))
    } else {
        DbosCell::Int4(i as i32 * 1_000 - 7)
    }
}

#[test]
fn dbos_row_null_bitmap_boundaries_with_physical_field_permutations() {
    for n in COLUMN_COUNTS {
        for permutation in ["identity", "reversed", "rotated"] {
            let layout = dbos_layout(n, permutation);
            let descriptor = DbosTupleDesc::parse(&layout.descriptor_payload(), None).unwrap();
            for i in 0..n {
                let phys = layout.phys[i] as usize;
                assert_eq!(descriptor.field_null_byte_offset[i], 2 + phys / 8);
                assert_eq!(descriptor.field_null_bit_mask[i], 1u8 << (phys % 8));
            }
            for (name, mask) in patterns(n) {
                let cells: Vec<Option<DbosCell>> = mask
                    .iter()
                    .enumerate()
                    .map(|(i, present)| present.then(|| dbos_cell(i)))
                    .collect();
                let row = layout.row_payload(&cells);
                let expected: Vec<NzValue> = mask
                    .iter()
                    .enumerate()
                    .map(|(i, present)| expected_value(i, *present))
                    .collect();
                assert_eq!(
                    descriptor.parse_row(&row).unwrap(),
                    expected,
                    "n={n} {permutation} {name}"
                );
                // A row too short to contain the bitmap is a protocol error.
                assert!(descriptor.parse_row(&row[..2 + n.div_ceil(8) - 1]).is_err());
            }
        }
    }
}

fn text_wire(n: usize) -> Vec<u8> {
    let names: Vec<String> = (0..n).map(|i| format!("C{i}")).collect();
    let columns: Vec<TextColumn<'_>> = (0..n)
        .map(|i| {
            if i % 3 == 2 {
                (names[i].as_str(), OID_VARCHAR, -1)
            } else {
                (names[i].as_str(), OID_INT4, 4)
            }
        })
        .collect();
    let mut wire = row_description(&columns);
    let cells: Vec<String> = (0..n).map(text_cell).collect();
    let patterns = patterns(n);
    for (_, mask) in &patterns {
        let encoded: Vec<Option<&[u8]>> = mask
            .iter()
            .zip(&cells)
            .map(|(p, c)| p.then_some(c.as_bytes()))
            .collect();
        wire.extend(text_row(&encoded));
    }
    wire.extend(command_complete(&format!("SELECT {}", patterns.len())));
    wire.extend(ready());
    wire
}

fn dbos_wire(n: usize) -> Vec<u8> {
    let mut wire = row_description(
        &(0..n)
            .map(|i| {
                if i % 3 == 2 {
                    ("V", OID_VARCHAR, -1)
                } else {
                    ("I", OID_INT4, 4)
                }
            })
            .collect::<Vec<_>>(),
    );
    let layout = dbos_layout(n, "rotated");
    wire.extend(dbos_descriptor(&layout));
    let patterns = patterns(n);
    for (_, mask) in &patterns {
        let cells: Vec<Option<DbosCell>> = mask
            .iter()
            .enumerate()
            .map(|(i, p)| p.then(|| dbos_cell(i)))
            .collect();
        wire.extend(dbos_row_frame(&layout.row_payload(&cells)));
    }
    wire.extend(command_complete(&format!("SELECT {}", patterns.len())));
    wire.extend(ready());
    wire
}

fn expected_rows(n: usize) -> Vec<Vec<NzValue>> {
    patterns(n)
        .into_iter()
        .map(|(_, mask)| {
            mask.iter()
                .enumerate()
                .map(|(i, present)| expected_value(i, *present))
                .collect()
        })
        .collect()
}

/// End-to-end through the mock backend: eager decoding (`query`), lazy rows
/// (`query_stream`, validated by `validate_text_row` / `validate_row_layout`
/// and decoded per cell on demand, last column first) and the legacy engine.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bitmap_patterns_round_trip_through_every_decode_path() {
    use futures_core::Stream;
    use std::pin::Pin;
    let server = MockServer::start(HandshakeScript::default(), |session| {
        session.serve_all(|sql| {
            let (kind, n) = sql.split_once(' ').expect("KIND N");
            let n: usize = n.parse().unwrap();
            if kind == "TEXT" {
                text_wire(n)
            } else {
                dbos_wire(n)
            }
        })
    });
    let client = nz_rust::Client::connect(&server.config()).await.unwrap();
    for n in COLUMN_COUNTS {
        let expected = expected_rows(n);
        for kind in ["TEXT", "DBOS"] {
            let sql = format!("{kind} {n}");
            let eager: Vec<Vec<NzValue>> = client
                .query(&sql, &[])
                .await
                .unwrap()
                .iter()
                .map(|row| row.try_values().unwrap().to_vec())
                .collect();
            assert_eq!(eager, expected, "eager {sql}");

            let mut stream = client.query_stream(&sql, &[]).await.unwrap();
            let mut row_index = 0;
            while let Some(row) =
                std::future::poll_fn(|cx| Pin::new(&mut stream).poll_next(cx)).await
            {
                let row = row.unwrap();
                for column in (0..n).rev() {
                    assert_eq!(
                        row.try_get_value(column).unwrap(),
                        &expected[row_index][column],
                        "lazy {sql} row {row_index} column {column}"
                    );
                }
                row_index += 1;
            }
            assert_eq!(row_index, expected.len(), "lazy {sql} row count");

            let first = client.query_one(&format!("{kind} 1"), &[]).await;
            assert!(first.is_err(), "query_one must reject multi-row results");
        }
    }
    client.close().await.unwrap();

    #[cfg(feature = "compat")]
    {
        let mut conn = nz_rust::NzConnection::connect(&server.config()).unwrap();
        for n in COLUMN_COUNTS {
            for kind in ["TEXT", "DBOS"] {
                let sql = format!("{kind} {n}");
                let result = conn.query(&sql, &[]).unwrap();
                let rows: Vec<Vec<NzValue>> = result.result_sets[0]
                    .rows
                    .iter()
                    .map(|row| row.try_values().unwrap().to_vec())
                    .collect();
                assert_eq!(rows, expected_rows(n), "legacy {sql}");
            }
        }
        conn.close();
    }
    server.assert_no_handler_panics();
}
