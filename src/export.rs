// Copyright 2026 Krzysztof Duśko.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Plain-text result export.
//!
//! Renders a [`QueryResult`] into a human-readable, tab-separated text block
//! (optionally with a column header) and writes it to any [`std::io::Write`].
//! This is the engine behind the `dump_to_txt` example and is deliberately
//! dependency-free so it can be unit-tested without an appliance.
//!
//! Format (one block per result set):
//!
//! ```text
//! # result set 1 (3 rows)
//! ID<TAB>NAME
//! 1<TAB>alice
//! 2<TAB>NULL
//! ```
//!
//! Tabs, carriage returns and newlines inside a value are escaped as `\t`,
//! `\r` and `\n` so the file keeps one record per line. SQL `NULL` is written
//! as the literal `NULL` (the Node/C# grid convention).

use crate::connection::QueryResult;
use crate::types::value::NzValue;
use std::io::{self, Write};

/// Escape a single rendered value for a one-record-per-line text file.
fn escape_cell(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '\t' => out.push_str("\\t"),
            '\r' => out.push_str("\\r"),
            '\n' => out.push_str("\\n"),
            '\\' => out.push_str("\\\\"),
            other => out.push(other),
        }
    }
    out
}

/// Render a value using the driver's display form, escaping control bytes.
pub fn render_value(value: &NzValue) -> String {
    escape_cell(&value.to_display_string())
}

/// Render the whole [`QueryResult`] to a string.
///
/// Every result set is emitted with a `# result set N (M rows)` banner; a
/// header row is written when the set carries column metadata.
pub fn result_to_text(result: &QueryResult, include_header: bool) -> String {
    let mut out = String::new();
    for (set_no, set) in result.result_sets.iter().enumerate() {
        out.push_str(&format!(
            "# result set {} ({} rows)\n",
            set_no + 1,
            set.rows.len()
        ));
        if include_header && !set.columns.is_empty() {
            let header = set
                .columns
                .iter()
                .map(|c| escape_cell(&c.name))
                .collect::<Vec<_>>()
                .join("\t");
            out.push_str(&header);
            out.push('\n');
        }
        for row in &set.rows {
            let line = row
                .values()
                .iter()
                .map(render_value)
                .collect::<Vec<_>>()
                .join("\t");
            out.push_str(&line);
            out.push('\n');
        }
    }
    if !result.notices.is_empty() {
        out.push_str("# notices\n");
        for notice in &result.notices {
            out.push_str(&format!("# {notice}\n"));
        }
    }
    out
}

/// Write the rendered [`QueryResult`] to `writer`.
pub fn write_result_to_txt<W: Write>(
    writer: &mut W,
    result: &QueryResult,
    include_header: bool,
) -> io::Result<()> {
    writer.write_all(result_to_text(result, include_header).as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::{ResultSet, Row};
    use crate::tuple_desc::ColumnDesc;

    fn col(name: &str, oid: i32) -> ColumnDesc {
        ColumnDesc {
            name: name.into(),
            type_oid: oid,
            type_len: -1,
            type_mod: -1,
            format: 0,
        }
    }

    fn sample() -> QueryResult {
        let columns = vec![col("ID", 23), col("NAME", 1043)];
        let rows = vec![
            Row::new(
                columns.clone(),
                vec![NzValue::Int4(1), NzValue::Text("alice".into())],
            ),
            Row::new(columns.clone(), vec![NzValue::Int4(2), NzValue::Null]),
        ];
        QueryResult {
            result_sets: vec![ResultSet::new(columns, rows)],
            rows_affected: 2,
            notices: vec![],
        }
    }

    #[test]
    fn renders_header_and_rows() {
        let text = result_to_text(&sample(), true);
        assert!(text.contains("# result set 1 (2 rows)"));
        assert!(text.contains("ID\tNAME\n"));
        assert!(text.contains("1\talice\n"));
        assert!(text.contains("2\tNULL\n"));
    }

    #[test]
    fn escapes_control_bytes_in_values() {
        let columns = vec![col("V", 1043)];
        let rows = vec![Row::new(
            columns.clone(),
            vec![NzValue::Text("a\tb\nc".into())],
        )];
        let result = QueryResult {
            result_sets: vec![ResultSet::new(columns, rows)],
            rows_affected: 1,
            notices: vec![],
        };
        let text = result_to_text(&result, false);
        assert!(text.contains("a\\tb\\nc\n"));
        // Exactly one data line — the embedded newline was escaped.
        assert_eq!(text.lines().count(), 2);
    }

    #[test]
    fn writes_to_any_writer() {
        let mut buf: Vec<u8> = Vec::new();
        write_result_to_txt(&mut buf, &sample(), true).unwrap();
        let text = String::from_utf8(buf).unwrap();
        assert!(text.contains("alice"));
    }
}
