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
    let mut out = Vec::new();
    write_result_to_txt(&mut out, result, include_header)
        .expect("result_to_text requires decodable rows");
    String::from_utf8(out).expect("text export is UTF-8")
}

fn write_escaped<W: Write>(writer: &mut W, value: &str) -> io::Result<()> {
    let mut start = 0;
    for (index, byte) in value.bytes().enumerate() {
        let replacement: &[u8] = match byte {
            b'\t' => b"\\t",
            b'\r' => b"\\r",
            b'\n' => b"\\n",
            b'\\' => b"\\\\",
            _ => continue,
        };
        writer.write_all(&value.as_bytes()[start..index])?;
        writer.write_all(replacement)?;
        start = index + 1;
    }
    writer.write_all(&value.as_bytes()[start..])
}

fn write_cell<W: Write>(writer: &mut W, value: &NzValue) -> io::Result<()> {
    match value {
        NzValue::Null => writer.write_all(b"NULL"),
        NzValue::Bool(value) => write!(writer, "{value}"),
        NzValue::Int2(value) => write!(writer, "{value}"),
        NzValue::Int4(value) => write!(writer, "{value}"),
        NzValue::Int8(value) => write!(writer, "{value}"),
        NzValue::Float4(value) => write!(writer, "{}", *value as f64),
        NzValue::Float8(value) => write!(writer, "{value}"),
        NzValue::Decimal(value) => write!(writer, "{value}"),
        NzValue::Numeric(text)
        | NzValue::Text(text)
        | NzValue::Date(text)
        | NzValue::Time(text)
        | NzValue::Timetz(text)
        | NzValue::Timestamp(text)
        | NzValue::Interval(text) => write_escaped(writer, text),
        NzValue::Bytea(bytes) => {
            writer.write_all(b"0x")?;
            for byte in bytes {
                write!(writer, "{byte:02x}")?;
            }
            Ok(())
        }
    }
}

/// Direct export sink for `NzConnection::execute_stream`, retaining no rows.
/// Wrap file writers in `BufWriter`. Banners omit the row count until completion.
pub struct TextExportSink<W> {
    writer: W,
    include_header: bool,
}
impl<W: Write> TextExportSink<W> {
    pub fn new(writer: W, include_header: bool) -> Self {
        Self {
            writer,
            include_header,
        }
    }
    pub fn into_inner(self) -> W {
        self.writer
    }
}
impl<W: Write> crate::QueryStreamSink for TextExportSink<W> {
    fn on_columns(
        &mut self,
        index: usize,
        columns: &[crate::ColumnDesc],
        _: Option<&[bool]>,
    ) -> crate::NzResult<()> {
        writeln!(self.writer, "# result set {}", index + 1)?;
        if self.include_header && !columns.is_empty() {
            for (index, column) in columns.iter().enumerate() {
                if index != 0 {
                    self.writer.write_all(b"\t")?;
                }
                write_escaped(&mut self.writer, &column.name)?;
            }
            self.writer.write_all(b"\n")?;
        }
        Ok(())
    }
    fn on_row(&mut self, index: usize, row: crate::Row) -> crate::NzResult<()> {
        self.on_values(index, row.columns(), row.try_values()?)
    }
    fn on_values(
        &mut self,
        _: usize,
        _: &[crate::ColumnDesc],
        values: &[NzValue],
    ) -> crate::NzResult<()> {
        for (index, value) in values.iter().enumerate() {
            if index != 0 {
                self.writer.write_all(b"\t")?;
            }
            write_cell(&mut self.writer, value)?;
        }
        self.writer.write_all(b"\n")?;
        Ok(())
    }
    fn on_notice(&mut self, message: &str) -> crate::NzResult<()> {
        self.writer.write_all(b"# ")?;
        write_escaped(&mut self.writer, message)?;
        self.writer.write_all(b"\n")?;
        Ok(())
    }
}

/// Write each row directly, without allocating a complete result string.
/// Decode errors are returned as `InvalidData`; writer errors are preserved.
pub fn write_result_to_txt<W: Write>(
    writer: &mut W,
    result: &QueryResult,
    include_header: bool,
) -> io::Result<()> {
    for (index, set) in result.result_sets.iter().enumerate() {
        writeln!(
            writer,
            "# result set {} ({} rows)",
            index + 1,
            set.rows.len()
        )?;
        if include_header && !set.columns.is_empty() {
            for (index, column) in set.columns.iter().enumerate() {
                if index != 0 {
                    writer.write_all(b"\t")?;
                }
                write_escaped(writer, &column.name)?;
            }
            writer.write_all(b"\n")?;
        }
        for row in &set.rows {
            for index in 0..row.len() {
                if index != 0 {
                    writer.write_all(b"\t")?;
                }
                let value = row
                    .try_get_value(index)
                    .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
                write_cell(writer, value)?;
            }
            writer.write_all(b"\n")?;
        }
    }
    if !result.notices.is_empty() {
        writer.write_all(b"# notices\n")?;
        for notice in &result.notices {
            writer.write_all(b"# ")?;
            write_escaped(writer, notice)?;
            writer.write_all(b"\n")?;
        }
    }
    Ok(())
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
    fn streaming_sink_writes_rows_and_preserves_writer_errors() {
        use crate::QueryStreamSink;
        let result = sample();
        let set = &result.result_sets[0];
        let mut sink = TextExportSink::new(Vec::new(), true);
        sink.on_columns(0, &set.columns, None).unwrap();
        for row in &set.rows {
            sink.on_row(0, row.clone()).unwrap();
        }
        assert_eq!(
            String::from_utf8(sink.into_inner()).unwrap(),
            "# result set 1\nID\tNAME\n1\talice\n2\tNULL\n"
        );
        struct BrokenWriter;
        impl Write for BrokenWriter {
            fn write(&mut self, _: &[u8]) -> io::Result<usize> {
                Err(io::Error::new(io::ErrorKind::BrokenPipe, "test writer"))
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        assert!(TextExportSink::new(BrokenWriter, false)
            .on_columns(0, &[], None)
            .is_err());
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
