//! Excel export for query results (`--out report.xlsb|.xlsx`, F6).
//!
//! Maps [`nz_rust::QueryResult`] to [`spreadsheet::CellValue`] and writes one
//! worksheet per result set through the streaming writer API, so exports stay
//! constant-memory regardless of result size.

use nz_rust::{NzValue, QueryResult, ResultSet};
use spreadsheet::{CellValue, XlsbSheetOptions, XlsbWriter, XlsxWriter};
use std::path::Path;

/// True when `path` selects an Excel workbook export (`.xlsb` / `.xlsx`).
pub fn is_workbook_path(path: &str) -> bool {
    let lower = path.to_ascii_lowercase();
    lower.ends_with(".xlsb") || lower.ends_with(".xlsx") || lower.ends_with(".xlsm")
}

/// Map a driver value to an Excel cell.
///
/// Dates and timestamps keep their calendar meaning (written as OA dates);
/// everything without a faithful numeric representation becomes text.
pub fn nz_value_to_cell(value: &NzValue) -> CellValue {
    match value {
        NzValue::Null => CellValue::Empty,
        NzValue::Bool(b) => CellValue::Boolean(*b),
        NzValue::Int2(v) => CellValue::Integer(*v as i64),
        NzValue::Int4(v) => CellValue::Integer(*v as i64),
        NzValue::Int8(v) => CellValue::Integer(*v),
        NzValue::Float4(v) => CellValue::Number(*v as f64),
        NzValue::Float8(v) => CellValue::Number(*v),
        NzValue::Numeric(s) => match s.parse::<f64>() {
            Ok(n) if n.is_finite() => CellValue::Number(n),
            _ => CellValue::Text(s.clone()),
        },
        NzValue::Decimal(decimal) => match decimal.to_string().parse::<f64>() {
            Ok(number) if number.is_finite() => CellValue::Number(number),
            _ => CellValue::Text(decimal.to_string()),
        },
        NzValue::Text(s) => CellValue::Text(s.clone()),
        NzValue::Date(s) => parse_date(s)
            .map(CellValue::DateTime)
            .unwrap_or_else(|| CellValue::Text(s.clone())),
        NzValue::Timestamp(s) => parse_timestamp(s)
            .map(CellValue::DateTime)
            .unwrap_or_else(|| CellValue::Text(s.clone())),
        // Times, intervals and binary payloads have no cell-native form;
        // keep the driver's text rendering.
        NzValue::Time(s) | NzValue::Timetz(s) | NzValue::Interval(s) => CellValue::Text(s.clone()),
        NzValue::Bytea(_) => CellValue::Text(value.to_display_string()),
    }
}

fn parse_date(s: &str) -> Option<chrono::NaiveDateTime> {
    let mut parts = s.split('-');
    let y: i32 = parts.next()?.parse().ok()?;
    let m: u32 = parts.next()?.parse().ok()?;
    let d: u32 = parts.next()?.parse().ok()?;
    if parts.next().is_some() {
        return None;
    }
    chrono::NaiveDate::from_ymd_opt(y, m, d)?.and_hms_opt(0, 0, 0)
}

fn parse_timestamp(s: &str) -> Option<chrono::NaiveDateTime> {
    let (date_part, time_part) = s.split_once(' ')?;
    let mut dp = date_part.split('-');
    let y: i32 = dp.next()?.parse().ok()?;
    let m: u32 = dp.next()?.parse().ok()?;
    let d: u32 = dp.next()?.parse().ok()?;
    if dp.next().is_some() {
        return None;
    }
    let mut tp = time_part.split(':');
    let h: u32 = tp.next()?.parse().ok()?;
    let min: u32 = tp.next()?.parse().ok()?;
    let (sec, nanos) = match tp.next()? {
        s if s.contains('.') => {
            let mut sp = s.split('.');
            let sec: u32 = sp.next()?.parse().ok()?;
            let frac = sp.next()?;
            if sp.next().is_some() || !frac.bytes().all(|b| b.is_ascii_digit()) {
                return None;
            }
            let mut frac = frac.to_owned();
            frac.truncate(9);
            while frac.len() < 9 {
                frac.push('0');
            }
            (sec, frac.parse::<u32>().ok()?)
        }
        s => (s.parse::<u32>().ok()?, 0),
    };
    if tp.next().is_some() {
        return None;
    }
    let date = chrono::NaiveDate::from_ymd_opt(y, m, d)?;
    let time = chrono::NaiveTime::from_hms_nano_opt(h, min, sec, nanos)?;
    Some(chrono::NaiveDateTime::new(date, time))
}

/// Write every result set to `path` (extension selects XLSB vs XLSX).
pub fn write_query_result_to_workbook(result: &QueryResult, path: &Path) -> Result<(), String> {
    let lower = path.to_string_lossy().to_ascii_lowercase();
    if lower.ends_with(".xlsb") {
        write_xlsb(result, path)
    } else if lower.ends_with(".xlsx") || lower.ends_with(".xlsm") {
        write_xlsx(result, path)
    } else {
        Err(format!("not a workbook path: {}", path.display()))
    }
}

fn sheet_headers(set: &ResultSet) -> Vec<String> {
    set.columns.iter().map(|c| c.name.clone()).collect()
}

fn write_xlsx(result: &QueryResult, path: &Path) -> Result<(), String> {
    let mut writer = XlsxWriter::create(path).map_err(|e| e.to_string())?;
    if result.result_sets.is_empty() {
        writer.add_sheet("Sheet1", false);
        writer
            .write_sheet(vec![], Some(&[]), false)
            .map_err(|e| e.to_string())?;
    }
    for (i, set) in result.result_sets.iter().enumerate() {
        let headers = sheet_headers(set);
        writer.add_sheet(&format!("Sheet{}", i + 1), false);
        let rows: Vec<Vec<CellValue>> = set
            .rows
            .iter()
            .map(|row| row.values().iter().map(nz_value_to_cell).collect())
            .collect();
        writer
            .write_sheet(rows, Some(&headers), true)
            .map_err(|e| e.to_string())?;
    }
    writer.finalize().map_err(|e| e.to_string())?;
    Ok(())
}

fn write_xlsb(result: &QueryResult, path: &Path) -> Result<(), String> {
    let mut writer = XlsbWriter::create(path).map_err(|e| e.to_string())?;
    if result.result_sets.is_empty() {
        writer.add_sheet("Sheet1", false);
        writer
            .write_sheet(vec![], Some(&[]), false)
            .map_err(|e| e.to_string())?;
    }
    for (i, set) in result.result_sets.iter().enumerate() {
        // Streaming path: start_sheet() registers the sheet itself.
        let headers = sheet_headers(set);
        writer
            .start_sheet(
                &format!("Sheet{}", i + 1),
                headers.len(),
                Some(&headers),
                XlsbSheetOptions::new(),
            )
            .map_err(|e| e.to_string())?;
        for row in &set.rows {
            let cells: Vec<CellValue> = row.values().iter().map(nz_value_to_cell).collect();
            writer.write_row(&cells).map_err(|e| e.to_string())?;
        }
        writer.end_sheet().map_err(|e| e.to_string())?;
    }
    writer.finalize().map_err(|e| e.to_string())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use nz_rust::{ColumnDesc, Row};

    fn col(name: &str) -> ColumnDesc {
        ColumnDesc {
            name: name.to_string(),
            type_oid: 25,
            type_len: -1,
            type_mod: -1,
            format: 0,
        }
    }

    fn sample_result() -> QueryResult {
        let columns = vec![col("ID"), col("WHEN")];
        let rows = vec![Row::new(
            columns.clone(),
            vec![
                NzValue::Int4(7),
                NzValue::Timestamp("2024-01-02 03:04:05".to_string()),
            ],
        )];
        QueryResult {
            result_sets: vec![ResultSet::new(columns, rows)],
            rows_affected: 1,
            notices: vec![],
        }
    }

    fn unique_temp(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("nz_export_test_{}_{name}", std::process::id()))
    }

    #[test]
    fn value_mapping_keeps_types() {
        assert_eq!(nz_value_to_cell(&NzValue::Null), CellValue::Empty);
        assert_eq!(nz_value_to_cell(&NzValue::Int8(-5)), CellValue::Integer(-5));
        assert_eq!(
            nz_value_to_cell(&NzValue::Float8(1.5)),
            CellValue::Number(1.5)
        );
        assert!(matches!(
            nz_value_to_cell(&NzValue::Date("2024-02-29".to_string())),
            CellValue::DateTime(_)
        ));
        assert_eq!(
            nz_value_to_cell(&NzValue::Date("not-a-date".to_string())),
            CellValue::Text("not-a-date".to_string())
        );
        assert_eq!(
            nz_value_to_cell(&NzValue::Numeric("1e400".to_string())),
            CellValue::Text("1e400".to_string())
        );
    }

    #[test]
    fn workbook_roundtrip_xlsx_and_xlsb() {
        for ext in ["xlsx", "xlsb"] {
            let path = unique_temp(&format!("r.{ext}"));
            let _ = std::fs::remove_file(&path);
            write_query_result_to_workbook(&sample_result(), &path).unwrap();
            assert!(path.exists(), "export file missing for {ext}");
            let mut reader = spreadsheet::create_reader(&path).unwrap();
            reader.open(&path, true).unwrap();
            assert!(reader.read().unwrap());
            assert_eq!(reader.get_value(0), CellValue::Text("ID".into()));
            assert!(reader.read().unwrap());
            assert_eq!(reader.get_value(0), CellValue::Number(7.0));
            assert!(matches!(reader.get_value(1), CellValue::DateTime(_)));
            let _ = std::fs::remove_file(&path);
        }
    }
}
