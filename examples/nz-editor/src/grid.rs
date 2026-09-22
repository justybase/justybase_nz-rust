//! Pure viewport math for the result grid.
//!
//! Everything here is a plain function over sizes and indices: no `App`, no
//! `ratatui`, no I/O. That keeps the fiddly scroll-window arithmetic testable
//! without a terminal, which is where scroll bugs actually live.
//!
//! Widths count `char`s, not terminal cells. For the SQL, ASCII and Latin-1
//! data this editor is built for the two are the same; East Asian wide glyphs
//! occupy two cells and would therefore be mis-measured. Handling that needs a
//! Unicode width table, which this crate deliberately does not depend on.

use nz_rust::ResultSet;
use std::ops::Range;

/// Blank columns written between two grid cells.
pub const SPACING: usize = 2;
/// Left gutter holding the current-row marker (`> ` / `  `).
pub const GUTTER: usize = 2;
/// Smallest rendered column width, so tiny values stay readable.
pub const MIN_COL_WIDTH: usize = 4;
/// Largest rendered column width, so one wide column cannot eat the screen.
pub const MAX_COL_WIDTH: usize = 40;
/// Rows sampled when measuring column widths (keeps huge results responsive).
pub const MEASURE_SAMPLE: usize = 500;

/// Truncate `text` to `width` display cells, adding `…` when it had to shorten.
pub fn fit(text: &str, width: usize) -> String {
    if width == 0 {
        return String::new();
    }
    if text.chars().count() <= width {
        return text.to_string();
    }
    if width == 1 {
        return "…".to_string();
    }
    let mut out: String = text.chars().take(width - 1).collect();
    out.push('…');
    out
}

/// Fit `text` to exactly `width` cells, padding with spaces on the right.
pub fn pad(text: &str, width: usize) -> String {
    let fitted = fit(text, width);
    let len = fitted.chars().count();
    if len >= width {
        fitted
    } else {
        format!("{fitted}{}", " ".repeat(width - len))
    }
}

/// Widths (in cells) for every column of `set`, clamped to the sane range.
///
/// Only the first [`MEASURE_SAMPLE`] rows are inspected: measuring a
/// million-row result must not stall the UI, and the sample is representative
/// in practice.
pub fn measure(set: &ResultSet) -> Vec<usize> {
    let mut widths: Vec<usize> = set.columns.iter().map(|c| c.name.chars().count()).collect();
    for row in set.rows.iter().take(MEASURE_SAMPLE) {
        for (i, value) in row.values().iter().enumerate() {
            if i >= widths.len() {
                break;
            }
            let len = value.to_display_string().chars().count();
            if len > widths[i] {
                widths[i] = len;
            }
        }
    }
    for width in &mut widths {
        *width = (*width).clamp(MIN_COL_WIDTH, MAX_COL_WIDTH);
    }
    widths
}

/// Indices of the columns visible when the viewport is `left` columns in and
/// `avail` cells wide.
///
/// The first column is always included even when it is wider than `avail`, so
/// the grid never renders completely empty.
pub fn column_window(widths: &[usize], left: usize, avail: usize, spacing: usize) -> Vec<usize> {
    if widths.is_empty() || avail == 0 {
        return Vec::new();
    }
    let left = left.min(widths.len() - 1);
    let mut out = Vec::new();
    let mut used = 0usize;
    for (i, width) in widths.iter().enumerate().skip(left) {
        let extra = if out.is_empty() {
            *width
        } else {
            spacing + *width
        };
        if !out.is_empty() && used + extra > avail {
            break;
        }
        used += extra;
        out.push(i);
    }
    out
}

/// Move `left` just far enough that column `sel_col` is inside the window.
pub fn ensure_col_visible(
    left: usize,
    sel_col: usize,
    widths: &[usize],
    avail: usize,
    spacing: usize,
) -> usize {
    if widths.is_empty() {
        return 0;
    }
    let sel_col = sel_col.min(widths.len() - 1);
    let mut left = left.min(sel_col);
    while left < sel_col && !column_window(widths, left, avail, spacing).contains(&sel_col) {
        left += 1;
    }
    left
}

/// The slice of rows to render for a viewport `height` rows tall starting at
/// `top`.
pub fn row_window(total: usize, top: usize, height: usize) -> Range<usize> {
    let start = top.min(total);
    let end = (start + height).min(total);
    start..end
}

/// Clamp `top` so that row `sel` is visible inside a `height`-row viewport.
///
/// Also keeps `top` inside the valid range for `total`, so shortening a result
/// set can never leave the viewport parked past the end of the data.
pub fn ensure_row_visible(top: usize, sel: usize, height: usize, total: usize) -> usize {
    if height == 0 || total == 0 {
        return 0;
    }
    let max_top = total.saturating_sub(height);
    let mut top = top.min(max_top);
    if sel < top {
        top = sel;
    } else if sel >= top + height {
        top = sel + 1 - height;
    }
    top.min(max_top)
}

#[cfg(test)]
mod tests {
    use super::*;
    use nz_rust::{ColumnDesc, NzValue, Row};

    fn col(name: &str, oid: i32) -> ColumnDesc {
        ColumnDesc {
            name: name.into(),
            type_oid: oid,
            type_len: -1,
            type_mod: -1,
            format: 0,
        }
    }

    fn set(columns: Vec<ColumnDesc>, rows: Vec<Vec<NzValue>>) -> ResultSet {
        let rows = rows
            .into_iter()
            .map(|values| Row::new(columns.clone(), values))
            .collect();
        ResultSet::new(columns, rows)
    }

    #[test]
    fn fit_truncates_with_ellipsis() {
        assert_eq!(fit("abc", 5), "abc");
        assert_eq!(fit("abcdef", 4), "abc…");
        assert_eq!(fit("abcdef", 1), "…");
        assert_eq!(fit("abcdef", 0), "");
        // Multi-byte characters must be counted as one cell, not as bytes.
        assert_eq!(fit("żółć", 4), "żółć");
        assert_eq!(fit("żółć", 2), "ż…");
    }

    #[test]
    fn pad_pads_and_truncates_to_exact_width() {
        assert_eq!(pad("ab", 4), "ab  ");
        assert_eq!(pad("abcdef", 4), "abc…");
        assert_eq!(pad("abcd", 4), "abcd");
        assert_eq!(pad("żółć", 5).chars().count(), 5);
    }

    #[test]
    fn measure_uses_headers_and_data_clamped() {
        let columns = vec![col("ID", 23), col("COMMENT", 1043)];
        let rows = vec![
            vec![NzValue::Int4(123456), NzValue::Text("hello".into())],
            vec![NzValue::Int4(1), NzValue::Null],
            vec![NzValue::Int4(2), NzValue::Text("x".repeat(100))],
        ];
        let widths = measure(&set(columns, rows));
        assert_eq!(widths[0], 6, "widened to the longest ID");
        assert_eq!(widths[1], MAX_COL_WIDTH, "clamped to the maximum");
    }

    #[test]
    fn measure_never_returns_zero_width() {
        let widths = measure(&set(vec![col("", 23)], vec![vec![NzValue::Null]]));
        assert_eq!(widths, vec![MIN_COL_WIDTH]);
    }

    #[test]
    fn column_window_walks_right_and_always_shows_one() {
        let widths = vec![10, 10, 10, 10];
        // Two 10-cell columns plus 2 cells of spacing do not fit in 15.
        assert_eq!(column_window(&widths, 0, 15, SPACING), vec![0]);
        assert_eq!(column_window(&widths, 0, 22, SPACING), vec![0, 1]);
        assert_eq!(column_window(&widths, 1, 22, SPACING), vec![1, 2]);
        assert_eq!(column_window(&widths, 3, 22, SPACING), vec![3]);
        // An over-wide single column is still rendered.
        assert_eq!(column_window(&[40], 0, 5, SPACING), vec![0]);
        assert!(column_window(&[], 0, 80, SPACING).is_empty());
        assert!(column_window(&widths, 0, 0, SPACING).is_empty());
    }

    #[test]
    fn ensure_col_visible_scrolls_right_then_stops() {
        let widths = vec![10, 10, 10, 10];
        // Column 3 is off-screen from offset 0; scroll just enough.
        assert_eq!(ensure_col_visible(0, 3, &widths, 22, SPACING), 2);
        // Already visible: offset is preserved.
        assert_eq!(ensure_col_visible(0, 1, &widths, 22, SPACING), 0);
        // Selecting to the left snaps the offset back.
        assert_eq!(ensure_col_visible(3, 0, &widths, 22, SPACING), 0);
        // Out-of-range selection is clamped instead of panicking.
        assert_eq!(ensure_col_visible(0, 99, &widths, 22, SPACING), 2);
    }

    #[test]
    fn row_window_clamps_both_ends() {
        assert_eq!(row_window(10, 0, 3), 0..3);
        assert_eq!(row_window(10, 8, 3), 8..10);
        assert_eq!(row_window(10, 50, 3), 10..10);
        assert_eq!(row_window(0, 0, 5), 0..0);
    }

    #[test]
    fn ensure_row_visible_follows_selection() {
        assert_eq!(ensure_row_visible(0, 5, 3, 10), 3);
        assert_eq!(ensure_row_visible(5, 2, 3, 10), 2);
        assert_eq!(ensure_row_visible(0, 1, 3, 10), 0);
        // Stale offsets past the end are pulled back in.
        assert_eq!(ensure_row_visible(99, 9, 3, 10), 7);
        assert_eq!(ensure_row_visible(5, 0, 3, 0), 0);
        assert_eq!(ensure_row_visible(5, 0, 0, 10), 0);
    }

    #[test]
    fn scroll_round_trip_keeps_selection_visible() {
        let widths = vec![8; 20];
        let mut left = 0usize;
        for sel in 0..20 {
            left = ensure_col_visible(left, sel, &widths, 30, SPACING);
            assert!(
                column_window(&widths, left, 30, SPACING).contains(&sel),
                "column {sel} must be visible at offset {left}"
            );
        }
        let mut top = 0usize;
        for sel in 0..200 {
            top = ensure_row_visible(top, sel, 10, 200);
            assert!(
                row_window(200, top, 10).contains(&sel),
                "row {sel} must be visible"
            );
        }
    }
}
