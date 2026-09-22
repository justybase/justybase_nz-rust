//! Terminal rendering: SQL editor, result grid and status/log pane.

use crate::app::{App, Focus, LogPane};
use crate::grid::{self, GUTTER, SPACING};
use crate::syntax::{HighlightSpan, Highlighter};
use nz_rust::{NzValue, ResultSet};
use ratatui::layout::{Constraint, Direction, Layout, Position, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, Paragraph};
use ratatui::Frame;

/// Rows spent on the two fixed status header lines (focus/connection and the
/// position counters) above the log tail.
const STATUS_HEADER_LINES: u16 = 2;

/// Total height of the status pane, compact vs. expanded (F2).
///
/// Sized so the pane is exactly `Borders::TOP` + the two header lines + the
/// log tail — one row too few silently clips the newest log line.
fn status_height(pane: LogPane) -> u16 {
    let log_lines = match pane {
        LogPane::Compact => 2,
        LogPane::Expanded => 12,
    };
    1 + STATUS_HEADER_LINES + log_lines
}

/// Log lines that fit in a status pane of `height` rows.
fn log_capacity(height: u16) -> usize {
    height.saturating_sub(1).saturating_sub(STATUS_HEADER_LINES) as usize
}

/// Ghost text shown while the SQL buffer is empty.
const EDITOR_HINT: &str =
    "-- SELECT * FROM <table> LIMIT 100;    F5/Ctrl+Enter runs  ·  F4 describes columns";

/// Rows available for data once the border and the two header lines are gone.
fn grid_body_height(area: Rect) -> usize {
    let inner = area.height.saturating_sub(2) as usize;
    inner.saturating_sub(inner.min(2))
}

pub fn draw(frame: &mut Frame, app: &mut App) {
    // Right column holds the optional schema browser sidebar; the editor and
    // the results share the rest. When the sidebar is hidden the layout is
    // the original single column.
    let (main_area, sidebar_area) = if app.show_browser {
        let columns = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Min(50), Constraint::Length(34)])
            .split(frame.area());
        (columns[0], Some(columns[1]))
    } else {
        (frame.area(), None)
    };

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage(35),
            Constraint::Min(5),
            Constraint::Length(status_height(app.log_pane)),
        ])
        .split(main_area);

    draw_editor(frame, chunks[0], app);
    draw_grid(frame, chunks[1], app);
    draw_status(frame, chunks[2], app);
    if let Some(area) = sidebar_area {
        draw_browser(frame, area, app);
    }
}

/// Render the schema browser sidebar: title with counts, filter echo, rows.
fn draw_browser(frame: &mut Frame, area: Rect, app: &mut App) {
    let focus_mark = if app.focus == Focus::Browser {
        "*"
    } else {
        " "
    };
    let loading = if app.loading_columns.is_some() {
        " …"
    } else {
        ""
    };
    let block = Block::default().borders(Borders::ALL).title(format!(
        "{focus_mark} Schema [F9]  {}/{} tables{loading} ",
        app.browser.row_count(),
        app.browser.table_count(),
    ));
    let inner = block.inner(area);
    frame.render_widget(block, area);

    if inner.width == 0 || inner.height == 0 {
        return;
    }
    app.browser
        .set_height(inner.height.saturating_sub(2) as usize);

    let width = inner.width as usize;
    let mut lines: Vec<Line> = Vec::new();
    // Filter line: shown first so typing is visibly echoed.
    let filter = if app.browser.filter().is_empty() {
        "  (type to filter, Enter expands)".to_string()
    } else {
        format!("  filter: ~{}~", app.browser.filter())
    };
    lines.push(Line::from(Span::styled(
        fit_text(&filter, width),
        Style::default().add_modifier(Modifier::DIM),
    )));
    lines.push(Line::from(""));

    if app.browser.is_empty() {
        let text = if app.browser_loaded {
            "no tables match".to_string()
        } else {
            "loading…".to_string()
        };
        lines.push(Line::from(Span::styled(
            text,
            Style::default().add_modifier(Modifier::DIM),
        )));
    } else {
        for row in app.browser.visible() {
            lines.push(browser_line(app, row, width));
        }
    }
    frame.render_widget(Paragraph::new(lines), inner);
}

/// One sidebar row: table marker/qualified name or a nested column.
fn browser_line(app: &App, row: usize, width: usize) -> Line<'static> {
    use crate::browser::Entry;
    let selected = row == app.browser.selected_index();
    let (marker, text, dim) = match app.browser.rows().get(row) {
        Some(Entry::Table { expanded, .. }) => {
            let name = app
                .browser
                .rows()
                .get(row)
                .and_then(|e| match e {
                    Entry::Table { table, .. } => Some(table.qualified_name()),
                    _ => None,
                })
                .unwrap_or_default();
            (if *expanded { "▾ " } else { "▸ " }, name, false)
        }
        Some(Entry::Column { column, .. }) => {
            let null_mark = if column.nullable { "" } else { " !" };
            (
                "  ",
                format!("{} {}{}", column.name, column.type_name, null_mark),
                true,
            )
        }
        None => ("  ", String::new(), true),
    };
    let mut style = Style::default();
    if dim {
        style = style.add_modifier(Modifier::DIM);
    }
    if selected {
        style = style.add_modifier(Modifier::REVERSED);
    }
    let body = fit_text(&format!("{marker}{text}"), width.saturating_sub(1));
    let used = 1 + body.chars().count();
    let mut spans = vec![Span::raw(" "), Span::styled(body, style)];
    if used < width {
        spans.push(Span::styled(
            " ".repeat(width - used),
            if selected {
                Style::default().add_modifier(Modifier::REVERSED)
            } else {
                Style::default()
            },
        ));
    }
    Line::from(spans)
}

/// Truncate `text` to `width` chars with an ellipsis (renderer-side helper).
fn fit_text(text: &str, width: usize) -> String {
    if text.chars().count() <= width {
        return text.to_string();
    }
    if width == 0 {
        return String::new();
    }
    let mut out: String = text.chars().take(width.saturating_sub(1)).collect();
    out.push('…');
    out
}

fn draw_editor(frame: &mut Frame, area: Rect, app: &mut App) {
    let focus = if app.focus == Focus::Input { "*" } else { " " };
    let block = Block::default().borders(Borders::ALL).title(format!(
        "{focus} SQL  {}  [F5/Ctrl+Enter run · Tab grid · F6 save · F2 log · Ctrl+Q quit] ",
        app.server
    ));
    let inner = block.inner(area);
    frame.render_widget(block, area);
    if inner.height == 0 || inner.width == 0 {
        return;
    }

    app.editor
        .ensure_cursor_visible(inner.height as usize, inner.width as usize);
    let width = inner.width as usize;
    let height = inner.height as usize;
    let mut highlighter = Highlighter::default();
    for row in 0..app.editor.top.min(app.editor.lines.len()) {
        let line: String = app.editor.lines[row].iter().collect();
        let _ = highlighter.highlight_line(&line);
    }
    let mut lines: Vec<Line<'static>> = (0..height)
        .filter_map(|offset| {
            let row = app.editor.top + offset;
            if row >= app.editor.lines.len() {
                return None;
            }
            let full_line: String = app.editor.lines[row].iter().collect();
            Some(clip_highlighted_line(
                highlighter.highlight_line(&full_line),
                app.editor.left,
                width,
            ))
        })
        .collect();
    if app.editor.is_empty() && !lines.is_empty() {
        lines[0] = Line::from(Span::styled(
            EDITOR_HINT,
            Style::default().add_modifier(Modifier::DIM | Modifier::ITALIC),
        ));
    }
    frame.render_widget(Paragraph::new(lines), inner);

    if app.focus == Focus::Input {
        let (row, col) = app.editor.cur;
        let x = inner.x.saturating_add((col - app.editor.left) as u16);
        let y = inner.y.saturating_add((row - app.editor.top) as u16);
        if x < inner.x + inner.width && y < inner.y + inner.height {
            frame.set_cursor_position(Position::new(x, y));
        }
    }
    draw_completion(frame, inner, app);
}

fn draw_completion(frame: &mut Frame, editor_area: Rect, app: &App) {
    if app.focus != Focus::Input || !app.completion.visible || app.completion.items.is_empty() {
        return;
    }
    let max_label = app
        .completion
        .items
        .iter()
        .map(|item| item.label.chars().count())
        .max()
        .unwrap_or(0);
    let max_detail = app
        .completion
        .items
        .iter()
        .map(|item| item.detail.chars().count())
        .max()
        .unwrap_or(0);
    let popup_width = (max_label + max_detail + 7).clamp(18, 54) as u16;
    let popup_height = (app.completion.items.len() + 2) as u16;
    let cursor_x = editor_area
        .x
        .saturating_add((app.editor.cur.1.saturating_sub(app.editor.left)) as u16);
    let cursor_y = editor_area
        .y
        .saturating_add((app.editor.cur.0.saturating_sub(app.editor.top)) as u16);
    let x = cursor_x
        .min(editor_area.right().saturating_sub(popup_width))
        .max(editor_area.x);
    let below = cursor_y.saturating_add(1);
    let y = if below.saturating_add(popup_height) <= editor_area.bottom() {
        below
    } else {
        cursor_y.saturating_sub(popup_height).max(editor_area.y)
    };
    let height = popup_height.min(editor_area.bottom().saturating_sub(y));
    let width = popup_width.min(editor_area.right().saturating_sub(x));
    if width < 4 || height < 3 {
        return;
    }
    let area = Rect::new(x, y, width, height);
    frame.render_widget(Clear, area);
    let lines = app
        .completion
        .items
        .iter()
        .enumerate()
        .map(|(index, item)| {
            let marker = match item.kind {
                crate::completion::CompletionKind::Keyword => "K",
                crate::completion::CompletionKind::Table => "T",
                crate::completion::CompletionKind::Column => "C",
            };
            let detail = format!("{marker} {}", item.detail);
            let body = format!(" {marker} {:<width$}  {}", item.label, detail, width = max_label);
            let body = fit_text(&body, width.saturating_sub(2) as usize);
            let style = if index == app.completion.selected {
                Style::default().add_modifier(Modifier::REVERSED)
            } else {
                Style::default()
            };
            Line::from(Span::styled(body, style))
        })
        .collect::<Vec<_>>();
    frame.render_widget(
        Paragraph::new(lines).block(Block::default().borders(Borders::ALL)),
        area,
    );
}

fn clip_highlighted_line(spans: Vec<HighlightSpan>, left: usize, width: usize) -> Line<'static> {
    let mut output = Vec::new();
    let mut skipped = 0usize;
    let mut remaining = width;

    for span in spans {
        if remaining == 0 {
            break;
        }
        let chars: Vec<char> = span.text.chars().collect();
        if skipped + chars.len() <= left {
            skipped += chars.len();
            continue;
        }
        let start = left.saturating_sub(skipped);
        let take = (chars.len() - start).min(remaining);
        if take > 0 {
            output.push(Span::styled(
                chars[start..start + take].iter().collect::<String>(),
                span.kind.style(),
            ));
            remaining -= take;
        }
        skipped += chars.len();
    }

    Line::from(output)
}

/// Columns to render plus the width each one gets, guaranteed to fit `avail`.
fn visible_layout(widths: &[usize], left: usize, avail: usize) -> Vec<(usize, usize)> {
    let window = grid::column_window(widths, left, avail, SPACING);
    let mut laid_out = Vec::with_capacity(window.len());
    let mut used = 0usize;
    for (pos, col) in window.iter().copied().enumerate() {
        let separator = if pos == 0 { 0 } else { SPACING };
        let mut width = widths[col];
        if used + separator + width > avail {
            width = avail.saturating_sub(used + separator);
        }
        if width == 0 {
            break;
        }
        used += separator + width;
        laid_out.push((col, width));
    }
    laid_out
}

fn draw_grid(frame: &mut Frame, area: Rect, app: &mut App) {
    let inner_width = area.width.saturating_sub(2) as usize;
    let avail = inner_width.saturating_sub(GUTTER);

    let total_rows = app.row_count();
    let total_cols = app.column_count();

    // Keep the focused cell inside the viewport before working out titles.
    let body_height = grid_body_height(area);
    let top = grid::ensure_row_visible(app.grid.top, app.grid.row, body_height, total_rows);
    app.grid.top = top;
    let left = grid::ensure_col_visible(
        app.grid.left,
        app.grid.col,
        &app.grid.widths,
        avail,
        SPACING,
    );
    app.grid.left = left;

    let laid_out = visible_layout(&app.grid.widths, left, avail);
    let first = laid_out.first().map(|(c, _)| c + 1).unwrap_or(0);
    let last = laid_out.last().map(|(c, _)| c + 1).unwrap_or(0);
    let more_left = left > 0;
    let more_right = laid_out
        .last()
        .map(|(c, _)| c + 1 < total_cols)
        .unwrap_or(false);

    let title = if total_cols == 0 {
        format!(
            " Results — set {}/{} ",
            app.set_idx + 1,
            app.set_count().max(1)
        )
    } else {
        format!(
            " Results — set {}/{} · {total_rows} row(s) · col {} {first}-{last}/{total_cols} {}{} ",
            app.set_idx + 1,
            app.set_count().max(1),
            app.grid.col + 1,
            if more_left { '◀' } else { ' ' },
            if more_right { '▶' } else { ' ' },
        )
    };

    let block = Block::default().borders(Borders::ALL).title(title);
    let inner = block.inner(area);
    frame.render_widget(block, area);
    if inner.height == 0 || inner.width == 0 {
        return;
    }

    let Some(set) = app.current_set() else {
        frame.render_widget(
            Paragraph::new("No results yet — press F5 to execute."),
            inner,
        );
        return;
    };
    if set.columns.is_empty() && set.rows.is_empty() {
        frame.render_widget(Paragraph::new("(statement returned no result set)"), inner);
        return;
    }

    // `body_height` was derived from the same area before rendering, so the
    // scroll window and the rendered rows can never disagree.
    let lines = grid_lines(GridView {
        set,
        layout: &laid_out,
        selected: app.grid.row,
        focused_col: app.grid.col,
        top: app.grid.top,
        body_height,
        pane_width: inner.width as usize,
    });
    frame.render_widget(Paragraph::new(lines), inner);
}

/// Everything needed to render one frame of the result grid.
struct GridView<'a> {
    set: &'a ResultSet,
    /// Rendered columns and their widths, already fitted to the pane.
    layout: &'a [(usize, usize)],
    /// Selected row index.
    selected: usize,
    /// Focused column index (drives horizontal scrolling, so it is underlined).
    focused_col: usize,
    /// Index of the first rendered row.
    top: usize,
    /// Number of data rows the pane can show.
    body_height: usize,
    pane_width: usize,
}

/// Build the header row, separator and the visible data rows.
fn grid_lines(view: GridView) -> Vec<Line<'static>> {
    let GridView {
        set,
        layout,
        selected,
        focused_col,
        top,
        body_height,
        pane_width,
    } = view;
    let mut lines: Vec<Line<'static>> = Vec::new();
    let gutter = " ".repeat(GUTTER);

    let mut header: Vec<Span> = vec![Span::raw(gutter.clone())];
    let mut separator = gutter.clone();
    let mut used = GUTTER;
    for (pos, (col, col_width)) in layout.iter().copied().enumerate() {
        if pos > 0 {
            header.push(Span::raw(" ".repeat(SPACING)));
            separator.push_str(&" ".repeat(SPACING));
            used += SPACING;
        }
        let name = set.columns.get(col).map(|c| c.name.as_str()).unwrap_or("");
        // The focused column is underlined: it is what Left/Right moves and
        // what drives the horizontal scroll, so it must be visible.
        let mut style = Style::default().add_modifier(Modifier::BOLD);
        if col == focused_col {
            style = style.add_modifier(Modifier::UNDERLINED);
        }
        header.push(Span::styled(grid::pad(name, col_width), style));
        separator.push_str(&"─".repeat(col_width));
        used += col_width;
    }
    // Fill any leftover cells so the separator spans the full pane.
    if used < pane_width {
        separator.push_str(&"─".repeat(pane_width - used));
    }
    lines.push(Line::from(header));
    lines.push(Line::from(Span::styled(
        separator,
        Style::default().add_modifier(Modifier::DIM),
    )));

    let gutter_empty = " ".repeat(GUTTER);
    for row in grid::row_window(set.rows.len(), top, body_height) {
        let is_selected = row == selected;
        let mut spans: Vec<Span> = vec![Span::raw(if is_selected {
            "> ".to_string()
        } else {
            gutter_empty.clone()
        })];
        let mut used = GUTTER;
        let values = set.rows[row].values();
        for (pos, (col, col_width)) in layout.iter().copied().enumerate() {
            if pos > 0 {
                spans.push(Span::raw(" ".repeat(SPACING)));
                used += SPACING;
            }
            let value = values.get(col);
            let text = value.map(|v| v.to_display_string()).unwrap_or_default();
            let mut style = Style::default();
            if value.is_none() || matches!(value, Some(NzValue::Null)) {
                style = style.add_modifier(Modifier::DIM | Modifier::ITALIC);
            }
            if is_selected {
                style = style.add_modifier(Modifier::REVERSED);
            }
            spans.push(Span::styled(grid::pad(&text, col_width), style));
            used += col_width;
        }
        // Pad the row out so the selection highlight spans the whole pane.
        if used < pane_width {
            let style = if is_selected {
                Style::default().add_modifier(Modifier::REVERSED)
            } else {
                Style::default()
            };
            spans.push(Span::styled(" ".repeat(pane_width - used), style));
        }
        lines.push(Line::from(spans));
    }
    lines
}

fn draw_status(frame: &mut Frame, area: Rect, app: &App) {
    let focus = match app.focus {
        Focus::Input => "EDITOR",
        Focus::Grid => "GRID  ",
        Focus::Browser => "BROWSER",
    };
    let elapsed = if app.running {
        match app.running_elapsed() {
            Some(d) => format!("running {} s", d.as_secs()),
            None => "running".into(),
        }
    } else {
        match app.last_elapsed {
            Some(d) => format!("{} ms", d.as_millis()),
            None => "—".into(),
        }
    };
    let mut lines: Vec<Line> = vec![
        Line::from(format!(
            " focus: {focus}   conn: {}   tx: {}   {}: {elapsed} ",
            app.server,
            app.in_transaction,
            if app.running { "state" } else { "last" }
        )),
        Line::from(if app.column_count() == 0 {
            format!(
                " set {}/{}   no result set ",
                app.set_idx + 1,
                app.set_count().max(1)
            )
        } else {
            format!(
                " set {}/{}   row {}/{}   col {}/{}   hist {}   F2 log  F4 describe ",
                app.set_idx + 1,
                app.set_count(),
                app.grid.row + 1,
                app.row_count(),
                app.grid.col + 1,
                app.column_count(),
                app.history.len(),
            )
        }),
    ];
    // Tail the log chronologically so the newest line sits at the bottom and
    // multi-line output (such as F4's column list) reads top-down.
    let start = app.log.len().saturating_sub(log_capacity(area.height));
    for message in &app.log[start..] {
        lines.push(Line::from(format!(" {message}")));
    }
    frame.render_widget(
        Paragraph::new(lines).block(Block::default().borders(Borders::TOP)),
        area,
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use nz_rust::{ColumnDesc, Row};

    fn col(name: &str, oid: i32) -> ColumnDesc {
        ColumnDesc {
            name: name.into(),
            type_oid: oid,
            type_len: -1,
            type_mod: -1,
            format: 0,
        }
    }

    /// Concatenated text of a rendered line, as the terminal would show it.
    fn text(line: &Line) -> String {
        line.spans.iter().map(|s| s.content.as_ref()).collect()
    }

    fn sample_set(rows: usize) -> ResultSet {
        let columns = vec![col("ID", 23), col("NAME", 1043)];
        let rows = (0..rows)
            .map(|i| {
                let name = if i == 2 {
                    NzValue::Null
                } else {
                    NzValue::Text(format!("name-{i}"))
                };
                Row::new(columns.clone(), vec![NzValue::Int4(i as i32), name])
            })
            .collect();
        ResultSet::new(columns, rows)
    }

    fn view<'a>(
        set: &'a ResultSet,
        layout: &'a [(usize, usize)],
        selected: usize,
        focused_col: usize,
        top: usize,
        body_height: usize,
    ) -> GridView<'a> {
        GridView {
            set,
            layout,
            selected,
            focused_col,
            top,
            body_height,
            pane_width: 80,
        }
    }

    fn used_width(layout: &[(usize, usize)]) -> usize {
        layout
            .iter()
            .enumerate()
            .map(|(i, (_, w))| if i == 0 { *w } else { SPACING + w })
            .sum()
    }

    #[test]
    fn visible_layout_always_fits_the_available_width() {
        for avail in 0..80usize {
            for left in 0..3usize {
                let layout = visible_layout(&[40, 40, 40], left, avail);
                assert!(
                    used_width(&layout) <= avail,
                    "avail={avail} left={left} used={}",
                    used_width(&layout)
                );
            }
        }
    }

    #[test]
    fn visible_layout_clamps_an_over_wide_first_column() {
        let layout = visible_layout(&[40], 0, 10);
        assert_eq!(layout, vec![(0, 10)]);
        assert!(visible_layout(&[40], 0, 0).is_empty());
    }

    #[test]
    fn visible_layout_reports_the_focused_column_offsets() {
        // Two 10-wide columns plus spacing need 22 cells; at 22 the window
        // starting at 1 holds columns 1 and 2.
        let layout = visible_layout(&[10, 10, 10], 1, 22);
        assert_eq!(
            layout.iter().map(|(c, _)| *c).collect::<Vec<_>>(),
            vec![1, 2]
        );
    }

    #[test]
    fn status_pane_exactly_fits_its_log_tail() {
        for pane in [LogPane::Compact, LogPane::Expanded] {
            let height = status_height(pane);
            assert_eq!(
                1 + STATUS_HEADER_LINES as usize + log_capacity(height),
                height as usize,
                "every row of the status pane is accounted for ({pane:?})"
            );
        }
        // Compact still shows more than nothing, expanded more than compact.
        assert!(log_capacity(status_height(LogPane::Compact)) >= 1);
        assert!(
            log_capacity(status_height(LogPane::Expanded))
                > log_capacity(status_height(LogPane::Compact))
        );
        // A pane too small for anything must not underflow.
        assert_eq!(log_capacity(0), 0);
        assert_eq!(log_capacity(2), 0);
    }

    #[test]
    fn grid_body_height_accounts_for_borders_and_header() {
        assert_eq!(grid_body_height(Rect::new(0, 0, 40, 20)), 16);
        // Tiny panes must not underflow.
        assert_eq!(
            grid_body_height(Rect::new(0, 0, 40, 2)),
            0,
            "no room inside the border"
        );
        // One interior line, which the header consumes.
        assert_eq!(grid_body_height(Rect::new(0, 0, 40, 3)), 0);
    }

    /// The scroll window is computed from `grid_body_height` before the block
    /// is rendered, while the rows come from the block's inner rect. If those
    /// two ever disagree the grid scrolls against a viewport it does not have.
    #[test]
    fn grid_body_height_matches_the_rendered_block_inner() {
        for height in 0..40u16 {
            for width in [0u16, 1, 40] {
                let area = Rect::new(0, 0, width, height);
                let block = Block::default().borders(Borders::ALL).title("result");
                let inner = block.inner(area);
                let rendered =
                    (inner.height as usize).saturating_sub((inner.height as usize).min(2));
                assert_eq!(
                    grid_body_height(area),
                    rendered,
                    "{width}x{height} viewport"
                );
            }
        }
    }

    #[test]
    fn grid_lines_renders_header_separator_and_a_row_window() {
        let set = sample_set(6);
        let widths = grid::measure(&set);
        let layout = visible_layout(&widths, 0, 80);
        let lines = grid_lines(view(&set, &layout, 3, 0, 1, 3));

        // header + separator + 3 visible rows.
        assert_eq!(lines.len(), 5);
        assert!(
            text(&lines[0]).starts_with("  ID"),
            "header: {:?}",
            text(&lines[0])
        );
        assert!(text(&lines[0]).contains("NAME"));
        assert!(text(&lines[1]).contains('─'));
        assert_eq!(
            text(&lines[1]).chars().count(),
            80,
            "separator spans the pane"
        );

        // top = 1, so the first body line is row 1, not row 0.
        assert!(text(&lines[2]).contains("name-1"));
        assert!(!text(&lines[2]).contains("name-0"));
        // Rows are exactly column-width padded, so every line is the same size.
        assert_eq!(text(&lines[2]).chars().count(), 80);

        // Every rendered line is exactly pane-wide, rows included.
        assert_eq!(text(&lines[4]).chars().count(), 80);

        // The selected row carries the marker; the others do not.
        assert!(lines[2].spans[0].content.trim().is_empty());
        assert_eq!(lines[4].spans[0].content.as_ref(), "> ");
    }

    #[test]
    fn grid_lines_dims_null_cells_and_marks_the_selection() {
        let set = sample_set(3);
        let widths = grid::measure(&set);
        let layout = visible_layout(&widths, 0, 80);
        let lines = grid_lines(view(&set, &layout, 0, 1, 0, 3));

        // Span layout is [gutter, col0, gap, col1, gap, col2, ...].
        fn cell<'a>(line: &'a Line, col: usize) -> &'a Span<'a> {
            &line.spans[1 + 2 * col]
        }

        // Row 2 is the third body line (header + separator come first) and
        // holds the NULL in column 1.
        let null_line = &lines[2 + 2];
        let null_span = cell(null_line, 1);
        assert_eq!(null_span.content.trim(), "NULL");
        assert!(null_span.style.add_modifier.contains(Modifier::DIM));
        assert!(
            !null_span.style.add_modifier.contains(Modifier::REVERSED),
            "only the selected row is reversed"
        );

        // A non-NULL value in the same column is not dimmed.
        assert!(!cell(&lines[2], 1)
            .style
            .add_modifier
            .contains(Modifier::DIM));
        // The selected first row is reversed, including its trailing padding.
        assert!(cell(&lines[2], 0)
            .style
            .add_modifier
            .contains(Modifier::REVERSED));
        assert!(lines[2].spans[0].content.as_ref() == "> ");
    }

    #[test]
    fn grid_lines_handles_an_empty_result_set() {
        let set = ResultSet::new(vec![col("A", 23)], vec![]);
        let layout = visible_layout(&grid::measure(&set), 0, 80);
        let lines = grid_lines(view(&set, &layout, 0, 0, 0, 5));
        assert_eq!(lines.len(), 2, "only the header and separator");
    }

    #[test]
    fn focused_column_header_is_underlined() {
        let set = sample_set(2);
        let layout = visible_layout(&grid::measure(&set), 0, 80);

        let lines = grid_lines(view(&set, &layout, 0, 0, 0, 2));
        let header = &lines[0];
        assert!(
            header.spans[1]
                .style
                .add_modifier
                .contains(Modifier::UNDERLINED),
            "the focused column is underlined"
        );
        assert!(!header.spans[3]
            .style
            .add_modifier
            .contains(Modifier::UNDERLINED));
        // Every header cell is still bold.
        assert!(header.spans[3].style.add_modifier.contains(Modifier::BOLD));

        // Moving the focus moves the underline.
        let lines = grid_lines(view(&set, &layout, 0, 1, 0, 2));
        let header = &lines[0];
        assert!(!header.spans[1]
            .style
            .add_modifier
            .contains(Modifier::UNDERLINED));
        assert!(header.spans[3]
            .style
            .add_modifier
            .contains(Modifier::UNDERLINED));
    }
}
