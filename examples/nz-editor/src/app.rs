//! Application state for the Netezza TUI: SQL buffer, result grid cursor,
//! query history and the message log.

use crate::browser::{Activation, Browser};
use crate::completion::{self, CompletionState};
use crate::grid;
use crate::worker::{Job, WorkerEvent};
use nz_rust::{NzConnectionConfig, QueryResult, ResultSet};
use std::collections::HashSet;
use std::sync::mpsc;
use std::time::{Duration, Instant};

/// Which pane receives keystrokes.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Focus {
    Input,
    Grid,
    Browser,
}

/// A multi-line SQL buffer with its own cursor and scroll offsets.
///
/// Lines are stored as `Vec<char>` so cursor movement and slicing are
/// character-based; using `String` byte offsets would corrupt non-ASCII SQL.
#[derive(Debug, Default, Clone)]
pub struct Editor {
    pub lines: Vec<Vec<char>>,
    pub cur: (usize, usize),
    pub top: usize,
    pub left: usize,
}

impl Editor {
    pub fn new() -> Self {
        Editor {
            lines: vec![Vec::new()],
            cur: (0, 0),
            top: 0,
            left: 0,
        }
    }

    /// Create a buffer from text for parser/editor unit tests.
    #[cfg(test)]
    pub fn from_text(text: &str) -> Self {
        let mut editor = Self::new();
        editor.set_text(text);
        editor
    }

    /// The whole buffer as text.
    pub fn text(&self) -> String {
        self.lines
            .iter()
            .map(|l| l.iter().collect::<String>())
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// Replace the buffer and place the cursor at the end.
    pub fn set_text(&mut self, text: &str) {
        self.lines = text.split('\n').map(|l| l.chars().collect()).collect();
        if self.lines.is_empty() {
            self.lines.push(Vec::new());
        }
        let last = self.lines.len() - 1;
        self.cur = (last, self.lines[last].len());
        self.top = 0;
        self.left = 0;
    }

    pub fn is_empty(&self) -> bool {
        self.lines.iter().all(|l| l.is_empty())
    }

    pub fn line_len(&self, row: usize) -> usize {
        self.lines.get(row).map(|l| l.len()).unwrap_or(0)
    }

    pub fn insert_char(&mut self, ch: char) {
        let (r, c) = self.cur;
        self.lines[r].insert(c, ch);
        self.cur.1 += 1;
    }

    pub fn newline(&mut self) {
        let (r, c) = self.cur;
        let tail = self.lines[r].split_off(c);
        self.lines.insert(r + 1, tail);
        self.cur = (r + 1, 0);
    }

    pub fn backspace(&mut self) {
        let (r, c) = self.cur;
        if c > 0 {
            self.lines[r].remove(c - 1);
            self.cur.1 -= 1;
        } else if r > 0 {
            let line = self.lines.remove(r);
            let prev_len = self.lines[r - 1].len();
            self.lines[r - 1].extend(line);
            self.cur = (r - 1, prev_len);
        }
    }

    pub fn delete(&mut self) {
        let (r, c) = self.cur;
        if c < self.lines[r].len() {
            self.lines[r].remove(c);
        } else if r + 1 < self.lines.len() {
            let next = self.lines.remove(r + 1);
            self.lines[r].extend(next);
        }
    }

    /// Insert `text` at the cursor, separated from surrounding text by a
    /// single space when needed (used by the schema-browser insert).
    pub fn insert_word(&mut self, text: &str) {
        let (r, c) = self.cur;
        let needs_left = c > 0
            && self.lines[r]
                .get(c - 1)
                .map(|ch| !ch.is_whitespace())
                .unwrap_or(false);
        let needs_right = !text.is_empty()
            && self.lines[r]
                .get(c)
                .map(|ch| !ch.is_whitespace())
                .unwrap_or(false);
        let mut inserted = 0usize;
        if needs_left {
            self.lines[r].insert(c, ' ');
            inserted += 1;
        }
        for ch in text.chars() {
            self.lines[r].insert(c + inserted, ch);
            inserted += 1;
        }
        if needs_right {
            self.lines[r].insert(c + inserted, ' ');
        }
        self.cur.1 = c + inserted;
    }

    /// Replace a range on one line and place the cursor after the inserted
    /// text. Completion ranges are always confined to the current line.
    pub fn replace_range(
        &mut self,
        start: (usize, usize),
        end: (usize, usize),
        text: &str,
    ) {
        if start.0 != end.0 || start.0 >= self.lines.len() {
            return;
        }
        let line = &mut self.lines[start.0];
        let from = start.1.min(line.len());
        let to = end.1.min(line.len()).max(from);
        line.splice(from..to, text.chars());
        self.cur = (start.0, from + text.chars().count());
    }

    pub fn move_left(&mut self) {
        let (r, c) = self.cur;
        if c > 0 {
            self.cur.1 -= 1;
        } else if r > 0 {
            self.cur = (r - 1, self.line_len(r - 1));
        }
    }

    pub fn move_right(&mut self) {
        let (r, c) = self.cur;
        if c < self.line_len(r) {
            self.cur.1 += 1;
        } else if r + 1 < self.lines.len() {
            self.cur = (r + 1, 0);
        }
    }

    pub fn move_up(&mut self) {
        if self.cur.0 > 0 {
            self.cur.0 -= 1;
            self.cur.1 = self.cur.1.min(self.line_len(self.cur.0));
        }
    }

    pub fn move_down(&mut self) {
        if self.cur.0 + 1 < self.lines.len() {
            self.cur.0 += 1;
            self.cur.1 = self.cur.1.min(self.line_len(self.cur.0));
        }
    }

    pub fn home(&mut self) {
        self.cur.1 = 0;
    }

    pub fn end(&mut self) {
        self.cur.1 = self.line_len(self.cur.0);
    }

    /// Scroll just enough that the cursor stays inside a `height` × `width`
    /// viewport. Called every frame before rendering.
    pub fn ensure_cursor_visible(&mut self, height: usize, width: usize) {
        let height = height.max(1);
        let width = width.max(1);
        let (row, col) = self.cur;
        if row < self.top {
            self.top = row;
        } else if row >= self.top + height {
            self.top = row + 1 - height;
        }
        if col < self.left {
            self.left = col;
        } else if col >= self.left + width {
            self.left = col + 1 - width;
        }
    }

    /// The `width` characters of line `row` that start at the horizontal
    /// scroll offset. Used by tests; the renderer uses its own span clipping.
    #[allow(dead_code)]
    pub fn visible_line(&self, row: usize, width: usize) -> String {
        let Some(line) = self.lines.get(row) else {
            return String::new();
        };
        let start = self.left.min(line.len());
        let end = (start + width).min(line.len());
        line[start..end].iter().collect()
    }
}

/// Cursor/scroll state of the result grid.
#[derive(Debug, Default, Clone)]
pub struct GridState {
    /// Selected row index.
    pub row: usize,
    /// Index of the first rendered row.
    pub top: usize,
    /// Index of the first rendered column.
    pub left: usize,
    /// Focused column index (drives horizontal scrolling and the header hint).
    pub col: usize,
    /// Rendered width of every column, measured from the data.
    pub widths: Vec<usize>,
}

impl GridState {
    /// Re-measure and rewind to the top-left of a freshly received set.
    pub fn reset(&mut self, set: Option<&ResultSet>) {
        self.row = 0;
        self.top = 0;
        self.left = 0;
        self.col = 0;
        self.widths = set.map(grid::measure).unwrap_or_default();
    }
}

/// How much of the message log to show.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogPane {
    Compact,
    Expanded,
}

pub struct App {
    pub server: String,
    pub editor: Editor,
    pub result: Option<QueryResult>,
    pub set_idx: usize,
    pub grid: GridState,
    pub focus: Focus,
    pub log: Vec<String>,
    pub log_pane: LogPane,
    pub last_elapsed: Option<Duration>,
    pub history: Vec<String>,
    pub history_pos: Option<usize>,
    pub out_path: String,
    pub should_quit: bool,
    /// Background worker that owns the connection.
    pub jobs: mpsc::Sender<Job>,
    pub events: mpsc::Receiver<WorkerEvent>,
    /// A query is in flight; the UI stays live and F8 can cancel it.
    pub running: bool,
    /// Monotonic stamp set when the running query started, for the elapsed
    /// readout while waiting.
    pub started: Option<Instant>,
    /// Sidebar schema browser.
    pub browser: Browser,
    /// Whether the sidebar pane is rendered at all.
    pub show_browser: bool,
    /// True once the first table load has been kicked off.
    pub browser_loaded: bool,
    /// Table index whose column load is in flight (for the spinner/state).
    pub loading_columns: Option<usize>,
    /// Column loads queued by the browser or completion, deduplicated by
    /// catalog table index.
    pending_column_loads: HashSet<usize>,
    /// Current editor completion popup and replacement range.
    pub completion: CompletionState,
    /// Backend key data for the out-of-band cancel (the worker owns the
    /// connection itself).
    cancel_config: NzConnectionConfig,
    cancel_pid: i32,
    cancel_key: i32,
    /// Session transaction state as of the last finished batch.
    pub in_transaction: bool,
    /// SQL queued for the running query, moved into history on success.
    pending_sql: Option<String>,
}

impl App {
    pub fn new(
        server: String,
        out_path: String,
        jobs: mpsc::Sender<Job>,
        events: mpsc::Receiver<WorkerEvent>,
        cancel: (NzConnectionConfig, i32, i32),
    ) -> Self {
        let (config, pid, key) = cancel;
        App {
            server,
            editor: Editor::new(),
            result: None,
            set_idx: 0,
            grid: GridState::default(),
            focus: Focus::Input,
            log: vec!["Ready — type SQL and press F5 (or Alt+Enter).".into()],
            log_pane: LogPane::Compact,
            last_elapsed: None,
            history: Vec::new(),
            history_pos: None,
            out_path,
            should_quit: false,
            jobs,
            events,
            running: false,
            started: None,
            browser: Browser::new(),
            show_browser: false,
            browser_loaded: false,
            loading_columns: None,
            pending_column_loads: HashSet::new(),
            completion: CompletionState::default(),
            cancel_config: config,
            cancel_pid: pid,
            cancel_key: key,
            in_transaction: false,
            pending_sql: None,
        }
    }

    pub fn push_log(&mut self, message: impl Into<String>) {
        self.log.push(message.into());
        if self.log.len() > 500 {
            self.log.remove(0);
        }
    }

    pub fn current_set(&self) -> Option<&ResultSet> {
        self.result
            .as_ref()
            .and_then(|r| r.result_sets.get(self.set_idx))
    }

    pub fn set_count(&self) -> usize {
        self.result
            .as_ref()
            .map(|r| r.result_sets.len())
            .unwrap_or(0)
    }

    pub fn row_count(&self) -> usize {
        self.current_set().map(|s| s.rows.len()).unwrap_or(0)
    }

    pub fn column_count(&self) -> usize {
        self.current_set().map(|s| s.columns.len()).unwrap_or(0)
    }

    /// Re-derive grid geometry for whichever result set is showing.
    fn sync_grid(&mut self) {
        let set = self
            .result
            .as_ref()
            .and_then(|r| r.result_sets.get(self.set_idx));
        self.grid.reset(set);
    }

    /// Queue the buffer for execution on the worker thread. Non-blocking:
    /// the UI keeps ticking and [`App::drain_events`] finishes the job.
    pub fn execute(&mut self) {
        self.completion.dismiss();
        let sql = self.editor.text().trim().to_string();
        if sql.is_empty() {
            self.push_log("Nothing to execute.");
            return;
        }
        if self.running {
            self.push_log("A query is already running — F8 cancels it.");
            return;
        }
        self.push_log(format!(">>> {}", first_line(&sql)));
        if self.jobs.send(Job::Query(sql.clone())).is_err() {
            self.push_log("ERROR: worker is gone");
            return;
        }
        self.pending_sql = Some(sql);
        self.running = true;
        self.started = Some(Instant::now());
    }

    /// Send the out-of-band cancel packet for the running query (F8).
    ///
    /// The cancel request opens its own connection, so it works while the
    /// worker is blocked reading the query response.
    pub fn cancel(&mut self) {
        if !self.running {
            self.push_log("Nothing to cancel.");
            return;
        }
        match nz_rust::cancel::send_cancel(&self.cancel_config, self.cancel_pid, self.cancel_key) {
            Ok(()) => self.push_log("Cancel request sent — waiting for the server…"),
            Err(e) => self.push_log(format!("Cancel failed: {e}")),
        }
    }

    /// Collect everything the worker has reported and update the UI state.
    /// Called every event-loop tick; never blocks.
    pub fn drain_events(&mut self) {
        while let Ok(event) = self.events.try_recv() {
            match event {
                WorkerEvent::QueryDone {
                    result,
                    elapsed,
                    in_transaction,
                } => {
                    self.running = false;
                    self.last_elapsed = Some(elapsed);
                    self.in_transaction = in_transaction;
                    match result {
                        Ok(result) => {
                            for notice in &result.notices {
                                self.push_log(format!("NOTICE: {notice}"));
                            }
                            let sets = result.result_sets.len();
                            let rows = result.row_count();
                            self.push_log(format!(
                                "OK — {sets} set(s), {rows} row(s), {} row(s) affected, {} ms",
                                result.rows_affected,
                                elapsed.as_millis()
                            ));
                            self.result = Some(result);
                            self.set_idx = 0;
                            self.sync_grid();
                            self.focus = Focus::Grid;
                            self.remember();
                        }
                        Err(e) => self.push_log(format!("ERROR: {e}")),
                    }
                }
                WorkerEvent::Tables(tables) => match tables {
                    Ok(tables) => {
                        let count = tables.len();
                        self.browser
                            .set_tables(tables.iter().map(|t| t.clone().into()).collect());
                        self.browser_loaded = true;
                        self.push_log(format!("Schema: {count} table(s)"));
                        self.refresh_completion(false);
                    }
                    Err(e) => self.push_log(format!("Schema load failed: {e}")),
                },
                WorkerEvent::Columns {
                    table_index,
                    result,
                } => {
                    self.pending_column_loads.remove(&table_index);
                    self.loading_columns = self.pending_column_loads.iter().next().copied();
                    match result {
                        Ok(cols) => {
                            self.browser
                                .set_columns(table_index, cols.iter().map(Into::into).collect());
                        }
                        Err(e) => {
                            self.push_log(format!("Columns load failed: {e}"));
                            // Keep the table expanded with an empty list so the
                            // failure is visible.
                            self.browser.set_columns(table_index, Vec::new());
                        }
                    }
                    self.refresh_completion(false);
                }
            }
        }
    }

    /// Record the queued statement in the history once its result landed
    /// (called from [`App::drain_events`]); skips immediate repeats.
    fn remember(&mut self) {
        let Some(sql) = self.pending_sql.take() else {
            return;
        };
        if self.history.last() != Some(&sql) {
            self.history.push(sql);
            if self.history.len() > 200 {
                self.history.remove(0);
            }
        }
        self.history_pos = None;
    }

    /// Recall the previous history entry into the editor (Ctrl+P).
    pub fn history_prev(&mut self) {
        if self.history.is_empty() {
            return;
        }
        let pos = match self.history_pos {
            None => self.history.len() - 1,
            Some(0) => 0,
            Some(p) => p - 1,
        };
        self.history_pos = Some(pos);
        let sql = self.history[pos].clone();
        self.editor.set_text(&sql);
        self.completion.dismiss();
        self.focus = Focus::Input;
    }

    /// Move forward through history (Ctrl+N); past the end restores a blank
    /// buffer.
    pub fn history_next(&mut self) {
        let Some(pos) = self.history_pos else {
            return;
        };
        if pos + 1 >= self.history.len() {
            self.history_pos = None;
            self.editor = Editor::new();
            self.completion.dismiss();
            self.focus = Focus::Input;
            return;
        }
        self.history_pos = Some(pos + 1);
        let sql = self.history[pos + 1].clone();
        self.editor.set_text(&sql);
        self.completion.dismiss();
        self.focus = Focus::Input;
    }

    /// Switch result set (`[` / `]`), wrapping around.
    pub fn step_set(&mut self, forward: bool) {
        let total = self.set_count();
        if total == 0 {
            return;
        }
        self.set_idx = if forward {
            (self.set_idx + 1) % total
        } else {
            (self.set_idx + total - 1) % total
        };
        self.sync_grid();
    }

    /// Move the row selection by `delta`, clamped to the data.
    pub fn move_row(&mut self, delta: i64) {
        let rows = self.row_count();
        if rows == 0 {
            return;
        }
        let next = (self.grid.row as i64 + delta).clamp(0, rows as i64 - 1);
        self.grid.row = next as usize;
    }

    pub fn move_col(&mut self, delta: i64) {
        let cols = self.column_count();
        if cols == 0 {
            return;
        }
        let next = (self.grid.col as i64 + delta).clamp(0, cols as i64 - 1);
        self.grid.col = next as usize;
    }

    pub fn toggle_focus(&mut self) {
        self.cycle_focus();
    }

    pub fn toggle_log_pane(&mut self) {
        self.log_pane = match self.log_pane {
            LogPane::Compact => LogPane::Expanded,
            LogPane::Expanded => LogPane::Compact,
        };
    }

    /// Dump the current result set's column metadata into the log.
    pub fn describe_columns(&mut self) {
        let Some(set) = self.current_set() else {
            self.push_log("No result set to describe.");
            return;
        };
        if set.columns.is_empty() {
            self.push_log("Result set has no columns.");
            return;
        }
        let nullability = set.nullability.clone();
        let lines: Vec<String> = set
            .columns
            .iter()
            .enumerate()
            .map(|(i, c)| {
                let nullable = match &nullability {
                    Some(n) => n.get(i).copied().unwrap_or(true),
                    None => true,
                };
                format!(
                    "  {}. {} {}{}",
                    i + 1,
                    c.name,
                    c.declared_type_name(),
                    if nullable { "" } else { " NOT NULL" }
                )
            })
            .collect();
        let count = lines.len();
        self.push_log(format!("Columns ({count}):"));
        for line in lines {
            self.push_log(line);
        }
        self.log_pane = LogPane::Expanded;
    }

    /// Write the current result to [`App::out_path`]: an Excel workbook for
    /// `.xlsb` / `.xlsx` paths, tab-separated text otherwise.
    pub fn save_results(&mut self) {
        let Some(result) = self.result.as_ref() else {
            self.push_log("Nothing to save.");
            return;
        };
        let path = self.out_path.clone();
        if crate::export_excel::is_workbook_path(&path) {
            match crate::export_excel::write_query_result_to_workbook(
                result,
                std::path::Path::new(&path),
            ) {
                Ok(()) => {
                    let bytes = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
                    self.push_log(format!("Saved {bytes} bytes to {path}"));
                }
                Err(e) => self.push_log(format!("Save failed: {path}: {e}")),
            }
            return;
        }
        let rendered = nz_rust::result_to_text(result, true);
        match std::fs::write(&path, rendered) {
            Ok(()) => {
                let bytes = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
                self.push_log(format!("Saved {bytes} bytes to {path}"));
            }
            Err(e) => self.push_log(format!("Save failed: {path}: {e}")),
        }
    }

    // -- schema browser ------------------------------------------------------

    /// Kick off the one-time table list load (called on startup).
    pub fn load_schema(&mut self) {
        if self.browser_loaded {
            return;
        }
        if self.jobs.send(Job::LoadTables).is_err() {
            self.push_log("ERROR: worker is gone");
            return;
        }
        self.browser_loaded = true;
    }

    /// Show/hide the sidebar. Showing it focuses it; hiding returns to the
    /// editor.
    pub fn toggle_browser(&mut self) {
        self.completion.dismiss();
        self.show_browser = !self.show_browser;
        if self.show_browser {
            self.load_schema();
            self.focus = Focus::Browser;
        } else if self.focus == Focus::Browser {
            self.focus = Focus::Input;
        }
    }

    /// Focus cycling: Input → Grid → Browser → Input.
    pub fn cycle_focus(&mut self) {
        self.focus = match self.focus {
            Focus::Input => Focus::Grid,
            Focus::Grid => Focus::Browser,
            Focus::Browser => Focus::Input,
        };
    }

    /// Enter on the browser: toggle/expand a table or insert a name into the
    /// editor at the cursor.
    pub fn browser_activate(&mut self) {
        match self.browser.activate() {
            Activation::Toggle(i) => {
                self.browser.toggle(i);
                if self.loading_columns == Some(i) {
                    self.loading_columns = None;
                }
            }
            Activation::LoadAndExpand(i) => {
                let Some((schema, table)) = self.browser.qualified_parts(i) else {
                    return;
                };
                // Mark expanded immediately (empty) so a second Enter does not
                // double-queue the load; the event fills the columns in.
                self.browser.set_columns(i, Vec::new());
                self.queue_columns(i, schema, table);
            }
            Activation::InsertColumn {
                table_index: _,
                name,
            } => {
                self.editor.insert_word(&name);
                self.focus = Focus::Input;
                self.completion.dismiss();
            }
            Activation::None => {}
        }
    }

    /// Type `text` into the browser's filter box, or backspace (Backspace).
    pub fn browser_filter_key(&mut self, ch: char) {
        let current = self.browser.filter().to_string();
        self.browser.set_filter(format!("{current}{ch}"));
    }

    pub fn browser_filter_backspace(&mut self) {
        let current = self.browser.filter().to_string();
        self.browser.set_filter(
            current
                .chars()
                .take(current.chars().count().saturating_sub(1))
                .collect::<String>(),
        );
    }

    /// Recompute completion candidates after an editor event and request any
    /// referenced table columns that have not been loaded yet.
    pub fn refresh_completion(&mut self, force: bool) {
        let analysis = completion::analyze(&self.editor, &self.browser, force);
        for table_index in &analysis.missing_tables {
            let Some((schema, table)) = self.browser.qualified_parts(*table_index) else {
                continue;
            };
            self.queue_columns(*table_index, schema, table);
        }
        self.completion.apply(analysis);
    }

    /// Accept the currently selected completion item.
    pub fn accept_completion(&mut self) -> bool {
        let Some(item) = self.completion.selected_item().cloned() else {
            return false;
        };
        let start = self.completion.replace_start;
        let end = self.completion.replace_end;
        self.editor.replace_range(start, end, &item.insert_text);
        self.completion.dismiss();
        true
    }

    pub fn dismiss_completion(&mut self) {
        self.completion.dismiss();
    }

    fn queue_columns(&mut self, table_index: usize, schema: String, table: String) {
        if !self.pending_column_loads.insert(table_index) {
            self.loading_columns = Some(table_index);
            return;
        }
        self.loading_columns = Some(table_index);
        if self
            .jobs
            .send(Job::LoadColumns {
                schema,
                table,
                table_index,
            })
            .is_err()
        {
            self.pending_column_loads.remove(&table_index);
            self.push_log("ERROR: worker is gone");
        }
    }

    /// True while a query is in flight; the status bar shows elapsed time.
    pub fn running_elapsed(&self) -> Option<Duration> {
        self.started.map(|s| s.elapsed())
    }
}

/// First line of a statement, truncated for the log.
pub fn first_line(sql: &str) -> String {
    let line = sql.lines().next().unwrap_or("").trim();
    if line.chars().count() > 70 {
        format!("{}…", line.chars().take(70).collect::<String>())
    } else {
        line.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn editor_text_and_set_text_round_trip() {
        let mut e = Editor::new();
        assert!(e.is_empty());
        e.set_text("SELECT 1\nFROM t");
        assert_eq!(e.text(), "SELECT 1\nFROM t");
        assert_eq!(e.cur, (1, 6), "cursor parks at the end of the buffer");
        // Trailing newline yields a final empty line, like a text editor.
        e.set_text("a\n");
        assert_eq!(e.lines.len(), 2);
        assert_eq!(e.cur, (1, 0));
    }

    #[test]
    fn editor_editing_operations() {
        let mut e = Editor::new();
        for ch in "SELECT".chars() {
            e.insert_char(ch);
        }
        assert_eq!(e.text(), "SELECT");
        e.insert_char(' ');
        e.insert_char('1');
        e.insert_char('2');
        assert_eq!(e.text(), "SELECT 12");
        e.backspace();
        assert_eq!(e.text(), "SELECT 1");
        e.home();
        e.delete();
        assert_eq!(e.text(), "ELECT 1");
        e.end();
        e.newline();
        e.insert_char('X');
        assert_eq!(e.text(), "ELECT 1\nX");
        e.backspace();
        assert_eq!(e.text(), "ELECT 1\n");
        assert_eq!(e.cur, (1, 0));
        // Backspace at column 0 joins the previous line.
        e.backspace();
        assert_eq!(e.text(), "ELECT 1");
        assert_eq!(e.cur, (0, 7));
    }

    #[test]
    fn backspace_at_buffer_start_is_a_no_op() {
        let mut e = Editor::new();
        e.backspace();
        e.delete();
        e.move_up();
        e.move_left();
        assert_eq!(e.text(), "");
        assert_eq!(e.cur, (0, 0));
    }

    #[test]
    fn delete_joins_next_line_at_end_of_line() {
        let mut e = Editor::new();
        e.set_text("ab\ncd");
        e.cur = (0, 2);
        e.delete();
        assert_eq!(e.text(), "abcd");
    }

    #[test]
    fn arrow_movement_crosses_line_boundaries() {
        let mut e = Editor::new();
        e.set_text("abc\nx");
        e.cur = (0, 0);
        e.move_left();
        assert_eq!(e.cur, (0, 0), "stays put at the very start");
        e.move_right();
        e.move_right();
        e.move_right();
        e.move_right();
        assert_eq!(e.cur, (1, 0), "right at end of line crosses to the next");
        e.end();
        assert_eq!(e.cur, (1, 1));
        e.move_down();
        assert_eq!(e.cur, (1, 1), "stays put at the very end");
        e.move_up();
        assert_eq!(e.cur, (0, 1), "keeps the column across lines");

        // The column clamps when the line above is shorter.
        e.set_text("x\nabc");
        e.cur = (1, 3);
        e.move_up();
        assert_eq!(e.cur, (0, 1), "column clamps to the shorter line");
    }

    #[test]
    fn cursor_visibility_scrolls_vertically_and_horizontally() {
        let mut e = Editor::new();
        let text = (0..20)
            .map(|i| format!("line {i}"))
            .collect::<Vec<_>>()
            .join("\n");
        e.set_text(&text);
        e.cur = (15, 3);
        e.ensure_cursor_visible(5, 80);
        assert_eq!(e.top, 11, "keeps the cursor on the last visible row");
        assert!(e.cur.0 >= e.top && e.cur.0 < e.top + 5);
        e.cur = (0, 0);
        e.ensure_cursor_visible(5, 80);
        assert_eq!(e.top, 0);
        // Horizontal scroll for a long single line.
        e.set_text(&"x".repeat(200));
        e.cur = (0, 150);
        e.ensure_cursor_visible(5, 40);
        assert_eq!(e.left, 111);
        assert_eq!(e.visible_line(0, 40).chars().count(), 40);
    }

    #[test]
    fn visible_line_handles_offsets_past_the_end() {
        let mut e = Editor::new();
        e.set_text("short");
        e.left = 99;
        assert_eq!(e.visible_line(0, 10), "");
        assert_eq!(e.visible_line(99, 10), "");
    }

    #[test]
    fn visible_line_respects_multibyte_characters() {
        let mut e = Editor::new();
        e.set_text("zażółć gęślą jaźń");
        assert_eq!(e.visible_line(0, 6).chars().count(), 6);
        e.left = 2;
        assert_eq!(e.visible_line(0, 3), "żół");
    }

    #[test]
    fn first_line_truncates() {
        assert_eq!(first_line("SELECT 1\nFROM t"), "SELECT 1");
        assert_eq!(first_line("  spaced  "), "spaced");
        let long = "x".repeat(100);
        let f = first_line(&long);
        assert!(f.ends_with('…'));
        assert_eq!(f.chars().count(), 71);
    }

    #[test]
    fn grid_state_reset_clears_offsets() {
        let mut g = GridState {
            row: 7,
            top: 3,
            left: 2,
            col: 4,
            widths: vec![9],
        };
        g.reset(None);
        assert_eq!((g.row, g.top, g.left, g.col), (0, 0, 0, 0));
        assert!(g.widths.is_empty());
    }
}
