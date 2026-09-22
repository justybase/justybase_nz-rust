//! Schema browser sidebar state: tables, their columns, a name filter and
//! the selection.
//!
//! Like [`crate::grid`], this module is deliberately free of ratatui, I/O and
//! the connection: the caller loads catalog data on a worker thread and feeds
//! the results in, so every rule below (filtering, expand/collapse, row
//! addressing, the visible window) is unit-testable without an appliance.

use crate::worker::TableEntryLite;
use nz_rust::{NzColumnInfo, NzTableInfo};
use std::ops::Range;

/// One row of the sidebar.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Entry {
    /// A table row; `index` is the table's position in the **full** table list
    /// (not the filtered view), so activations survive filtering.
    Table {
        index: usize,
        table: TableEntry,
        expanded: bool,
    },
    /// A column of the table above it; `table_index` points into the table
    /// list, not the rendered rows.
    Column {
        table_index: usize,
        column: ColumnEntry,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableEntry {
    pub schema: String,
    pub name: String,
}

impl TableEntry {
    /// Two-part qualified name (`SCHEMA.TABLE`) — verified to be accepted by
    /// the appliance for queries and DML.
    pub fn qualified_name(&self) -> String {
        format!("{}.{}", self.schema, self.name)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnEntry {
    pub name: String,
    pub type_name: String,
    pub nullable: bool,
}

impl From<&NzTableInfo> for TableEntry {
    fn from(t: &NzTableInfo) -> Self {
        TableEntry {
            schema: t.schema.clone(),
            name: t.name.clone(),
        }
    }
}

impl From<TableEntryLite> for TableEntry {
    fn from(t: TableEntryLite) -> Self {
        TableEntry {
            schema: t.schema,
            name: t.name,
        }
    }
}

impl From<(String, String)> for TableEntry {
    fn from((schema, name): (String, String)) -> Self {
        TableEntry { schema, name }
    }
}

impl From<&NzColumnInfo> for ColumnEntry {
    fn from(c: &NzColumnInfo) -> Self {
        ColumnEntry {
            name: c.name.clone(),
            type_name: c.type_name.clone(),
            nullable: c.nullable,
        }
    }
}

/// What pressing `Enter` on the selection should do.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Activation {
    /// Expand/collapse the selected table.
    Toggle(usize),
    /// A table was expanded whose columns are not loaded yet: the caller must
    /// load them and hand them to [`Browser::set_columns`].
    LoadAndExpand(usize),
    /// A column row was picked: insert its name into the editor.
    InsertColumn { table_index: usize, name: String },
    /// Nothing to do (empty list).
    None,
}

/// Sidebar state: the loaded catalog, the filter, the rendered rows and the
/// selection/scroll window.
#[derive(Debug, Default, Clone)]
pub struct Browser {
    /// Unfiltered table list.
    tables: Vec<TableEntry>,
    /// Columns per table index: `None` = collapsed / not loaded,
    /// `Some(..)` = expanded (empty on a failed load).
    columns: Vec<Option<Vec<ColumnEntry>>>,
    /// Case-insensitive substring filter on the qualified table name.
    filter: String,
    /// Flat rendered rows, derived from the fields above by [`Browser::rebuild`].
    rows: Vec<Entry>,
    /// Selected row index into `rows`.
    selected: usize,
    /// First visible row.
    top: usize,
    /// Viewport height in rows, set by the renderer each frame.
    height: usize,
}

impl Browser {
    pub fn new() -> Self {
        Browser::default()
    }

    /// Replace the table list and drop all expansions (the catalog moved).
    pub fn set_tables(&mut self, tables: Vec<TableEntry>) {
        self.tables = tables;
        self.columns = vec![None; self.tables.len()];
        self.rebuild();
        self.selected = 0;
        self.top = 0;
    }

    /// Attach loaded columns to a table. An empty list (e.g. a failed load)
    /// keeps the table expanded so the failure stays visible.
    pub fn set_columns(&mut self, table_index: usize, columns: Vec<ColumnEntry>) {
        if table_index < self.columns.len() {
            self.columns[table_index] = Some(columns);
        }
        self.rebuild();
    }

    pub fn filter(&self) -> &str {
        &self.filter
    }

    /// Set the filter (case-insensitive substring on `SCHEMA.TABLE`).
    pub fn set_filter(&mut self, filter: impl Into<String>) {
        self.filter = filter.into();
        self.rebuild();
        self.clamp();
    }

    /// True when no rows are rendered (empty catalog or everything filtered).
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Tables currently matching the filter, as indices into the table list.
    pub fn matching_tables(&self) -> Vec<usize> {
        let needle = self.filter.to_uppercase();
        self.tables
            .iter()
            .enumerate()
            .filter(|(_, t)| {
                needle.is_empty() || t.qualified_name().to_uppercase().contains(&needle)
            })
            .map(|(i, _)| i)
            .collect()
    }

    /// Recompute `rows` from the tables, columns and filter.
    fn rebuild(&mut self) {
        self.rows.clear();
        for i in self.matching_tables() {
            let expanded = self.columns[i].is_some();
            self.rows.push(Entry::Table {
                index: i,
                table: self.tables[i].clone(),
                expanded,
            });
            if let Some(cols) = &self.columns[i] {
                for c in cols {
                    self.rows.push(Entry::Column {
                        table_index: i,
                        column: c.clone(),
                    });
                }
            }
        }
    }

    /// Pull `top`/`selected` back inside the (possibly shrunken) row list.
    fn clamp(&mut self) {
        let last = self.rows.len().saturating_sub(1);
        self.selected = self.selected.min(last);
        if self.rows.is_empty() || self.height == 0 {
            self.top = 0;
            return;
        }
        let max_top = self.rows.len().saturating_sub(self.height);
        self.top = self.top.min(max_top);
        if self.selected < self.top {
            self.top = self.selected;
        } else if self.selected >= self.top + self.height {
            self.top = self.selected + 1 - self.height;
        }
    }

    /// Viewport height in rows; the renderer calls this every frame.
    pub fn set_height(&mut self, height: usize) {
        self.height = height;
        self.clamp();
    }

    /// The rendered row under the selection, if any.
    pub fn selected_entry(&self) -> Option<&Entry> {
        self.rows.get(self.selected)
    }

    /// The visible row window for rendering.
    pub fn visible(&self) -> Range<usize> {
        let start = self.top.min(self.rows.len());
        let end = (start + self.height).min(self.rows.len());
        start..end
    }

    pub fn rows(&self) -> &[Entry] {
        &self.rows
    }

    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    pub fn table_count(&self) -> usize {
        self.tables.len()
    }

    /// All tables currently known to the catalog snapshot.
    pub fn tables(&self) -> &[TableEntry] {
        &self.tables
    }

    /// Columns loaded for a table, or `None` while that table is still lazy.
    pub fn columns_for(&self, table_index: usize) -> Option<&[ColumnEntry]> {
        self.columns.get(table_index).and_then(|columns| columns.as_deref())
    }

    /// Find a catalog table by case-insensitive schema/name.
    pub fn find_table_index(&self, schema: Option<&str>, name: &str) -> Option<usize> {
        self.tables.iter().enumerate().find_map(|(index, table)| {
            let same_name = table.name.eq_ignore_ascii_case(name);
            let same_schema = schema
                .map(|schema| table.schema.eq_ignore_ascii_case(schema))
                .unwrap_or(true);
            (same_name && same_schema).then_some(index)
        })
    }

    pub fn selected_index(&self) -> usize {
        self.selected
    }

    /// Move the selection by `delta`, clamped to the data.
    pub fn move_selection(&mut self, delta: i64) {
        if self.rows.is_empty() {
            return;
        }
        let next = (self.selected as i64 + delta).clamp(0, self.rows.len() as i64 - 1);
        self.selected = next as usize;
        self.clamp();
    }

    pub fn select_first(&mut self) {
        self.selected = 0;
        self.clamp();
    }

    pub fn select_last(&mut self) {
        self.selected = self.rows.len().saturating_sub(1);
        self.clamp();
    }

    /// Resolve `Enter` on the current selection without mutating anything;
    /// apply [`Activation::Toggle`] with [`Browser::toggle`] afterwards.
    ///
    /// Collapsed tables always request a load: the catalog may have changed
    /// since the last expand, and reloading is a cheap background query.
    pub fn activate(&self) -> Activation {
        match self.selected_entry() {
            Some(Entry::Table {
                index, expanded, ..
            }) => {
                if *expanded {
                    Activation::Toggle(*index)
                } else {
                    Activation::LoadAndExpand(*index)
                }
            }
            Some(Entry::Column {
                table_index,
                column,
            }) => Activation::InsertColumn {
                table_index: *table_index,
                name: column.name.clone(),
            },
            None => Activation::None,
        }
    }

    /// Expand or collapse a table by its table index. Collapsing always works;
    /// expanding leaves an empty column list the caller fills after loading.
    pub fn toggle(&mut self, table_index: usize) {
        let Some(slot) = self.columns.get_mut(table_index) else {
            return;
        };
        *slot = match slot {
            Some(_) => None,
            None => Some(Vec::new()),
        };
        self.rebuild();
        self.clamp();
    }

    /// The `(schema, table)` pair of the table with the given list index.
    pub fn qualified_parts(&self, table_index: usize) -> Option<(String, String)> {
        self.tables
            .get(table_index)
            .map(|t| (t.schema.clone(), t.name.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tables(names: &[(&str, &str)]) -> Vec<TableEntry> {
        names
            .iter()
            .map(|(s, n)| TableEntry {
                schema: (*s).into(),
                name: (*n).into(),
            })
            .collect()
    }

    fn cols(names: &[(&str, &str)]) -> Vec<ColumnEntry> {
        names
            .iter()
            .map(|(n, t)| ColumnEntry {
                name: (*n).into(),
                type_name: (*t).into(),
                nullable: true,
            })
            .collect()
    }

    #[test]
    fn qualified_names_use_two_parts() {
        let t = TableEntry {
            schema: "ADMIN".into(),
            name: "DIMACCOUNT".into(),
        };
        assert_eq!(t.qualified_name(), "ADMIN.DIMACCOUNT");
    }

    #[test]
    fn rows_are_tables_plus_expanded_columns() {
        let mut b = Browser::new();
        b.set_tables(tables(&[("ADMIN", "A"), ("ADMIN", "B")]));
        assert_eq!(b.row_count(), 2);
        b.set_columns(0, cols(&[("ID", "INTEGER"), ("NAME", "VARCHAR(32)")]));
        assert_eq!(b.row_count(), 4, "table A + 2 columns + table B");
        assert!(matches!(b.rows()[0], Entry::Table { expanded: true, .. }));
        assert!(matches!(
            b.rows()[3],
            Entry::Table {
                expanded: false,
                ..
            }
        ));
        // Column rows remember their table by list index, not row index.
        match &b.rows()[1] {
            Entry::Column {
                table_index,
                column,
            } => {
                assert_eq!(*table_index, 0);
                assert_eq!(column.name, "ID");
            }
            other => panic!("expected a column row, got {other:?}"),
        }
    }

    #[test]
    fn filter_is_case_insensitive_on_the_qualified_name() {
        let mut b = Browser::new();
        b.set_tables(tables(&[
            ("ADMIN", "DIMDATE"),
            ("ADMIN", "DIMACCOUNT"),
            ("MJ", "FACTS"),
        ]));
        b.set_filter("dim");
        assert_eq!(b.row_count(), 2);
        b.set_filter("FACT");
        assert_eq!(b.row_count(), 1);
        b.set_filter("NOSUCHTABLE");
        assert_eq!(b.row_count(), 0);
        // An empty filter shows everything again.
        b.set_filter("");
        assert_eq!(b.row_count(), 3);
    }

    #[test]
    fn filter_keeps_the_selection_in_range() {
        let mut b = Browser::new();
        b.set_tables(tables(&[("S", "A"), ("S", "B"), ("S", "C")]));
        b.move_selection(2);
        assert_eq!(b.selected_index(), 2);
        // Shrinking the list must pull the selection back.
        b.set_filter("A");
        assert_eq!(b.selected_index(), 0);
    }

    #[test]
    fn activation_on_unloaded_table_requests_a_load() {
        let mut b = Browser::new();
        b.set_tables(tables(&[("ADMIN", "DIMDATE")]));
        assert_eq!(b.activate(), Activation::LoadAndExpand(0));
    }

    #[test]
    fn activation_on_loaded_table_toggles() {
        let mut b = Browser::new();
        b.set_tables(tables(&[("ADMIN", "DIMDATE")]));
        b.set_columns(0, cols(&[("ID", "INTEGER")]));
        assert_eq!(b.activate(), Activation::Toggle(0));
        b.toggle(0);
        assert_eq!(b.row_count(), 1, "collapsed again");
        // Collapsing drops the columns, so re-expanding reloads (fresh data).
        assert_eq!(b.activate(), Activation::LoadAndExpand(0));
        b.set_columns(0, cols(&[("ID", "INTEGER")]));
        assert_eq!(b.row_count(), 2);
    }

    #[test]
    fn activation_on_column_inserts_the_name() {
        let mut b = Browser::new();
        b.set_tables(tables(&[("ADMIN", "DIMDATE")]));
        b.set_columns(0, cols(&[("DATE_SK", "INTEGER"), ("CAL_DATE", "DATE")]));
        b.move_selection(1);
        assert_eq!(
            b.activate(),
            Activation::InsertColumn {
                table_index: 0,
                name: "DATE_SK".into()
            }
        );
        // The qualified table name is reachable from the column's table index.
        assert_eq!(
            b.qualified_parts(0),
            Some(("ADMIN".into(), "DIMDATE".into()))
        );
        assert_eq!(
            TableEntry::from(("ADMIN".to_string(), "DIMDATE".to_string())).qualified_name(),
            "ADMIN.DIMDATE"
        );
    }

    #[test]
    fn toggle_unknown_index_is_a_no_op() {
        let mut b = Browser::new();
        b.set_tables(tables(&[("S", "A")]));
        b.toggle(99);
        assert_eq!(b.row_count(), 1);
    }

    #[test]
    fn failed_load_keeps_the_table_expanded_and_empty() {
        let mut b = Browser::new();
        b.set_tables(tables(&[("S", "A")]));
        b.set_columns(0, Vec::new());
        assert_eq!(b.row_count(), 1);
        assert!(matches!(b.rows()[0], Entry::Table { expanded: true, .. }));
    }

    #[test]
    fn movement_clamps_at_both_ends() {
        let mut b = Browser::new();
        b.set_tables(tables(&[("S", "A"), ("S", "B")]));
        b.move_selection(-5);
        assert_eq!(b.selected_index(), 0);
        b.move_selection(99);
        assert_eq!(b.selected_index(), 1);
        b.select_first();
        assert_eq!(b.selected_index(), 0);
        b.select_last();
        assert_eq!(b.selected_index(), 1);
        // Empty list: movement and activation are no-ops.
        b.set_filter("zzz");
        b.move_selection(3);
        assert_eq!(b.activate(), Activation::None);
    }

    #[test]
    fn visible_window_scrolls_with_the_selection() {
        let names: Vec<(String, String)> = (0..50)
            .map(|i| ("S".to_string(), format!("T{i:02}")))
            .collect();
        let borrowed: Vec<(&str, &str)> = names
            .iter()
            .map(|(s, n)| (s.as_str(), n.as_str()))
            .collect();
        let mut b = Browser::new();
        b.set_tables(tables(&borrowed));
        b.set_height(10);
        assert_eq!(b.visible(), 0..10);

        // Walking down pulls the window along and never shows past the end.
        for _ in 0..49 {
            b.move_selection(1);
        }
        assert_eq!(b.selected_index(), 49);
        let window = b.visible();
        assert_eq!(window, 40..50, "window follows to the bottom");
        assert!(window.contains(&b.selected_index()));

        // And back to the top.
        b.select_first();
        assert_eq!(b.visible(), 0..10);
    }

    #[test]
    fn zero_height_and_empty_states_stay_safe() {
        let mut b = Browser::new();
        b.set_tables(tables(&[("S", "A")]));
        b.set_height(0);
        assert_eq!(b.visible(), 0..0);
        b.move_selection(1);
        assert_eq!(b.visible(), 0..0);
        b.set_height(5);
        assert_eq!(b.visible(), 0..1);
    }

    #[test]
    fn set_tables_resets_expansions_and_selection() {
        let mut b = Browser::new();
        b.set_tables(tables(&[("S", "A"), ("S", "B")]));
        b.set_columns(0, cols(&[("ID", "INTEGER")]));
        b.move_selection(5);
        b.set_tables(tables(&[("S", "C")]));
        assert_eq!(b.row_count(), 1);
        assert_eq!(b.selected_index(), 0);
        assert!(matches!(
            b.rows()[0],
            Entry::Table {
                expanded: false,
                ..
            }
        ));
    }
}
