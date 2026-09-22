//! Lightweight SQL completion for the terminal editor.
//!
//! This intentionally uses a bounded lexical scan instead of a full SQL AST:
//! the buffer is normally incomplete while completion is requested, and the
//! editor targets a Netezza dialect that is not represented by a dedicated
//! parser here. The scanner understands the useful v1 cases (`FROM`, `JOIN`,
//! simple aliases and `alias.`) while ignoring strings and comments.

use crate::app::Editor;
use crate::browser::Browser;
use crate::syntax;
use std::collections::HashSet;

const MAX_ITEMS: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompletionKind {
    Keyword,
    Table,
    Column,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletionItem {
    pub label: String,
    pub insert_text: String,
    pub detail: String,
    pub kind: CompletionKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompletionMode {
    Table,
    Column,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletionAnalysis {
    pub items: Vec<CompletionItem>,
    pub replace_start: (usize, usize),
    pub replace_end: (usize, usize),
    pub missing_tables: Vec<usize>,
    pub show: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CompletionState {
    pub visible: bool,
    pub selected: usize,
    pub items: Vec<CompletionItem>,
    pub replace_start: (usize, usize),
    pub replace_end: (usize, usize),
}

impl CompletionState {
    pub fn dismiss(&mut self) {
        self.visible = false;
        self.selected = 0;
        self.items.clear();
    }

    pub fn apply(&mut self, analysis: CompletionAnalysis) {
        self.items = analysis.items;
        self.replace_start = analysis.replace_start;
        self.replace_end = analysis.replace_end;
        self.selected = self.selected.min(self.items.len().saturating_sub(1));
        self.visible = analysis.show && !self.items.is_empty();
        if !self.visible {
            self.selected = 0;
        }
    }

    pub fn move_selection(&mut self, delta: i64) {
        if !self.visible || self.items.is_empty() {
            return;
        }
        let last = self.items.len() as i64 - 1;
        self.selected = (self.selected as i64 + delta).clamp(0, last) as usize;
    }

    pub fn selected_item(&self) -> Option<&CompletionItem> {
        self.visible.then(|| self.items.get(self.selected)).flatten()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Token {
    text: String,
    start: usize,
    end: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TableRef {
    schema: Option<String>,
    name: String,
    alias: Option<String>,
    table_index: Option<usize>,
}

/// Analyze the text before the editor cursor and produce bounded candidates.
pub fn analyze(editor: &Editor, browser: &Browser, force: bool) -> CompletionAnalysis {
    let text = editor.text();
    let chars: Vec<char> = text.chars().collect();
    let cursor = cursor_offset(editor);
    let tokens = tokenize(&chars[..cursor.min(chars.len())]);
    let active = active_tokens(&tokens);
    let (prefix, prefix_start) = current_prefix(&chars, &tokens, cursor);
    let qualifier = qualifier_before(&tokens, prefix_start);
    let mode = if qualifier.is_some() {
        CompletionMode::Column
    } else {
        completion_mode(&active)
    };
    let refs = table_refs(&active, browser);

    let mut missing_tables = Vec::new();
    let mut candidates = Vec::new();
    match mode {
        CompletionMode::Table => {
            candidates.extend(browser.tables().iter().map(|table| CompletionItem {
                label: table.qualified_name(),
                insert_text: table.qualified_name(),
                detail: "table".into(),
                kind: CompletionKind::Table,
            }));
        }
        CompletionMode::Column => {
            let mut seen = HashSet::new();
            for table_ref in &refs {
                if let Some(qualifier) = qualifier.as_deref() {
                    if !table_ref_matches(table_ref, qualifier) {
                        continue;
                    }
                }
                let Some(index) = table_ref.table_index else {
                    continue;
                };
                let Some(columns) = browser.columns_for(index) else {
                    missing_tables.push(index);
                    continue;
                };
                let table = &browser.tables()[index];
                for column in columns {
                    if seen.insert(column.name.to_ascii_uppercase()) {
                        candidates.push(CompletionItem {
                            label: column.name.clone(),
                            insert_text: column.name.clone(),
                            detail: format!("{} {}", table.qualified_name(), column.type_name),
                            kind: CompletionKind::Column,
                        });
                    }
                }
            }
            if qualifier.is_none() {
                candidates.extend(syntax::KEYWORDS.iter().chain(syntax::TYPES).map(|word| {
                    CompletionItem {
                        label: (*word).into(),
                        insert_text: (*word).into(),
                        detail: "keyword".into(),
                        kind: CompletionKind::Keyword,
                    }
                }));
            }
        }
    }

    let prefix_upper = prefix.to_ascii_uppercase();
    candidates.retain(|item| item.label.to_ascii_uppercase().starts_with(&prefix_upper));
    candidates.sort_by_key(|item| {
        let rank = match (mode, item.kind) {
            (CompletionMode::Table, CompletionKind::Table) => 0,
            (CompletionMode::Table, _) => 1,
            (CompletionMode::Column, CompletionKind::Column) => 0,
            (CompletionMode::Column, CompletionKind::Keyword) => 1,
            (CompletionMode::Column, CompletionKind::Table) => 2,
        };
        (rank, item.label.to_ascii_uppercase())
    });
    candidates.truncate(MAX_ITEMS);

    let show = force
        || !prefix.is_empty()
        || qualifier.is_some()
        || (mode == CompletionMode::Table && active.last().is_some_and(|t| {
            matches!(t.text.to_ascii_uppercase().as_str(), "FROM" | "JOIN")
        }));
    let replace_start = position_for_offset(editor, prefix_start);
    let replace_end = position_for_offset(editor, cursor);
    CompletionAnalysis {
        items: candidates,
        replace_start,
        replace_end,
        missing_tables: dedup(missing_tables),
        show,
    }
}

fn cursor_offset(editor: &Editor) -> usize {
    editor.lines[..editor.cur.0]
        .iter()
        .map(|line| line.len() + 1)
        .sum::<usize>()
        + editor.cur.1
}

fn position_for_offset(editor: &Editor, mut offset: usize) -> (usize, usize) {
    for (row, line) in editor.lines.iter().enumerate() {
        if offset <= line.len() {
            return (row, offset);
        }
        offset = offset.saturating_sub(line.len() + 1);
    }
    let row = editor.lines.len().saturating_sub(1);
    (row, editor.line_len(row))
}

fn current_prefix(chars: &[char], tokens: &[Token], cursor: usize) -> (String, usize) {
    if let Some(token) = tokens
        .last()
        .filter(|token| token.end == cursor && is_identifier(&token.text))
    {
        return (token.text.clone(), token.start);
    }
    let start = cursor;
    (chars[start..cursor].iter().collect(), start)
}

fn qualifier_before(tokens: &[Token], prefix_start: usize) -> Option<String> {
    let before = tokens.iter().rev().find(|token| token.end <= prefix_start)?;
    if before.text != "." {
        return None;
    }
    tokens
        .iter()
        .rev()
        .skip(1)
        .find(|token| token.end <= before.start)
        .filter(|token| is_identifier(&token.text))
        .map(|token| token.text.clone())
}

fn completion_mode(tokens: &[Token]) -> CompletionMode {
    let mut mode = CompletionMode::Column;
    for token in tokens {
        match token.text.to_ascii_uppercase().as_str() {
            "FROM" | "JOIN" => mode = CompletionMode::Table,
            "SELECT" | "WHERE" | "ON" | "HAVING" | "GROUP" | "ORDER" | "SET"
            | "VALUES" => mode = CompletionMode::Column,
            _ => {}
        }
    }
    mode
}

fn table_refs(tokens: &[Token], browser: &Browser) -> Vec<TableRef> {
    let mut refs = Vec::new();
    let mut i = 0;
    while i < tokens.len() {
        let keyword = tokens[i].text.to_ascii_uppercase();
        if keyword != "FROM" && keyword != "JOIN" {
            i += 1;
            continue;
        }
        let Some((schema, name, mut next)) = qualified_name(tokens, i + 1) else {
            i += 1;
            continue;
        };
        let mut alias = None;
        if tokens.get(next).is_some_and(|token| token.text.eq_ignore_ascii_case("AS")) {
            if let Some(token) = tokens.get(next + 1).filter(|token| is_identifier(&token.text)) {
                alias = Some(token.text.clone());
                next += 2;
            }
        } else if let Some(token) = tokens.get(next).filter(|token| {
            is_identifier(&token.text) && !is_clause_keyword(&token.text)
        }) {
            alias = Some(token.text.clone());
        }
        let table_index = browser.find_table_index(schema.as_deref(), &name);
        refs.push(TableRef {
            schema,
            name,
            alias,
            table_index,
        });
        i = next.max(i + 1);
    }
    refs
}

fn qualified_name(tokens: &[Token], start: usize) -> Option<(Option<String>, String, usize)> {
    let name = tokens.get(start).filter(|token| is_identifier(&token.text))?;
    if tokens.get(start + 1).is_some_and(|token| token.text == ".") {
        let tail = tokens
            .get(start + 2)
            .filter(|token| is_identifier(&token.text))?;
        return Some((Some(name.text.clone()), tail.text.clone(), start + 3));
    }
    Some((None, name.text.clone(), start + 1))
}

fn table_ref_matches(table: &TableRef, qualifier: &str) -> bool {
    table
        .alias
        .as_deref()
        .is_some_and(|alias| alias.eq_ignore_ascii_case(qualifier))
        || table.name.eq_ignore_ascii_case(qualifier)
}

fn is_clause_keyword(text: &str) -> bool {
    matches!(
        text.to_ascii_uppercase().as_str(),
        "FROM"
            | "JOIN"
            | "ON"
            | "WHERE"
            | "GROUP"
            | "ORDER"
            | "HAVING"
            | "LIMIT"
            | "UNION"
            | "LEFT"
            | "RIGHT"
            | "FULL"
            | "INNER"
            | "OUTER"
    )
}

fn is_identifier(text: &str) -> bool {
    text.chars().next().is_some_and(|ch| {
        ch.is_ascii_alphabetic() || ch == '_' || ch == '$'
    }) && text
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '$'))
}

fn active_tokens(tokens: &[Token]) -> Vec<Token> {
    tokens
        .iter()
        .rposition(|token| token.text == ";")
        .map(|index| tokens[index + 1..].to_vec())
        .unwrap_or_else(|| tokens.to_vec())
}

fn tokenize(chars: &[char]) -> Vec<Token> {
    let mut tokens = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        if chars[i] == '-' && chars.get(i + 1) == Some(&'-') {
            i += 2;
            while i < chars.len() && chars[i] != '\n' {
                i += 1;
            }
            continue;
        }
        if chars[i] == '/' && chars.get(i + 1) == Some(&'*') {
            i += 2;
            while i + 1 < chars.len() {
                if chars[i] == '*' && chars[i + 1] == '/' {
                    i += 2;
                    break;
                }
                i += 1;
            }
            continue;
        }
        if chars[i] == '\'' {
            i += 1;
            while i < chars.len() {
                if chars[i] == '\'' {
                    if chars.get(i + 1) == Some(&'\'') {
                        i += 2;
                    } else {
                        i += 1;
                        break;
                    }
                } else {
                    i += 1;
                }
            }
            continue;
        }
        if chars[i].is_ascii_alphabetic() || matches!(chars[i], '_' | '$') {
            let start = i;
            i += 1;
            while i < chars.len()
                && (chars[i].is_ascii_alphanumeric() || matches!(chars[i], '_' | '$'))
            {
                i += 1;
            }
            tokens.push(Token {
                text: chars[start..i].iter().collect(),
                start,
                end: i,
            });
            continue;
        }
        if matches!(chars[i], '.' | ',' | ';') {
            tokens.push(Token {
                text: chars[i].to_string(),
                start: i,
                end: i + 1,
            });
        }
        i += 1;
    }
    tokens
}

fn dedup(values: Vec<usize>) -> Vec<usize> {
    let mut seen = HashSet::new();
    values.into_iter().filter(|value| seen.insert(*value)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::browser::{ColumnEntry, TableEntry};

    fn browser() -> Browser {
        let mut browser = Browser::new();
        browser.set_tables(vec![
            TableEntry {
                schema: "ADMIN".into(),
                name: "USERS".into(),
            },
            TableEntry {
                schema: "ADMIN".into(),
                name: "ORDERS".into(),
            },
        ]);
        browser.set_columns(
            0,
            vec![ColumnEntry {
                name: "USER_ID".into(),
                type_name: "INTEGER".into(),
                nullable: false,
            }],
        );
        browser
    }

    fn labels(analysis: &CompletionAnalysis) -> Vec<&str> {
        analysis
            .items
            .iter()
            .map(|item| item.label.as_str())
            .collect()
    }

    #[test]
    fn suggests_tables_after_from() {
        let editor = Editor::from_text("SELECT * FROM AD");
        let analysis = analyze(&editor, &browser(), false);
        assert_eq!(labels(&analysis), vec!["ADMIN.ORDERS", "ADMIN.USERS"]);
    }

    #[test]
    fn suggests_columns_for_a_simple_alias() {
        let editor = Editor::from_text("SELECT u. FROM ADMIN.USERS u WHERE u.");
        let analysis = analyze(&editor, &browser(), true);
        assert_eq!(labels(&analysis), vec!["USER_ID"]);
    }

    #[test]
    fn strings_and_comments_do_not_change_context() {
        let editor = Editor::from_text("SELECT '-- FROM fake' -- JOIN fake\nFROM AD");
        let analysis = analyze(&editor, &browser(), false);
        assert_eq!(labels(&analysis), vec!["ADMIN.ORDERS", "ADMIN.USERS"]);
    }

    #[test]
    fn reports_unloaded_referenced_columns() {
        let editor = Editor::from_text("SELECT * FROM ADMIN.ORDERS o WHERE o.");
        let analysis = analyze(&editor, &browser(), true);
        assert_eq!(analysis.missing_tables, vec![1]);
    }
}
