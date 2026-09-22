//! Small SQL syntax highlighter for the terminal editor.

use ratatui::style::{Color, Modifier, Style};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenKind {
    Plain,
    Keyword,
    Type,
    Function,
    Number,
    String,
    Comment,
    Operator,
}

impl TokenKind {
    pub(crate) fn style(self) -> Style {
        match self {
            Self::Plain => Style::default(),
            Self::Keyword => Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
            Self::Type => Style::default().fg(Color::Yellow),
            Self::Function => Style::default().fg(Color::Magenta),
            Self::Number => Style::default().fg(Color::Green),
            Self::String => Style::default().fg(Color::LightGreen),
            Self::Comment => Style::default()
                .fg(Color::DarkGray)
                .add_modifier(Modifier::ITALIC),
            Self::Operator => Style::default().fg(Color::LightBlue),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HighlightSpan {
    pub text: String,
    pub kind: TokenKind,
}

#[derive(Debug, Default)]
pub struct Highlighter {
    in_block_comment: bool,
}

impl Highlighter {
    pub fn highlight_line(&mut self, text: &str) -> Vec<HighlightSpan> {
        let chars: Vec<char> = text.chars().collect();
        let mut spans = Vec::new();
        let mut i = 0;

        while i < chars.len() {
            if self.in_block_comment {
                let start = i;
                while i + 1 < chars.len() {
                    if chars[i] == '*' && chars[i + 1] == '/' {
                        i += 2;
                        self.in_block_comment = false;
                        break;
                    }
                    i += 1;
                }
                if self.in_block_comment {
                    i = chars.len();
                }
                push_span(&mut spans, &chars[start..i], TokenKind::Comment);
                continue;
            }

            if chars[i] == '-' && chars.get(i + 1) == Some(&'-') {
                push_span(&mut spans, &chars[i..], TokenKind::Comment);
                break;
            }
            if chars[i] == '/' && chars.get(i + 1) == Some(&'*') {
                let start = i;
                i += 2;
                self.in_block_comment = true;
                while i + 1 < chars.len() {
                    if chars[i] == '*' && chars[i + 1] == '/' {
                        i += 2;
                        self.in_block_comment = false;
                        break;
                    }
                    i += 1;
                }
                if self.in_block_comment {
                    i = chars.len();
                }
                push_span(&mut spans, &chars[start..i], TokenKind::Comment);
                continue;
            }
            if chars[i] == '\'' {
                let start = i;
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
                push_span(&mut spans, &chars[start..i], TokenKind::String);
                continue;
            }
            if chars[i].is_ascii_alphabetic() || chars[i] == '_' {
                let start = i;
                i += 1;
                while i < chars.len() && (chars[i].is_ascii_alphanumeric() || chars[i] == '_') {
                    i += 1;
                }
                let word: String = chars[start..i].iter().collect();
                let kind = classify_word(&word, chars.get(i) == Some(&'('));
                push_span(&mut spans, &chars[start..i], kind);
                continue;
            }
            if chars[i].is_ascii_digit() {
                let start = i;
                i += 1;
                while i < chars.len()
                    && (chars[i].is_ascii_digit()
                        || matches!(chars[i], '.' | 'e' | 'E' | '+' | '-'))
                {
                    i += 1;
                }
                push_span(&mut spans, &chars[start..i], TokenKind::Number);
                continue;
            }
            if is_operator(chars[i]) {
                push_span(&mut spans, &chars[i..i + 1], TokenKind::Operator);
                i += 1;
                continue;
            }

            let start = i;
            i += 1;
            while i < chars.len()
                && !chars[i].is_ascii_alphanumeric()
                && chars[i] != '_'
                && chars[i] != '\''
                && !is_operator(chars[i])
                && !(chars[i] == '-' && chars.get(i + 1) == Some(&'-'))
                && !(chars[i] == '/' && chars.get(i + 1) == Some(&'*'))
            {
                i += 1;
            }
            push_span(&mut spans, &chars[start..i], TokenKind::Plain);
        }

        spans
    }
}

fn push_span(spans: &mut Vec<HighlightSpan>, chars: &[char], kind: TokenKind) {
    if !chars.is_empty() {
        spans.push(HighlightSpan {
            text: chars.iter().collect(),
            kind,
        });
    }
}

fn is_operator(ch: char) -> bool {
    matches!(
        ch,
        '=' | '<' | '>' | '+' | '-' | '*' | '/' | '%' | '!' | '|'
    )
}

fn classify_word(word: &str, followed_by_paren: bool) -> TokenKind {
    let upper = word.to_ascii_uppercase();
    if followed_by_paren {
        return TokenKind::Function;
    }
    if KEYWORDS.contains(&upper.as_str()) {
        TokenKind::Keyword
    } else if TYPES.contains(&upper.as_str()) {
        TokenKind::Type
    } else {
        TokenKind::Plain
    }
}

const KEYWORDS: &[&str] = &[
    "ALL", "AND", "AS", "ASC", "BEGIN", "BY", "CASE", "COMMIT", "CREATE", "DELETE", "DISTINCT",
    "DROP", "ELSE", "END", "EXISTS", "FROM", "GROUP", "HAVING", "IN", "INSERT", "INTO", "IS",
    "JOIN", "LEFT", "LIKE", "LIMIT", "NOT", "NULL", "ON", "OR", "ORDER", "OUTER", "RIGHT",
    "ROLLBACK", "SELECT", "SET", "TABLE", "THEN", "UNION", "UPDATE", "VALUES", "WHEN", "WHERE",
    "WITH",
];

const TYPES: &[&str] = &[
    "BIGINT",
    "BOOLEAN",
    "CHAR",
    "DATE",
    "DECIMAL",
    "DOUBLE",
    "FLOAT",
    "INTEGER",
    "INT",
    "NUMERIC",
    "REAL",
    "SMALLINT",
    "TIME",
    "TIMESTAMP",
    "VARCHAR",
];

#[cfg(test)]
mod tests {
    use super::*;

    fn kinds(text: &str) -> Vec<TokenKind> {
        Highlighter::default()
            .highlight_line(text)
            .into_iter()
            .map(|span| span.kind)
            .collect()
    }

    #[test]
    fn highlights_sql_tokens_case_insensitively() {
        let token_kinds = kinds("select count(*) from items where id = 42");
        assert_eq!(token_kinds.first(), Some(&TokenKind::Keyword));
        assert!(token_kinds.contains(&TokenKind::Function));
        assert!(token_kinds.contains(&TokenKind::Operator));
        assert!(token_kinds.contains(&TokenKind::Number));
        assert_eq!(
            token_kinds
                .iter()
                .filter(|kind| **kind == TokenKind::Keyword)
                .count(),
            3
        );
    }

    #[test]
    fn strings_and_comments_are_not_tokenized_as_sql() {
        let spans = Highlighter::default().highlight_line("SELECT 'FROM 1', -- WHERE");
        assert!(spans.iter().any(|span| span.kind == TokenKind::String));
        assert!(spans.iter().any(|span| span.kind == TokenKind::Comment));
    }

    #[test]
    fn block_comments_continue_across_lines() {
        let mut highlighter = Highlighter::default();
        assert_eq!(
            highlighter.highlight_line("/* SELECT")[0].kind,
            TokenKind::Comment
        );
        let spans = highlighter.highlight_line("FROM */ SELECT");
        assert_eq!(spans[0].kind, TokenKind::Comment);
        assert_eq!(spans.last().unwrap().kind, TokenKind::Keyword);
    }
}
