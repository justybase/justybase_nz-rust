//! Catalog helpers shared with the Python metadata API and DDL generation.

use super::*;
use crate::connection::Row;

/// Sequence identity returned by the catalog.
#[derive(Debug, Clone, PartialEq)]
pub struct NzSequenceInfo {
    pub schema: String,
    pub name: String,
    pub owner: Option<String>,
    pub object_id: Option<i64>,
}

/// User identity returned by the catalog.
#[derive(Debug, Clone, PartialEq)]
pub struct NzUserInfo {
    pub name: String,
    pub object_id: Option<i64>,
}

/// Group identity returned by the catalog.
#[derive(Debug, Clone, PartialEq)]
pub struct NzGroupInfo {
    pub name: String,
    pub object_id: Option<i64>,
}

/// Available query history fields; absent values remain None.
#[derive(Debug, Clone, PartialEq)]
pub struct NzQueryHistoryInfo {
    pub session_id: Option<i64>,
    pub username: Option<String>,
    pub database: Option<String>,
    pub query_text: Option<String>,
    pub submit_time: Option<String>,
    pub start_time: Option<String>,
    pub result_rows: Option<i64>,
}

/// Column definition and optional comments used for DDL reconstruction.
#[derive(Debug, Clone, PartialEq)]
pub struct NzDetailedColumnInfo {
    pub schema: String,
    pub name: String,
    pub ordinal: i32,
    pub type_name: String,
    pub not_null: bool,
    pub default_value: Option<String>,
    pub description: Option<String>,
}

/// Table constraint metadata, including referenced columns for foreign keys.
#[derive(Debug, Clone, PartialEq)]
pub struct NzTableKeyInfo {
    pub name: String,
    pub key_type: String,
    pub type_char: char,
    pub columns: Vec<String>,
    pub pk_database: Option<String>,
    pub pk_schema: Option<String>,
    pub pk_relation: Option<String>,
    pub pk_columns: Vec<String>,
    pub update_type: String,
    pub delete_type: String,
}

/// One batch DDL result; error is set when reconstruction failed.
#[derive(Debug, Clone, PartialEq)]
pub struct NzDdlBatchResult {
    pub schema: String,
    pub name: String,
    pub ddl: String,
    pub error: Option<String>,
}

impl NzMetadata<'_> {
    fn query_rows(&mut self, sql: &str) -> NzResult<Vec<Row>> {
        let result = self.connection.query(sql, &[])?;
        Ok(result
            .result_sets
            .into_iter()
            .next()
            .map(|set| set.rows)
            .unwrap_or_default())
    }

    /// Return the database selected by the active session.
    pub fn current_database(&mut self) -> NzResult<Option<String>> {
        self.query_rows("SELECT current_catalog")?
            .first()
            .map(|row| optional_text(row, 0))
            .transpose()
            .map(Option::flatten)
    }

    /// Return the current schema search path.
    pub fn current_schema(&mut self) -> NzResult<Option<String>> {
        self.query_rows("SELECT current_schema")?
            .first()
            .map(|row| optional_text(row, 0))
            .transpose()
            .map(Option::flatten)
    }

    /// List visible sequences.
    pub fn sequences(&mut self, schema: Option<&str>) -> NzResult<Vec<NzSequenceInfo>> {
        let mut sql = String::from(
            "SELECT schema, seqname, owner, objid FROM _v_sequence WHERE seqname IS NOT NULL",
        );
        if let Some(schema) = schema {
            sql.push_str(&format!(" AND schema = {}", literal(schema)?));
        }
        sql.push_str(" ORDER BY schema, seqname");
        self.query_rows(&sql)?
            .iter()
            .map(|row| {
                Ok(NzSequenceInfo {
                    schema: text(row, 0)?,
                    name: text(row, 1)?,
                    owner: optional_text(row, 2)?,
                    object_id: optional_i64(row, 3)?,
                })
            })
            .collect()
    }

    /// List users visible to the current connection.
    pub fn users(&mut self) -> NzResult<Vec<NzUserInfo>> {
        self.query_rows("SELECT username, objid FROM _v_user ORDER BY username")?
            .iter()
            .map(|row| {
                Ok(NzUserInfo {
                    name: text(row, 0)?,
                    object_id: optional_i64(row, 1)?,
                })
            })
            .collect()
    }

    /// List database groups.
    pub fn groups(&mut self) -> NzResult<Vec<NzGroupInfo>> {
        self.query_rows("SELECT groupname, objid FROM _v_group ORDER BY groupname")?
            .iter()
            .map(|row| {
                Ok(NzGroupInfo {
                    name: text(row, 0)?,
                    object_id: optional_i64(row, 1)?,
                })
            })
            .collect()
    }

    /// Return recent queries, newest first; an unconfigured history view is empty.
    pub fn query_history(
        &mut self,
        limit: usize,
        username: Option<&str>,
    ) -> NzResult<Vec<NzQueryHistoryInfo>> {
        let mut sql = String::from(
            "SELECT qh_sessionid, qh_user, qh_database, qh_sql, qh_tsubmit, qh_tstart, qh_resrows FROM _v_qryhist WHERE 1=1",
        );
        if let Some(username) = username {
            sql.push_str(&format!(" AND qh_user = {}", literal(username)?));
        }
        sql.push_str(&format!(" ORDER BY qh_tsubmit DESC LIMIT {limit}"));
        let rows = match self.query_rows(&sql) {
            Ok(rows) => rows,
            Err(error) if missing_relation(&error) => return Ok(Vec::new()),
            Err(error) => return Err(error),
        };
        rows.iter()
            .map(|row| {
                Ok(NzQueryHistoryInfo {
                    session_id: optional_i64(row, 0)?,
                    username: optional_text(row, 1)?,
                    database: optional_text(row, 2)?,
                    query_text: optional_text(row, 3)?,
                    submit_time: optional_text(row, 4)?,
                    start_time: optional_text(row, 5)?,
                    result_rows: optional_i64(row, 6)?,
                })
            })
            .collect()
    }

    /// Detailed columns including defaults and comments for CREATE TABLE DDL.
    pub fn detailed_columns(
        &mut self,
        table: &str,
        schema: Option<&str>,
    ) -> NzResult<Vec<NzDetailedColumnInfo>> {
        let (schema, table) = normalize_object_name(table, schema)?;
        let mut sql = format!(
            "SELECT schema, attname, attnum, format_type, attnotnull, coldefault, description FROM _v_relation_column WHERE name = {}",
            literal(&table)?
        );
        if let Some(schema) = &schema {
            sql.push_str(&format!(" AND schema = {}", literal(schema)?));
        }
        sql.push_str(" ORDER BY attnum");
        let rows = self.query_rows(&sql)?;
        if schema.is_none() {
            require_unique_schema(&rows, &table)?;
        }
        rows.iter()
            .map(|row| {
                Ok(NzDetailedColumnInfo {
                    schema: text(row, 0)?,
                    name: text(row, 1)?,
                    ordinal: row.try_get(2)?,
                    type_name: text(row, 3)?,
                    not_null: bool_value(row, 4)?,
                    default_value: optional_text(row, 5)?,
                    description: optional_text(row, 6)?,
                })
            })
            .collect()
    }

    /// Return ORGANIZE ON columns in catalog order.
    pub fn organize_columns(&mut self, table: &str, schema: Option<&str>) -> NzResult<Vec<String>> {
        let (schema, table) = normalize_object_name(table, schema)?;
        let mut sql = format!(
            "SELECT attname FROM _v_table_organize_column WHERE tablename = {}",
            literal(&table)?
        );
        if let Some(schema) = &schema {
            sql.push_str(&format!(" AND schema = {}", literal(schema)?));
        }
        sql.push_str(" ORDER BY orgseqno");
        let rows = match self.query_rows(&sql) {
            Ok(rows) => rows,
            Err(error) if missing_relation(&error) => return Ok(Vec::new()),
            Err(error) => return Err(error),
        };
        rows.iter().map(|row| text(row, 0)).collect()
    }

    /// Return primary, unique and foreign keys grouped by constraint name.
    pub fn table_keys(
        &mut self,
        table: &str,
        schema: Option<&str>,
    ) -> NzResult<Vec<NzTableKeyInfo>> {
        let (schema, table) = normalize_object_name(table, schema)?;
        let mut sql = format!(
            "SELECT constraintname, contype, attname, pkdatabase, pkschema, pkrelation, pkattname, updt_type, del_type FROM _v_relation_keydata WHERE relation = {}",
            literal(&table)?
        );
        if let Some(schema) = &schema {
            sql.push_str(&format!(" AND schema = {}", literal(schema)?));
        }
        sql.push_str(" ORDER BY constraintname, conseq");
        let rows = match self.query_rows(&sql) {
            Ok(rows) => rows,
            Err(error) if missing_relation(&error) => return Ok(Vec::new()),
            Err(error) => return Err(error),
        };
        let mut keys: Vec<NzTableKeyInfo> = Vec::new();
        for row in &rows {
            let name = text(row, 0)?;
            if keys.last().is_none_or(|key| key.name != name) {
                let type_char = text(row, 1)?.chars().next().unwrap_or('?');
                keys.push(NzTableKeyInfo {
                    name,
                    key_type: match type_char {
                        'p' => "PRIMARY KEY",
                        'f' => "FOREIGN KEY",
                        'u' => "UNIQUE",
                        _ => "UNKNOWN",
                    }
                    .into(),
                    type_char,
                    columns: Vec::new(),
                    pk_database: optional_text(row, 3)?,
                    pk_schema: optional_text(row, 4)?,
                    pk_relation: optional_text(row, 5)?,
                    pk_columns: Vec::new(),
                    update_type: optional_text(row, 7)?.unwrap_or_else(|| "NO ACTION".into()),
                    delete_type: optional_text(row, 8)?.unwrap_or_else(|| "NO ACTION".into()),
                });
            }
            let key = keys.last_mut().expect("new key inserted above");
            if let Some(column) = optional_text(row, 2)? {
                key.columns.push(column);
            }
            if let Some(column) = optional_text(row, 6)? {
                key.pk_columns.push(column);
            }
        }
        Ok(keys)
    }

    /// Return a table comment, if present.
    pub fn table_comment(&mut self, table: &str, schema: Option<&str>) -> NzResult<Option<String>> {
        let (schema, table) = normalize_object_name(table, schema)?;
        let mut where_clause = format!("objname = {}", literal(&table)?);
        if let Some(schema) = &schema {
            where_clause.push_str(&format!(" AND schema = {}", literal(schema)?));
        }
        for suffix in [" AND objtype = 'TABLE'", ""] {
            let sql =
                format!("SELECT description FROM _v_object_data WHERE {where_clause}{suffix}");
            let rows = match self.query_rows(&sql) {
                Ok(rows) => rows,
                Err(error) if missing_relation(&error) => return Ok(None),
                Err(error) => return Err(error),
            };
            for row in &rows {
                if let Some(comment) = optional_text(row, 0)? {
                    if !comment.trim().is_empty() {
                        return Ok(Some(comment));
                    }
                }
            }
        }
        Ok(None)
    }

    /// Return the owner of a table, if found.
    pub fn table_owner(&mut self, table: &str, schema: Option<&str>) -> NzResult<Option<String>> {
        let (schema, table) = normalize_object_name(table, schema)?;
        let mut sql = format!(
            "SELECT owner FROM _v_table WHERE tablename = {}",
            literal(&table)?
        );
        if let Some(schema) = &schema {
            sql.push_str(&format!(" AND schema = {}", literal(schema)?));
        }
        match self.query_rows(&sql) {
            Ok(rows) => rows
                .first()
                .map(|row| optional_text(row, 0))
                .transpose()
                .map(Option::flatten),
            Err(error) if missing_relation(&error) => Ok(None),
            Err(error) => Err(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::normalize_object_name;
    use crate::error::NzError;

    #[test]
    fn normalize_object_name_rejects_database_qualified_names() {
        for name in ["DB.SCHEMA.TABLE", "DB.\"Mixed Schema\".TABLE"] {
            assert!(matches!(
                normalize_object_name(name, None),
                Err(NzError::Config(message))
                    if message.contains("expected [schema.]object")
            ));
        }
    }

    #[test]
    fn normalize_object_name_keeps_schema_and_quoted_dots() {
        assert_eq!(
            normalize_object_name("SCHEMA.TABLE", None).unwrap(),
            (Some("SCHEMA".into()), "TABLE".into())
        );
        assert_eq!(
            normalize_object_name("\"Schema.Name\".\"Table.Name\"", None).unwrap(),
            (Some("Schema.Name".into()), "Table.Name".into())
        );
    }
}

fn bool_value(row: &Row, index: usize) -> NzResult<bool> {
    Ok(matches!(
        row.try_get_value(index)?
            .to_display_string()
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "true" | "t" | "1" | "yes" | "on"
    ))
}

fn missing_relation(error: &NzError) -> bool {
    matches!(error, NzError::Database(database) if database.code.as_deref() == Some("42P01"))
}

fn require_unique_schema(rows: &[Row], name: &str) -> NzResult<()> {
    let mut schema: Option<String> = None;
    for row in rows {
        let current = text(row, 0)?;
        if schema.as_ref().is_some_and(|existing| existing != &current) {
            return Err(NzError::Config(format!(
                "{name} exists in multiple schemas; pass schema explicitly"
            )));
        }
        schema = Some(current);
    }
    Ok(())
}

fn normalize_object_name(name: &str, schema: Option<&str>) -> NzResult<(Option<String>, String)> {
    let mut parts = Vec::new();
    let mut part = String::new();
    let mut in_quotes = false;
    let mut parentheses = 0usize;
    let mut chars = name.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '"' {
            if in_quotes && chars.peek() == Some(&'"') {
                part.push('"');
                chars.next();
            } else {
                in_quotes = !in_quotes;
                part.push(ch);
            }
        } else if !in_quotes && ch == '(' {
            parentheses += 1;
            part.push(ch);
        } else if !in_quotes && ch == ')' {
            parentheses = parentheses.saturating_sub(1);
            part.push(ch);
        } else if !in_quotes && parentheses == 0 && ch == '.' {
            parts.push(part);
            part = String::new();
        } else {
            part.push(ch);
        }
    }
    parts.push(part);
    if in_quotes || parentheses != 0 || parts.is_empty() || parts.len() > 2 {
        return Err(NzError::Config(format!(
            "invalid qualified object name (expected [schema.]object): {name}"
        )));
    }
    let object = normalize_identifier(parts.last().expect("nonempty"))?;
    let schema = if parts.len() >= 2 {
        Some(normalize_identifier(&parts[parts.len() - 2])?)
    } else {
        schema.map(normalize_identifier).transpose()?
    };
    Ok((schema, object))
}

fn normalize_identifier(part: &str) -> NzResult<String> {
    let trimmed = part.trim();
    if trimmed.is_empty() {
        return Err(NzError::Config("empty SQL identifier".into()));
    }
    if trimmed.starts_with('"') {
        if trimmed.len() < 2 || !trimmed.ends_with('"') {
            return Err(NzError::Config(format!(
                "invalid quoted SQL identifier: {trimmed}"
            )));
        }
        Ok(trimmed[1..trimmed.len() - 1].replace("\"\"", "\""))
    } else {
        Ok(trimmed.to_uppercase())
    }
}

fn quote_identifier(name: &str) -> String {
    if !name.is_empty()
        && name
            .chars()
            .next()
            .is_some_and(|ch| ch.is_ascii_uppercase() || ch == '_')
        && name
            .chars()
            .all(|ch| ch.is_ascii_uppercase() || ch.is_ascii_digit() || ch == '_')
    {
        name.to_owned()
    } else {
        format!("\"{}\"", name.replace('"', "\"\""))
    }
}

fn qualified(database: &str, schema: &str, name: &str) -> String {
    format!(
        "{}.{}.{}",
        quote_identifier(database),
        quote_identifier(schema),
        quote_identifier(name)
    )
}

fn sql_string(value: &str) -> String {
    value.replace('\'', "''")
}

fn fixed_return_type(value: &str) -> String {
    let upper = value.trim().to_uppercase();
    match upper.as_str() {
        "CHARACTER VARYING" | "NATIONAL CHARACTER VARYING" | "NATIONAL CHARACTER" | "CHARACTER" => {
            format!("{upper}(ANY)")
        }
        _ => value.to_owned(),
    }
}

impl NzMetadata<'_> {
    /// Reconstruct executable CREATE TABLE SQL from catalog columns and keys.
    pub fn table_ddl(
        &mut self,
        table: &str,
        schema: Option<&str>,
        database: Option<&str>,
    ) -> NzResult<String> {
        let (schema, table) = normalize_object_name(table, schema)?;
        let quoted_table = quote_identifier(&table);
        let quoted_schema = schema.as_deref().map(quote_identifier);
        let columns = self.detailed_columns(&quoted_table, quoted_schema.as_deref())?;
        if columns.is_empty() {
            return Err(NzError::Config(format!("table {table} not found")));
        }
        let schema = schema.unwrap_or_else(|| columns[0].schema.clone());
        let database = match database {
            Some(database) => database.to_owned(),
            None => self.current_database()?.unwrap_or_else(|| "UNKNOWN".into()),
        };
        let distribution = self.distribution_key(&table, Some(&schema))?;
        let organize = self.organize_columns(&quoted_table, Some(&quote_identifier(&schema)))?;
        let keys = self.table_keys(&quoted_table, Some(&quote_identifier(&schema)))?;
        let comment = self.table_comment(&quoted_table, Some(&quote_identifier(&schema)))?;
        Ok(build_table_ddl(
            &database,
            &schema,
            &table,
            &columns,
            &distribution,
            &organize,
            &keys,
            comment.as_deref(),
        ))
    }

    /// Reconstruct CREATE OR REPLACE VIEW SQL from the catalog definition.
    pub fn view_ddl(
        &mut self,
        view: &str,
        schema: Option<&str>,
        database: Option<&str>,
    ) -> NzResult<String> {
        let (schema, view) = normalize_object_name(view, schema)?;
        let mut sql = format!(
            "SELECT schema, viewname, definition FROM _v_view WHERE viewname = {}",
            literal(&view)?
        );
        if let Some(schema) = &schema {
            sql.push_str(&format!(" AND schema = {}", literal(schema)?));
        }
        sql.push_str(" ORDER BY schema, viewname");
        let rows = self.query_rows(&sql)?;
        require_unique_schema(&rows, &view)?;
        let row = rows
            .first()
            .ok_or_else(|| NzError::Config(format!("view {view} not found")))?;
        let schema = text(row, 0)?;
        let definition = optional_text(row, 2)?
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| NzError::Config(format!("view {schema}.{view} has no definition")))?;
        let database = match database {
            Some(database) => database.to_owned(),
            None => self.current_database()?.unwrap_or_else(|| "UNKNOWN".into()),
        };
        let body = definition.trim().trim_end_matches(';').trim_end();
        Ok(format!(
            "CREATE OR REPLACE VIEW {} AS\n{body};",
            qualified(&database, &schema, &view)
        ))
    }

    /// Reconstruct CREATE OR REPLACE PROCEDURE SQL, including the source body.
    pub fn procedure_ddl(
        &mut self,
        procedure: &str,
        schema: Option<&str>,
        database: Option<&str>,
    ) -> NzResult<String> {
        let (schema, procedure) = normalize_object_name(procedure, schema)?;
        let column = if procedure.contains('(') {
            "proceduresignature"
        } else {
            "procedure"
        };
        let mut sql = format!(
            "SELECT schema, procedure, proceduresignature, arguments, returns, executedasowner, description, proceduresource FROM _v_procedure WHERE {column} = {}",
            literal(&procedure)?
        );
        if let Some(schema) = &schema {
            sql.push_str(&format!(" AND schema = {}", literal(schema)?));
        }
        sql.push_str(" ORDER BY proceduresignature");
        let rows = self.query_rows(&sql)?;
        require_unique_schema(&rows, &procedure)?;
        if rows.len() > 1 {
            return Err(NzError::Config(format!(
                "procedure {procedure} has multiple overloads; pass its signature"
            )));
        }
        let row = rows
            .first()
            .ok_or_else(|| NzError::Config(format!("procedure {procedure} not found")))?;
        let schema = text(row, 0)?;
        let name = text(row, 1)?;
        let args = optional_text(row, 3)?.unwrap_or_default();
        let args = args.trim();
        let args = if args.is_empty() {
            "()".to_owned()
        } else if args.starts_with('(') && args.ends_with(')') {
            args.to_owned()
        } else {
            format!("({args})")
        };
        let returns =
            fixed_return_type(&optional_text(row, 4)?.unwrap_or_else(|| "INTEGER".into()));
        let execute_as_owner = match row.try_get_value(5)? {
            NzValue::Null => true,
            _ => bool_value(row, 5)?,
        };
        let description = optional_text(row, 6)?;
        let source = optional_text(row, 7)?.unwrap_or_default();
        let database = match database {
            Some(database) => database.to_owned(),
            None => self.current_database()?.unwrap_or_else(|| "UNKNOWN".into()),
        };
        let full_name = qualified(&database, &schema, &name);
        let mut ddl = format!(
            "CREATE OR REPLACE PROCEDURE {full_name}{args}\nRETURNS {returns}\nEXECUTE AS {}\nLANGUAGE NZPLSQL AS\nBEGIN_PROC\n{source}\nEND_PROC;",
            if execute_as_owner { "OWNER" } else { "CALLER" }
        );
        if let Some(description) = description {
            ddl.push_str(&format!(
                "\nCOMMENT ON PROCEDURE {full_name} IS '{}';",
                sql_string(&description)
            ));
        }
        Ok(ddl)
    }

    /// Reconstruct DDL for several tables while retaining per-object errors.
    pub fn tables_ddl(
        &mut self,
        schema: Option<&str>,
        pattern: Option<&str>,
        tables: Option<&[String]>,
    ) -> NzResult<Vec<NzDdlBatchResult>> {
        self.batch_ddl(DdlKind::Table, schema, pattern, tables)
    }

    /// Reconstruct DDL for several views while retaining per-object errors.
    pub fn views_ddl(
        &mut self,
        schema: Option<&str>,
        pattern: Option<&str>,
        views: Option<&[String]>,
    ) -> NzResult<Vec<NzDdlBatchResult>> {
        self.batch_ddl(DdlKind::View, schema, pattern, views)
    }

    /// Reconstruct DDL for several procedures while retaining per-object errors.
    pub fn procedures_ddl(
        &mut self,
        schema: Option<&str>,
        pattern: Option<&str>,
        procedures: Option<&[String]>,
    ) -> NzResult<Vec<NzDdlBatchResult>> {
        self.batch_ddl(DdlKind::Procedure, schema, pattern, procedures)
    }

    fn batch_ddl(
        &mut self,
        kind: DdlKind,
        schema: Option<&str>,
        pattern: Option<&str>,
        names: Option<&[String]>,
    ) -> NzResult<Vec<NzDdlBatchResult>> {
        let targets = if let Some(names) = names {
            names
                .iter()
                .map(|name| normalize_object_name(name, schema))
                .collect::<NzResult<Vec<_>>>()?
        } else {
            let (catalog, name_column, selected_name) = match kind {
                DdlKind::Table => ("_v_table", "tablename", "tablename"),
                DdlKind::View => ("_v_view", "viewname", "viewname"),
                DdlKind::Procedure => ("_v_procedure", "procedure", "proceduresignature"),
            };
            let mut sql = format!(
                "SELECT schema, {selected_name} FROM {catalog} WHERE {name_column} IS NOT NULL"
            );
            if let Some(schema) = schema {
                sql.push_str(&format!(" AND schema = {}", literal(schema)?));
            }
            if let Some(pattern) = pattern {
                sql.push_str(&format!(" AND {name_column} LIKE {}", literal(pattern)?));
            }
            sql.push_str(&format!(" ORDER BY schema, {selected_name}"));
            self.query_rows(&sql)?
                .iter()
                .map(|row| Ok((Some(text(row, 0)?), text(row, 1)?)))
                .collect::<NzResult<Vec<_>>>()?
        };
        let mut result = Vec::with_capacity(targets.len());
        for (schema, name) in targets {
            let quoted_name = quote_identifier(&name);
            let quoted_schema = schema.as_deref().map(quote_identifier);
            let ddl = match kind {
                DdlKind::Table => self.table_ddl(&quoted_name, quoted_schema.as_deref(), None),
                DdlKind::View => self.view_ddl(&quoted_name, quoted_schema.as_deref(), None),
                DdlKind::Procedure => {
                    self.procedure_ddl(&quoted_name, quoted_schema.as_deref(), None)
                }
            };
            match ddl {
                Ok(ddl) => result.push(NzDdlBatchResult {
                    schema: schema.unwrap_or_else(|| "UNKNOWN".into()),
                    name,
                    ddl,
                    error: None,
                }),
                Err(error) => result.push(NzDdlBatchResult {
                    schema: schema.unwrap_or_else(|| "UNKNOWN".into()),
                    name,
                    ddl: String::new(),
                    error: Some(error.to_string()),
                }),
            }
        }
        Ok(result)
    }
}

#[derive(Clone, Copy)]
enum DdlKind {
    Table,
    View,
    Procedure,
}

#[allow(clippy::too_many_arguments)]
fn build_table_ddl(
    database: &str,
    schema: &str,
    table: &str,
    columns: &[NzDetailedColumnInfo],
    distribution: &[String],
    organize: &[String],
    keys: &[NzTableKeyInfo],
    comment: Option<&str>,
) -> String {
    let name = qualified(database, schema, table);
    let column_lines = columns
        .iter()
        .map(|column| {
            let mut line = format!(
                "    {} {}",
                quote_identifier(&column.name),
                column.type_name
            );
            if column.not_null {
                line.push_str(" NOT NULL");
            }
            if let Some(default) = &column.default_value {
                line.push_str(&format!(" DEFAULT {default}"));
            }
            line
        })
        .collect::<Vec<_>>()
        .join(",\n");
    let mut lines = vec![format!("CREATE TABLE {name}"), "(".into(), column_lines];
    if distribution.is_empty() {
        lines.push(")\nDISTRIBUTE ON RANDOM".into());
    } else {
        lines.push(format!(
            ")\nDISTRIBUTE ON ({})",
            distribution
                .iter()
                .map(|part| quote_identifier(part))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    if !organize.is_empty() {
        lines.push(format!(
            "ORGANIZE ON ({})",
            organize
                .iter()
                .map(|part| quote_identifier(part))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    lines.extend([";".into(), String::new()]);
    for key in keys {
        let key_name = quote_identifier(&key.name);
        let columns = key
            .columns
            .iter()
            .map(|part| quote_identifier(part))
            .collect::<Vec<_>>()
            .join(", ");
        match key.type_char {
            'p' | 'u' => lines.push(format!(
                "ALTER TABLE {name} ADD CONSTRAINT {key_name} {} ({columns});",
                key.key_type
            )),
            'f' if !key.pk_columns.is_empty() => {
                if let (Some(db), Some(schema), Some(relation)) =
                    (&key.pk_database, &key.pk_schema, &key.pk_relation)
                {
                    let reference = qualified(db, schema, relation);
                    let reference_columns = key
                        .pk_columns
                        .iter()
                        .map(|part| quote_identifier(part))
                        .collect::<Vec<_>>()
                        .join(", ");
                    lines.push(format!(
                        "ALTER TABLE {name} ADD CONSTRAINT {key_name} {} ({columns}) REFERENCES {reference} ({reference_columns}) ON DELETE {} ON UPDATE {};",
                        key.key_type, key.delete_type, key.update_type
                    ));
                }
            }
            _ => {}
        }
    }
    if let Some(comment) = comment.filter(|comment| !comment.is_empty()) {
        lines.push(String::new());
        lines.push(format!(
            "COMMENT ON TABLE {name} IS '{}';",
            sql_string(comment)
        ));
    }
    for column in columns {
        if let Some(description) = column
            .description
            .as_deref()
            .filter(|value| !value.is_empty())
        {
            lines.push(format!(
                "COMMENT ON COLUMN {name}.{} IS '{}';",
                quote_identifier(&column.name),
                sql_string(description)
            ));
        }
    }
    lines.join("\n")
}

#[derive(Clone, Copy)]
enum ExternalOptionKind {
    String,
    Number,
    Boolean,
}

const EXTERNAL_OPTIONS: &[(&str, &str, ExternalOptionKind)] = &[
    ("DELIMITER", "DELIM", ExternalOptionKind::String),
    ("ENCODING", "ENCODING", ExternalOptionKind::String),
    ("TIMESTYLE", "TIMESTYLE", ExternalOptionKind::String),
    ("REMOTESOURCE", "REMOTESOURCE", ExternalOptionKind::String),
    ("SKIPROWS", "SKIPROWS", ExternalOptionKind::Number),
    ("MAXERRORS", "MAXERRORS", ExternalOptionKind::Number),
    ("ESCAPECHAR", "ESCAPE", ExternalOptionKind::String),
    ("DECIMALDELIM", "DECIMALDELIM", ExternalOptionKind::String),
    ("LOGDIR", "LOGDIR", ExternalOptionKind::String),
    ("QUOTEDVALUE", "QUOTEDVALUE", ExternalOptionKind::String),
    ("NULLVALUE", "NULLVALUE", ExternalOptionKind::String),
    ("CRINSTRING", "CRINSTRING", ExternalOptionKind::Boolean),
    ("TRUNCSTRING", "TRUNCSTRING", ExternalOptionKind::Boolean),
    ("CTRLCHARS", "CTRLCHARS", ExternalOptionKind::Boolean),
    ("IGNOREZERO", "IGNOREZERO", ExternalOptionKind::Boolean),
    (
        "TIMEEXTRAZEROS",
        "TIMEEXTRAZEROS",
        ExternalOptionKind::Boolean,
    ),
    ("Y2BASE", "Y2BASE", ExternalOptionKind::Number),
    ("FILLRECORD", "FILLRECORD", ExternalOptionKind::Boolean),
    ("COMPRESS", "COMPRESS", ExternalOptionKind::Boolean),
    (
        "INCLUDEHEADER",
        "INCLUDEHEADER",
        ExternalOptionKind::Boolean,
    ),
    ("LFINSTRING", "LFINSTRING", ExternalOptionKind::Boolean),
    ("DATESTYLE", "DATESTYLE", ExternalOptionKind::String),
    ("DATEDELIM", "DATEDELIM", ExternalOptionKind::String),
    ("TIMEDELIM", "TIMEDELIM", ExternalOptionKind::String),
    ("BOOLSTYLE", "BOOLSTYLE", ExternalOptionKind::String),
    ("FORMAT", "FORMAT", ExternalOptionKind::String),
    ("SOCKETBUFSIZE", "SOCKETBUFSIZE", ExternalOptionKind::Number),
    ("RECORDDELIM", "RECORDDELIM", ExternalOptionKind::String),
    ("MAXROWS", "MAXROWS", ExternalOptionKind::Number),
    (
        "REQUIREQUOTES",
        "REQUIREQUOTES",
        ExternalOptionKind::Boolean,
    ),
    ("RECORDLENGTH", "RECORDLENGTH", ExternalOptionKind::Number),
    ("DATETIMEDELIM", "DATETIMEDELIM", ExternalOptionKind::String),
    ("REJECTFILE", "REJECTFILE", ExternalOptionKind::String),
];

impl NzMetadata<'_> {
    /// Reconstruct CREATE EXTERNAL TABLE SQL including the catalog's USING options.
    pub fn external_table_ddl(
        &mut self,
        table: &str,
        schema: Option<&str>,
        database: Option<&str>,
    ) -> NzResult<String> {
        let (schema, table) = normalize_object_name(table, schema)?;
        let fields = EXTERNAL_OPTIONS
            .iter()
            .map(|(_, column, _)| format!("E.{column}"))
            .collect::<Vec<_>>()
            .join(", ");
        let mut sql = format!(
            "SELECT E.SCHEMA, E.TABLENAME, X.EXTOBJNAME, {fields} FROM _v_external E JOIN _v_extobject X ON E.RELID = X.OBJID WHERE E.TABLENAME = {}",
            literal(&table)?
        );
        if let Some(schema) = &schema {
            sql.push_str(&format!(" AND E.SCHEMA = {}", literal(schema)?));
        }
        let rows = self.query_rows(&sql)?;
        require_unique_schema(&rows, &table)?;
        let row = rows
            .first()
            .ok_or_else(|| NzError::Config(format!("external table {table} not found")))?;
        let schema = text(row, 0)?;
        let object = optional_text(row, 2)?;
        let database = match database {
            Some(database) => database.to_owned(),
            None => self.current_database()?.unwrap_or_else(|| "UNKNOWN".into()),
        };
        let column_sql = format!(
            "SELECT C.ATTNAME, C.FORMAT_TYPE, C.ATTNOTNULL FROM _v_relation_column C JOIN _v_external E ON C.OBJID = E.RELID WHERE E.SCHEMA = {} AND E.TABLENAME = {} ORDER BY C.ATTNUM",
            literal(&schema)?,
            literal(&table)?
        );
        let columns = self.query_rows(&column_sql)?;
        if columns.is_empty() {
            return Err(NzError::Config(format!(
                "external table {schema}.{table} has no columns"
            )));
        }
        let column_lines = columns
            .iter()
            .map(|column| {
                Ok(format!(
                    "    {} {}{}",
                    quote_identifier(&text(column, 0)?),
                    text(column, 1)?,
                    if bool_value(column, 2)? {
                        " NOT NULL"
                    } else {
                        ""
                    }
                ))
            })
            .collect::<NzResult<Vec<_>>>()?
            .join(",\n");
        let mut lines = vec![
            format!(
                "CREATE EXTERNAL TABLE {}",
                qualified(&database, &schema, &table)
            ),
            "(".into(),
            column_lines,
            ")".into(),
            "USING".into(),
            "(".into(),
        ];
        if let Some(object) = object {
            lines.push(format!("    DATAOBJECT('{}')", sql_string(&object)));
        }
        for (index, (keyword, _, kind)) in EXTERNAL_OPTIONS.iter().enumerate() {
            let Some(value) = optional_text(row, index + 3)? else {
                continue;
            };
            let rendered = match kind {
                ExternalOptionKind::String => format!("'{}'", sql_string(&value)),
                ExternalOptionKind::Number => value,
                ExternalOptionKind::Boolean => if bool_value(row, index + 3)? {
                    "true"
                } else {
                    "false"
                }
                .into(),
            };
            lines.push(format!("    {keyword} {rendered}"));
        }
        lines.push(");".into());
        Ok(lines.join("\n"))
    }

    /// Reconstruct CREATE SYNONYM SQL from catalog information.
    pub fn synonym_ddl(
        &mut self,
        synonym: &str,
        schema: Option<&str>,
        database: Option<&str>,
    ) -> NzResult<String> {
        let (schema, synonym) = normalize_object_name(synonym, schema)?;
        let mut sql = format!(
            "SELECT schema, owner, synonym_name, refobjname, description, refdatabase, refschema FROM _v_synonym WHERE synonym_name = {}",
            literal(&synonym)?
        );
        if let Some(schema) = &schema {
            sql.push_str(&format!(" AND schema = {}", literal(schema)?));
        }
        let rows = self.query_rows(&sql)?;
        require_unique_schema(&rows, &synonym)?;
        let row = rows
            .first()
            .ok_or_else(|| NzError::Config(format!("synonym {synonym} not found")))?;
        let schema = text(row, 0)?;
        let _owner = optional_text(row, 1)?;
        let reference = text(row, 3)?;
        let description = optional_text(row, 4)?;
        let reference_database = optional_text(row, 5)?;
        let reference_schema = optional_text(row, 6)?;
        let database = match database {
            Some(database) => database.to_owned(),
            None => self.current_database()?.unwrap_or_else(|| "UNKNOWN".into()),
        };
        let target = if reference.contains('.') {
            reference
                .split('.')
                .map(quote_identifier)
                .collect::<Vec<_>>()
                .join(".")
        } else if let (Some(db), Some(schema)) = (reference_database, reference_schema) {
            qualified(&db, &schema, &reference)
        } else {
            quote_identifier(&reference)
        };
        let mut ddl = format!(
            "CREATE SYNONYM {} FOR {target};",
            qualified(&database, &schema, &synonym)
        );
        if let Some(description) = description {
            ddl.push_str(&format!(
                "\nCOMMENT ON SYNONYM {} IS '{}';",
                quote_identifier(&synonym),
                sql_string(&description)
            ));
        }
        Ok(ddl)
    }
}
