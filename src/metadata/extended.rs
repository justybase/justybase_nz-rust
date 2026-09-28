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
    use super::{normalize_object_name, split_identifier_path};
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

    #[test]
    fn synonym_target_parser_keeps_quoted_dots_and_unescapes_quotes() {
        assert_eq!(
            split_identifier_path("\"Data.Schema\".\"Tar\"\"get\"").unwrap(),
            vec!["Data.Schema", "Tar\"get"]
        );
        assert_eq!(
            split_identifier_path("  \" Schema Name \" . \" Target Name \"  ").unwrap(),
            vec![" Schema Name ", " Target Name "]
        );
    }

    #[test]
    fn synonym_target_parser_preserves_omitted_schema_and_rejects_bad_paths() {
        assert_eq!(
            split_identifier_path("OTHER_DB..TARGET").unwrap(),
            vec!["OTHER_DB", "", "TARGET"]
        );
        assert!(split_identifier_path("DB....TARGET").is_err());
        assert!(split_identifier_path("A.B.C.D").is_err());
    }

    #[test]
    fn view_ddl_includes_escaped_object_and_column_comments() {
        let columns = [super::NzDetailedColumnInfo {
            schema: "ADMIN".into(),
            name: "SELECT".into(),
            ordinal: 1,
            type_name: "INTEGER".into(),
            not_null: false,
            default_value: None,
            description: Some("owner's identifier".into()),
        }];
        let ddl = super::build_view_ddl(
            "DB",
            "ADMIN",
            "V",
            "SELECT ID FROM T;",
            Some("owner's view"),
            &columns,
        );
        assert!(ddl.contains("COMMENT ON VIEW DB.ADMIN.V IS 'owner''s view';"));
        assert!(ddl.contains("COMMENT ON COLUMN DB.ADMIN.V.\"SELECT\" IS 'owner''s identifier';"));
    }

    #[test]
    fn external_layout_is_emitted_as_zone_syntax() {
        assert_eq!(
            super::format_external_layout("BYTES 4, BYTES 8"),
            "(BYTES 4, BYTES 8)"
        );
        assert_eq!(super::format_external_layout("(BYTES 4)"), "(BYTES 4)");
    }

    #[test]
    fn external_layout_reconstructs_zone_metadata() {
        let zones = [
            super::ExternalLayoutZoneInfo {
                use_type: "FILLER".into(),
                name: "F1".into(),
                type_name: "CHAR(2)".into(),
                style: "INTERNAL".into(),
                length: "BYTES 2".into(),
                ..Default::default()
            },
            super::ExternalLayoutZoneInfo {
                name: "SELECT".into(),
                type_name: "INT4".into(),
                style: "DECIMAL".into(),
                length: "BYTES 4".into(),
                null_if: "&&2 = ''".into(),
                ..Default::default()
            },
            super::ExternalLayoutZoneInfo {
                name: "DT".into(),
                type_name: "DATE".into(),
                style: "YMD".into(),
                delimiter: "-".into(),
                length: "BYTES 10".into(),
                ..Default::default()
            },
            super::ExternalLayoutZoneInfo {
                name: " DATE FIELD ".into(),
                type_name: "DATE".into(),
                style: "YMD".into(),
                delimiter: " ".into(),
                length: "BYTES 10".into(),
                ..Default::default()
            },
        ];
        assert_eq!(
            super::format_external_layout_zones(&zones).unwrap(),
            "FILLER F1 CHAR(2) INTERNAL BYTES 2, \"SELECT\" INT4 DECIMAL BYTES 4 NULLIF &&2 = '', DT DATE YMD '-' BYTES 10, \" DATE FIELD \" DATE YMD ' ' BYTES 10"
        );
        assert_eq!(super::layout_zone_count("0"), None);
        assert_eq!(super::layout_zone_count("3"), Some(3));
    }

    #[test]
    fn quotes_reserved_identifiers_and_underscore_prefixes() {
        assert_eq!(super::quote_identifier("SELECT"), "\"SELECT\"");
        assert_eq!(super::quote_identifier("_PRIVATE"), "\"_PRIVATE\"");
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
    const RESERVED: &str = "ABORT ALL ALLOCATE ANALYSE ANALYZE AND ANY AS ASC AUTOMAINT AWSS3 AZUREBLOB BETWEEN BINARY BIT BOTH CASE CAST CHAR CHARACTER CHECK CLUSTER COALESCE COLLATE COLLATION COLUMN CONSTRAINT COPY CROSS CURRENT CURRENT_CATALOG CURRENT_DATE CURRENT_DB CURRENT_SCHEMA CURRENT_SID CURRENT_TIME CURRENT_TIMESTAMP CURRENT_USER CURRENT_USERID CURRENT_USEROID DAYSPERROW DEALLOCATE DEC DECIMAL DECODE DEFAULT DEREGISTER DESC DISTINCT DISTRIBUTE DO ELSE END EXCEPT EXCLUDE EXISTS EXPLAIN EXPRESS EXTEND EXTERNAL EXTRACT FALSE FIRST FLOAT FOLLOWING FOR FOREIGN FROM FULL FUNCTION GENSTATS GLOBAL GROUP HAVING HISTOGRAM IDENTIFIER_CASE ILIKE IN INDEX INITIALLY INNER INOUT INTERSECT INTERVAL INTO JOURNAL LEADING LEFT LIKE LIMIT LOAD LOCAL LOCK MINUS MOVE NATURAL NCHAR NEW NOCASCADE NOT NOTNULL NULL NULLS NUMERIC NVL NVL2 OFFSET OFF OLD ON ONLINE ONLY OR ORDER OTHERS OUT OUTER OVER OVERLAPS PAUSESTEPS PAUSETIME PARTITION POSITION PRECEDING PRECISION PRESERVE PRIMARY REGISTER RESET REUSE RIGHT ROWS SELECT SESSION_USER SETOF SHOW SOME TABLE TEMPORAL THEN TIES TIME TIME_TRAVEL_ENABLE TIMESTAMP TO TRAILING TRANSACTION TRIGGER TRIM TRUE UNBOUNDED UNION UNIQUE USER USING VACUUM VARCHAR VERBOSE VERSION VIEW WHEN WHERE WITH WRITE CTID OID XMIN CMIN XMAX CMAX TABLEOID ROWID DATASLICEID CREATEXID DELETEXID";
    if !name.is_empty()
        && name
            .chars()
            .next()
            .is_some_and(|ch| ch.is_ascii_uppercase())
        && name
            .chars()
            .all(|ch| ch.is_ascii_uppercase() || ch.is_ascii_digit() || ch == '_')
        && !RESERVED.split_whitespace().any(|word| word == name)
    {
        name.to_owned()
    } else {
        format!("\"{}\"", name.replace('"', "\"\""))
    }
}

fn build_view_ddl(
    database: &str,
    schema: &str,
    view: &str,
    definition: &str,
    view_comment: Option<&str>,
    columns: &[NzDetailedColumnInfo],
) -> String {
    let name = qualified(database, schema, view);
    let mut lines = vec![
        format!("CREATE OR REPLACE VIEW {name} AS"),
        definition.to_owned(),
    ];
    if let Some(comment) = view_comment.filter(|value| !value.trim().is_empty()) {
        lines.push(String::new());
        lines.push(format!(
            "COMMENT ON VIEW {name} IS '{}';",
            sql_string(comment.trim())
        ));
    }
    for column in columns {
        if let Some(description) = column
            .description
            .as_deref()
            .filter(|value| !value.trim().is_empty())
        {
            lines.push(format!(
                "COMMENT ON COLUMN {name}.{} IS '{}';",
                quote_identifier(&column.name),
                sql_string(description.trim())
            ));
        }
    }
    lines.join("\n")
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
        let quoted_view = quote_identifier(&view);
        let quoted_schema = quote_identifier(&schema);
        let columns = self.detailed_columns(&quoted_view, Some(&quoted_schema))?;
        let comment = self.table_comment(&quoted_view, Some(&quoted_schema))?;
        Ok(build_view_ddl(
            &database,
            &schema,
            &view,
            &definition,
            comment.as_deref(),
            &columns,
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
        let signature = optional_text(row, 2)?.unwrap_or_default();
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
        if let Some(description) = description.filter(|value| !value.is_empty()) {
            let signature_open = signature.find('(');
            let signature_open = signature_open.ok_or_else(|| {
                NzError::Config(format!("procedure {name} has a comment but no signature"))
            })?;
            let comment_signature = &signature[signature_open..];
            ddl.push_str(&format!(
                "\nCOMMENT ON PROCEDURE {full_name}{comment_signature} IS '{}';",
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
    Compression,
    Layout,
}

#[derive(Default)]
struct ExternalLayoutZoneInfo {
    use_type: String,
    name: String,
    type_name: String,
    style: String,
    length: String,
    delimiter: String,
    around: String,
    null_if: String,
    endian: String,
    alignment: String,
    modulus: String,
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
    ("COMPRESS", "COMPRESS", ExternalOptionKind::Compression),
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
    ("LAYOUT", "LAYOUT", ExternalOptionKind::Layout),
    (
        "INCLUDEZEROSECONDS",
        "INCLUDEZEROSECONDS",
        ExternalOptionKind::Boolean,
    ),
    ("MERIDIANDELIM", "MERIDIANDELIM", ExternalOptionKind::String),
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
        let layout_index = EXTERNAL_OPTIONS
            .iter()
            .position(|(keyword, _, _)| *keyword == "LAYOUT")
            .expect("LAYOUT external option")
            + 3;
        let catalog_layout = optional_text(row, layout_index)?.unwrap_or_default();
        let layout = if let Some(expected_count) = layout_zone_count(&catalog_layout) {
            let zone_sql = format!(
                "SELECT Z.USETYPE, Z.NAME, Z.TYPE, Z.STYLE, Z.LENGTH, Z.DELIMITER, Z.AROUND, Z.NULLIF, Z.ENDIAN, Z.ALIGNMENT, Z.MODULUS FROM _v_external E JOIN _v_extzones Z ON E.RELID = Z.RELID WHERE E.SCHEMA = {} AND E.TABLENAME = {} ORDER BY Z.ZONEID",
                literal(&schema)?,
                literal(&table)?
            );
            let zone_rows = self.query_rows(&zone_sql)?;
            if zone_rows.len() != expected_count {
                return Err(NzError::Config(format!(
                    "cannot reconstruct external table LAYOUT: catalog reports {expected_count} zones, but _V_EXTZONES returned {}",
                    zone_rows.len()
                )));
            }
            let zones = zone_rows
                .iter()
                .map(|zone| {
                    Ok(ExternalLayoutZoneInfo {
                        use_type: optional_text(zone, 0)?.unwrap_or_default(),
                        name: optional_text(zone, 1)?.unwrap_or_default(),
                        type_name: optional_text(zone, 2)?.unwrap_or_default(),
                        style: optional_text(zone, 3)?.unwrap_or_default(),
                        length: optional_text(zone, 4)?.unwrap_or_default(),
                        delimiter: optional_text(zone, 5)?.unwrap_or_default(),
                        around: optional_text(zone, 6)?.unwrap_or_default(),
                        null_if: optional_text(zone, 7)?.unwrap_or_default(),
                        endian: optional_text(zone, 8)?.unwrap_or_default(),
                        alignment: optional_text(zone, 9)?.unwrap_or_default(),
                        modulus: optional_text(zone, 10)?.unwrap_or_default(),
                    })
                })
                .collect::<NzResult<Vec<_>>>()?;
            Some(format_external_layout_zones(&zones)?)
        } else if catalog_layout.trim().parse::<usize>().is_ok() || catalog_layout.trim().is_empty()
        {
            None
        } else {
            Some(catalog_layout)
        };
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
            let value = if matches!(kind, ExternalOptionKind::Layout) {
                layout.clone()
            } else {
                optional_text(row, index + 3)?
            };
            let Some(value) = value else {
                continue;
            };
            let rendered = match kind {
                ExternalOptionKind::String => format!("'{}'", sql_string(&value)),
                ExternalOptionKind::Layout => format_external_layout(&value),
                ExternalOptionKind::Number => value.clone(),
                ExternalOptionKind::Compression => {
                    let normalized = value.trim().to_ascii_lowercase();
                    if ["true", "t", "1", "yes", "on"].contains(&normalized.as_str()) {
                        "true".into()
                    } else if ["false", "f", "0", "no", "off"].contains(&normalized.as_str()) {
                        "false".into()
                    } else {
                        value.clone()
                    }
                }
                ExternalOptionKind::Boolean => if bool_value(row, index + 3)? {
                    "true"
                } else {
                    "false"
                }
                .into(),
            };
            if matches!(kind, ExternalOptionKind::Layout) && rendered.is_empty() {
                continue;
            }
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
        let mut reference_parts = split_identifier_path(&reference)?;
        if reference_parts.len() == 1 {
            if let Some(db) = reference_database {
                reference_parts.splice(0..0, [db, reference_schema.unwrap_or_default()]);
            } else if let Some(schema) = reference_schema {
                reference_parts.insert(0, schema);
            }
        } else if reference_parts.len() == 2 {
            if let Some(db) = reference_database {
                reference_parts.insert(0, db);
            }
        }
        let target = reference_parts
            .iter()
            .map(|part| {
                if part.is_empty() {
                    String::new()
                } else {
                    quote_identifier(part)
                }
            })
            .collect::<Vec<_>>()
            .join(".");
        let mut ddl = format!(
            "CREATE SYNONYM {} FOR {target};",
            qualified(&database, &schema, &synonym)
        );
        if let Some(description) = description.filter(|value| !value.is_empty()) {
            ddl.push_str(&format!(
                "\nCOMMENT ON SYNONYM {} IS '{}';",
                qualified(&database, &schema, &synonym),
                sql_string(&description)
            ));
        }
        Ok(ddl)
    }
}

fn layout_zone_count(value: &str) -> Option<usize> {
    value
        .trim()
        .parse::<usize>()
        .ok()
        .filter(|count| *count > 0)
}

fn format_external_layout_zones(zones: &[ExternalLayoutZoneInfo]) -> NzResult<String> {
    let mut definitions = Vec::with_capacity(zones.len());
    for (index, zone) in zones.iter().enumerate() {
        let use_type = zone.use_type.trim().to_ascii_uppercase();
        if !use_type.is_empty() && use_type != "REF" && use_type != "FILLER" {
            return Err(NzError::Config(format!(
                "cannot reconstruct external table LAYOUT: unsupported zone use type {use_type}"
            )));
        }
        let length = zone.length.trim();
        if length.is_empty() {
            return Err(NzError::Config(format!(
                "cannot reconstruct external table LAYOUT: zone {} has no length",
                index + 1
            )));
        }
        for (field, value) in [
            ("AROUND", zone.around.as_str()),
            ("ENDIAN", zone.endian.as_str()),
            ("ALIGNMENT", zone.alignment.as_str()),
            ("MODULUS", zone.modulus.as_str()),
        ] {
            if !value.trim().is_empty() {
                return Err(NzError::Config(format!(
                    "cannot reconstruct external table LAYOUT: zone {} uses unsupported {field} metadata",
                    index + 1
                )));
            }
        }
        let style = zone.style.trim();
        let mut parts = Vec::new();
        if !use_type.is_empty() {
            parts.push(use_type);
        }
        if !zone.name.is_empty() {
            parts.push(quote_identifier(&zone.name));
        }
        if !zone.type_name.trim().is_empty() {
            parts.push(zone.type_name.trim().to_owned());
        }
        if !style.is_empty() {
            parts.push(style.to_owned());
        }
        if !zone.delimiter.is_empty() {
            if style.is_empty() {
                return Err(NzError::Config(format!(
                    "cannot reconstruct external table LAYOUT: zone {} has a delimiter without a style",
                    index + 1
                )));
            }
            if !style.contains('\'') {
                parts.push(format!("'{}'", sql_string(&zone.delimiter)));
            }
        }
        parts.push(length.to_owned());
        let null_if = zone.null_if.trim();
        if !null_if.is_empty() {
            if null_if.to_ascii_uppercase().starts_with("NULLIF") {
                parts.push(null_if.to_owned());
            } else {
                parts.push(format!("NULLIF {null_if}"));
            }
        }
        definitions.push(parts.join(" "));
    }
    Ok(definitions.join(", "))
}

fn format_external_layout(value: &str) -> String {
    let layout = value.trim();
    if layout.is_empty() {
        String::new()
    } else if layout.starts_with('(') && layout.ends_with(')') {
        layout.to_owned()
    } else {
        format!("({layout})")
    }
}

fn split_identifier_path(value: &str) -> NzResult<Vec<String>> {
    let mut raw_parts = Vec::new();
    let mut current = String::new();
    let mut chars = value.chars().peekable();
    let mut quoted = false;
    while let Some(ch) = chars.next() {
        match ch {
            '"' if quoted && chars.peek() == Some(&'"') => {
                current.push_str("\"\"");
                chars.next();
            }
            '"' => {
                current.push(ch);
                quoted = !quoted;
            }
            '.' if !quoted => {
                raw_parts.push(std::mem::take(&mut current));
            }
            _ => current.push(ch),
        }
    }
    if quoted {
        return Err(NzError::Config("invalid synonym target".into()));
    }
    raw_parts.push(current);
    let mut parts = Vec::with_capacity(raw_parts.len());
    for raw_part in raw_parts {
        let part = raw_part.trim();
        if !part.starts_with('"') {
            if part.contains('"') {
                return Err(NzError::Config("invalid synonym target".into()));
            }
            parts.push(part.to_owned());
            continue;
        }
        if part.len() < 2 || !part.ends_with('"') {
            return Err(NzError::Config("invalid synonym target".into()));
        }
        let mut identifier = String::new();
        let mut chars = part[1..part.len() - 1].chars().peekable();
        while let Some(ch) = chars.next() {
            if ch == '"' {
                if chars.next() != Some('"') {
                    return Err(NzError::Config("invalid synonym target".into()));
                }
                identifier.push('"');
            } else {
                identifier.push(ch);
            }
        }
        parts.push(identifier);
    }
    let has_omitted_schema =
        parts.len() == 3 && !parts[0].is_empty() && parts[1].is_empty() && !parts[2].is_empty();
    if parts.len() > 3 || (parts.iter().any(String::is_empty) && !has_omitted_schema) {
        return Err(NzError::Config("invalid synonym target".into()));
    }
    Ok(parts)
}
