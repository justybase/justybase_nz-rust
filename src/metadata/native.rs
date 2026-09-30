//! Native Tokio catalog operations sharing SQL and row decoders with blocking metadata.
use super::extended::{
    bool_value, build_table_ddl, build_view_ddl, fixed_return_type, format_external_layout,
    format_external_layout_zones, layout_zone_count, missing_relation, normalize_object_name,
    qualified, quote_identifier, require_unique_schema, split_identifier_path, sql_string,
    ExternalLayoutZoneInfo, ExternalOptionKind, EXTERNAL_OPTIONS,
};
use super::*;
use super::{optional_text as extended_optional_text, text as extended_text};
use crate::Client;
use std::collections::HashMap;

/// Catalog helpers borrowing a native client.
pub struct AsyncMetadata<'a> {
    client: &'a Client,
}
/// Related catalog lists retrieved in one protocol request.
#[derive(Debug, Clone)]
pub struct CatalogSnapshot {
    pub schemas: Vec<String>,
    pub databases: Vec<NzDatabaseInfo>,
    pub tables: Vec<NzTableInfo>,
    pub views: Vec<NzViewInfo>,
    pub procedures: Vec<NzProcedureInfo>,
}
impl Client {
    pub fn metadata(&self) -> AsyncMetadata<'_> {
        AsyncMetadata { client: self }
    }
}
impl AsyncMetadata<'_> {
    async fn query_rows(&self, sql: &str) -> NzResult<Vec<crate::connection::Row>> {
        self.client.query(sql, &[]).await
    }

    pub async fn current_database(&self) -> NzResult<Option<String>> {
        self.query_rows("SELECT current_catalog")
            .await?
            .first()
            .map(|row| super::optional_text(row, 0))
            .transpose()
            .map(Option::flatten)
    }

    pub async fn schemas(&self) -> NzResult<Vec<String>> {
        self.client
            .query("SELECT schema FROM _v_schema ORDER BY schema", &[])
            .await?
            .iter()
            .map(|row| row.try_get(0))
            .collect()
    }
    pub async fn databases(&self) -> NzResult<Vec<NzDatabaseInfo>> {
        self.client
            .query(
                "SELECT database, owner, defschema FROM _v_database ORDER BY database",
                &[],
            )
            .await?
            .iter()
            .map(database_from_row)
            .collect()
    }
    pub async fn tables(
        &self,
        schema: Option<&str>,
        pattern: Option<&str>,
    ) -> NzResult<Vec<NzTableInfo>> {
        self.client
            .query(&tables_sql(schema, pattern)?, &[])
            .await?
            .iter()
            .map(table_from_row)
            .collect()
    }
    pub async fn columns(&self, table: &str, schema: Option<&str>) -> NzResult<Vec<NzColumnInfo>> {
        self.client
            .query(&columns_sql(table, schema)?, &[])
            .await?
            .iter()
            .map(column_from_row)
            .collect()
    }
    pub async fn views(&self, schema: Option<&str>) -> NzResult<Vec<NzViewInfo>> {
        self.client
            .query(&views_sql(schema)?, &[])
            .await?
            .iter()
            .map(view_from_row)
            .collect()
    }
    pub async fn procedures(&self, schema: Option<&str>) -> NzResult<Vec<NzProcedureInfo>> {
        self.client
            .query(&procedures_sql(schema)?, &[])
            .await?
            .iter()
            .map(procedure_from_row)
            .collect()
    }

    /// Detailed column definitions, defaults and comments for DDL generation.
    pub async fn detailed_columns(
        &self,
        table: &str,
        schema: Option<&str>,
    ) -> NzResult<Vec<NzDetailedColumnInfo>> {
        let (schema, table) = normalize_object_name(table, schema)?;
        let mut sql = format!(
            "SELECT schema, attname, attnum, format_type, attnotnull, coldefault, description FROM _v_relation_column WHERE name = {}",
            super::literal(&table)?
        );
        if let Some(schema) = &schema {
            sql.push_str(&format!(" AND schema = {}", super::literal(schema)?));
        }
        sql.push_str(" ORDER BY attnum");
        let rows = self.query_rows(&sql).await?;
        if schema.is_none() {
            require_unique_schema(&rows, &table)?;
        }
        rows.iter()
            .map(|row| {
                Ok(NzDetailedColumnInfo {
                    schema: extended_text(row, 0)?,
                    name: extended_text(row, 1)?,
                    ordinal: row.try_get(2)?,
                    type_name: extended_text(row, 3)?,
                    not_null: bool_value(row, 4)?,
                    default_value: extended_optional_text(row, 5)?,
                    description: extended_optional_text(row, 6)?,
                })
            })
            .collect()
    }

    /// Reconstruct executable CREATE TABLE SQL from catalog metadata.
    pub async fn table_ddl(
        &self,
        table: &str,
        schema: Option<&str>,
        database: Option<&str>,
    ) -> NzResult<String> {
        let names = [table.to_owned()];
        let mut results = self
            .table_ddl_batch(schema, None, Some(&names), database)
            .await?;
        let result = results
            .pop()
            .ok_or_else(|| NzError::Config(format!("table {table} not found")))?;
        result
            .error
            .map_or(Ok(result.ddl), |message| Err(NzError::Config(message)))
    }

    /// Reconstruct CREATE OR REPLACE VIEW SQL and catalog comments.
    pub async fn view_ddl(
        &self,
        view: &str,
        schema: Option<&str>,
        database: Option<&str>,
    ) -> NzResult<String> {
        let (schema, view) = normalize_object_name(view, schema)?;
        let mut sql = format!(
            "SELECT schema, viewname, definition FROM _v_view WHERE viewname = {}",
            super::literal(&view)?
        );
        if let Some(schema) = &schema {
            sql.push_str(&format!(" AND schema = {}", super::literal(schema)?));
        }
        sql.push_str(" ORDER BY schema, viewname");
        let rows = self.query_rows(&sql).await?;
        require_unique_schema(&rows, &view)?;
        let row = rows
            .first()
            .ok_or_else(|| NzError::Config(format!("view {view} not found")))?;
        let schema = extended_text(row, 0)?;
        let definition = extended_optional_text(row, 2)?
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| NzError::Config(format!("view {schema}.{view} has no definition")))?;
        let database = match database {
            Some(database) => database.to_owned(),
            None => self
                .current_database()
                .await?
                .unwrap_or_else(|| "UNKNOWN".into()),
        };
        let quoted_view = quote_identifier(&view);
        let quoted_schema = quote_identifier(&schema);
        let columns = self
            .detailed_columns(&quoted_view, Some(&quoted_schema))
            .await?;
        let comment = self
            .table_comment(&quoted_view, Some(&quoted_schema))
            .await?;
        Ok(build_view_ddl(
            &database,
            &schema,
            &view,
            &definition,
            comment.as_deref(),
            &columns,
        ))
    }

    /// Reconstruct CREATE OR REPLACE PROCEDURE SQL, including its source body.
    pub async fn procedure_ddl(
        &self,
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
            super::literal(&procedure)?
        );
        if let Some(schema) = &schema {
            sql.push_str(&format!(" AND schema = {}", super::literal(schema)?));
        }
        sql.push_str(" ORDER BY proceduresignature");
        let rows = self.query_rows(&sql).await?;
        require_unique_schema(&rows, &procedure)?;
        if rows.len() > 1 {
            return Err(NzError::Config(format!(
                "procedure {procedure} has multiple overloads; pass its signature"
            )));
        }
        let row = rows
            .first()
            .ok_or_else(|| NzError::Config(format!("procedure {procedure} not found")))?;
        let database = match database {
            Some(database) => database.to_owned(),
            None => self
                .current_database()
                .await?
                .unwrap_or_else(|| "UNKNOWN".into()),
        };
        render_procedure_ddl(row, &database)
    }

    /// Reconstruct external-table DDL, including FIXED layout zones.
    pub async fn external_table_ddl(
        &self,
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
            super::literal(&table)?
        );
        if let Some(schema) = &schema {
            sql.push_str(&format!(" AND E.SCHEMA = {}", super::literal(schema)?));
        }
        let rows = self.query_rows(&sql).await?;
        require_unique_schema(&rows, &table)?;
        let row = rows
            .first()
            .ok_or_else(|| NzError::Config(format!("external table {table} not found")))?;
        let schema = extended_text(row, 0)?;
        let object = extended_optional_text(row, 2)?;
        let layout_index = EXTERNAL_OPTIONS
            .iter()
            .position(|(keyword, _, _)| *keyword == "LAYOUT")
            .expect("LAYOUT option exists")
            + 3;
        let catalog_layout = extended_optional_text(row, layout_index)?.unwrap_or_default();
        let layout = if let Some(expected_count) = layout_zone_count(&catalog_layout) {
            let zone_sql = format!(
                "SELECT Z.USETYPE, Z.NAME, Z.TYPE, Z.STYLE, Z.LENGTH, Z.DELIMITER, Z.AROUND, Z.NULLIF, Z.ENDIAN, Z.ALIGNMENT, Z.MODULUS FROM _v_external E JOIN _v_extzones Z ON E.RELID = Z.RELID WHERE E.SCHEMA = {} AND E.TABLENAME = {} ORDER BY Z.ZONEID",
                super::literal(&schema)?,
                super::literal(&table)?
            );
            let zone_rows = self.query_rows(&zone_sql).await?;
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
                        use_type: extended_optional_text(zone, 0)?.unwrap_or_default(),
                        name: extended_optional_text(zone, 1)?.unwrap_or_default(),
                        type_name: extended_optional_text(zone, 2)?.unwrap_or_default(),
                        style: extended_optional_text(zone, 3)?.unwrap_or_default(),
                        length: extended_optional_text(zone, 4)?.unwrap_or_default(),
                        delimiter: extended_optional_text(zone, 5)?.unwrap_or_default(),
                        around: extended_optional_text(zone, 6)?.unwrap_or_default(),
                        null_if: extended_optional_text(zone, 7)?.unwrap_or_default(),
                        endian: extended_optional_text(zone, 8)?.unwrap_or_default(),
                        alignment: extended_optional_text(zone, 9)?.unwrap_or_default(),
                        modulus: extended_optional_text(zone, 10)?.unwrap_or_default(),
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
            None => self
                .current_database()
                .await?
                .unwrap_or_else(|| "UNKNOWN".into()),
        };
        let column_sql = format!(
            "SELECT C.ATTNAME, C.FORMAT_TYPE, C.ATTNOTNULL FROM _v_relation_column C JOIN _v_external E ON C.OBJID = E.RELID WHERE E.SCHEMA = {} AND E.TABLENAME = {} ORDER BY C.ATTNUM",
            super::literal(&schema)?,
            super::literal(&table)?
        );
        let columns = self.query_rows(&column_sql).await?;
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
                    quote_identifier(&extended_text(column, 0)?),
                    extended_text(column, 1)?,
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
                extended_optional_text(row, index + 3)?
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

    /// Reconstruct CREATE SYNONYM SQL and its optional comment.
    pub async fn synonym_ddl(
        &self,
        synonym: &str,
        schema: Option<&str>,
        database: Option<&str>,
    ) -> NzResult<String> {
        let (schema, synonym) = normalize_object_name(synonym, schema)?;
        let mut sql = format!(
            "SELECT schema, owner, synonym_name, refobjname, description, refdatabase, refschema FROM _v_synonym WHERE synonym_name = {}",
            super::literal(&synonym)?
        );
        if let Some(schema) = &schema {
            sql.push_str(&format!(" AND schema = {}", super::literal(schema)?));
        }
        let rows = self.query_rows(&sql).await?;
        require_unique_schema(&rows, &synonym)?;
        let row = rows
            .first()
            .ok_or_else(|| NzError::Config(format!("synonym {synonym} not found")))?;
        let schema = extended_text(row, 0)?;
        let reference = extended_text(row, 3)?;
        let description = extended_optional_text(row, 4)?;
        let reference_database = extended_optional_text(row, 5)?;
        let reference_schema = extended_optional_text(row, 6)?;
        let database = match database {
            Some(database) => database.to_owned(),
            None => self
                .current_database()
                .await?
                .unwrap_or_else(|| "UNKNOWN".into()),
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
        let full_name = qualified(&database, &schema, &synonym);
        let mut ddl = format!("CREATE SYNONYM {full_name} FOR {target};");
        if let Some(description) = description.filter(|value| !value.is_empty()) {
            ddl.push_str(&format!(
                "\nCOMMENT ON SYNONYM {full_name} IS '{}';",
                sql_string(&description)
            ));
        }
        Ok(ddl)
    }

    pub async fn distribution_key(
        &self,
        table: &str,
        schema: Option<&str>,
    ) -> NzResult<Vec<String>> {
        let (schema, table) = normalize_object_name(table, schema)?;
        let mut sql = format!(
            "SELECT attname FROM _v_table_dist_map WHERE tablename = {}",
            super::literal(&table)?
        );
        if let Some(schema) = schema {
            sql.push_str(&format!(" AND schema = {}", super::literal(&schema)?));
        }
        sql.push_str(" ORDER BY distattnum");
        self.query_rows(&sql)
            .await?
            .iter()
            .map(|row| extended_text(row, 0))
            .collect()
    }

    pub async fn organize_columns(
        &self,
        table: &str,
        schema: Option<&str>,
    ) -> NzResult<Vec<String>> {
        let (schema, table) = normalize_object_name(table, schema)?;
        let mut sql = format!(
            "SELECT attname FROM _v_table_organize_column WHERE tablename = {}",
            super::literal(&table)?
        );
        if let Some(schema) = schema {
            sql.push_str(&format!(" AND schema = {}", super::literal(&schema)?));
        }
        sql.push_str(" ORDER BY orgseqno");
        let rows = match self.query_rows(&sql).await {
            Ok(rows) => rows,
            Err(error) if missing_relation(&error) => return Ok(Vec::new()),
            Err(error) => return Err(error),
        };
        rows.iter().map(|row| extended_text(row, 0)).collect()
    }

    pub async fn table_keys(
        &self,
        table: &str,
        schema: Option<&str>,
    ) -> NzResult<Vec<NzTableKeyInfo>> {
        let (schema, table) = normalize_object_name(table, schema)?;
        let mut sql = format!(
            "SELECT constraintname, contype, attname, pkdatabase, pkschema, pkrelation, pkattname, updt_type, del_type FROM _v_relation_keydata WHERE relation = {}",
            super::literal(&table)?
        );
        if let Some(schema) = schema {
            sql.push_str(&format!(" AND schema = {}", super::literal(&schema)?));
        }
        sql.push_str(" ORDER BY constraintname, conseq");
        let rows = match self.query_rows(&sql).await {
            Ok(rows) => rows,
            Err(error) if missing_relation(&error) => return Ok(Vec::new()),
            Err(error) => return Err(error),
        };
        let mut keys: Vec<NzTableKeyInfo> = Vec::new();
        for row in &rows {
            let name = extended_text(row, 0)?;
            if keys.last().is_none_or(|key| key.name != name) {
                let type_char = extended_text(row, 1)?.chars().next().unwrap_or('?');
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
                    pk_database: extended_optional_text(row, 3)?,
                    pk_schema: extended_optional_text(row, 4)?,
                    pk_relation: extended_optional_text(row, 5)?,
                    pk_columns: Vec::new(),
                    update_type: extended_optional_text(row, 7)?
                        .unwrap_or_else(|| "NO ACTION".into()),
                    delete_type: extended_optional_text(row, 8)?
                        .unwrap_or_else(|| "NO ACTION".into()),
                });
            }
            let key = keys.last_mut().expect("key inserted above");
            if let Some(column) = extended_optional_text(row, 2)? {
                key.columns.push(column);
            }
            if let Some(column) = extended_optional_text(row, 6)? {
                key.pk_columns.push(column);
            }
        }
        Ok(keys)
    }

    pub async fn table_comment(
        &self,
        table: &str,
        schema: Option<&str>,
    ) -> NzResult<Option<String>> {
        let (schema, table) = normalize_object_name(table, schema)?;
        let mut where_clause = format!("objname = {}", super::literal(&table)?);
        if let Some(schema) = schema {
            where_clause.push_str(&format!(" AND schema = {}", super::literal(&schema)?));
        }
        for suffix in [" AND objtype = 'TABLE'", ""] {
            let sql =
                format!("SELECT description FROM _v_object_data WHERE {where_clause}{suffix}");
            let rows = match self.query_rows(&sql).await {
                Ok(rows) => rows,
                Err(error) if missing_relation(&error) => return Ok(None),
                Err(error) => return Err(error),
            };
            for row in &rows {
                if let Some(comment) = extended_optional_text(row, 0)? {
                    if !comment.trim().is_empty() {
                        return Ok(Some(comment));
                    }
                }
            }
        }
        Ok(None)
    }

    async fn table_ddl_batch(
        &self,
        schema: Option<&str>,
        pattern: Option<&str>,
        names: Option<&[String]>,
        database: Option<&str>,
    ) -> NzResult<Vec<NzDdlBatchResult>> {
        let targets = if let Some(names) = names {
            names
                .iter()
                .map(|name| normalize_object_name(name, schema))
                .collect::<NzResult<Vec<_>>>()?
        } else {
            let mut sql =
                String::from("SELECT schema, tablename FROM _v_table WHERE tablename IS NOT NULL");
            if let Some(schema) = schema {
                sql.push_str(&format!(" AND schema = {}", super::literal(schema)?));
            }
            if let Some(pattern) = pattern {
                sql.push_str(&format!(" AND tablename LIKE {}", super::literal(pattern)?));
            }
            sql.push_str(" ORDER BY schema, tablename");
            self.query_rows(&sql)
                .await?
                .iter()
                .map(|row| Ok((Some(extended_text(row, 0)?), extended_text(row, 1)?)))
                .collect::<NzResult<Vec<_>>>()?
        };
        if targets.is_empty() {
            return Ok(Vec::new());
        }

        let filter = target_filter(&targets, "schema", "name")?;
        let table_filter = target_filter(&targets, "schema", "tablename")?;
        let relation_filter = target_filter(&targets, "schema", "relation")?;
        let object_filter = target_filter(&targets, "schema", "objname")?;
        let statements = [
            "SELECT current_catalog".to_owned(),
            format!("SELECT schema, name, attname, attnum, format_type, attnotnull, coldefault, description FROM _v_relation_column WHERE {filter} ORDER BY schema, name, attnum"),
            format!("SELECT schema, tablename, attname FROM _v_table_dist_map WHERE {table_filter} ORDER BY schema, tablename, distattnum"),
            format!("SELECT schema, tablename, attname FROM _v_table_organize_column WHERE {table_filter} ORDER BY schema, tablename, orgseqno"),
            format!("SELECT schema, relation, constraintname, contype, attname, pkdatabase, pkschema, pkrelation, pkattname, updt_type, del_type FROM _v_relation_keydata WHERE {relation_filter} ORDER BY schema, relation, constraintname, conseq"),
            format!("SELECT schema, objname, description FROM _v_object_data WHERE {object_filter} AND objtype = 'TABLE'"),
        ];
        let result = self.client.query_multi(&statements.join("; "), &[]).await?;
        if result.result_sets.len() != statements.len() {
            return Err(NzError::Protocol(format!(
                "table DDL catalog batch expected {} result sets, got {}",
                statements.len(),
                result.result_sets.len()
            )));
        }
        let rows = |index: usize| &result.result_sets[index].rows;
        let database = database
            .map(str::to_owned)
            .or(rows(0)
                .first()
                .map(|row| extended_optional_text(row, 0))
                .transpose()?
                .flatten())
            .unwrap_or_else(|| "UNKNOWN".into());

        let mut columns: HashMap<(String, String), Vec<NzDetailedColumnInfo>> = HashMap::new();
        for row in rows(1) {
            let key = (extended_text(row, 0)?, extended_text(row, 1)?);
            columns.entry(key).or_default().push(NzDetailedColumnInfo {
                schema: extended_text(row, 0)?,
                name: extended_text(row, 2)?,
                ordinal: row.try_get(3)?,
                type_name: extended_text(row, 4)?,
                not_null: bool_value(row, 5)?,
                default_value: extended_optional_text(row, 6)?,
                description: extended_optional_text(row, 7)?,
            });
        }
        let mut distribution: HashMap<(String, String), Vec<String>> = HashMap::new();
        for row in rows(2) {
            distribution
                .entry((extended_text(row, 0)?, extended_text(row, 1)?))
                .or_default()
                .push(extended_text(row, 2)?);
        }
        let mut organize: HashMap<(String, String), Vec<String>> = HashMap::new();
        for row in rows(3) {
            organize
                .entry((extended_text(row, 0)?, extended_text(row, 1)?))
                .or_default()
                .push(extended_text(row, 2)?);
        }
        let mut keys: HashMap<(String, String), Vec<NzTableKeyInfo>> = HashMap::new();
        for row in rows(4) {
            let object = (extended_text(row, 0)?, extended_text(row, 1)?);
            let name = extended_text(row, 2)?;
            let object_keys = keys.entry(object).or_default();
            if object_keys.last().is_none_or(|key| key.name != name) {
                let type_char = extended_text(row, 3)?.chars().next().unwrap_or('?');
                object_keys.push(NzTableKeyInfo {
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
                    pk_database: extended_optional_text(row, 5)?,
                    pk_schema: extended_optional_text(row, 6)?,
                    pk_relation: extended_optional_text(row, 7)?,
                    pk_columns: Vec::new(),
                    update_type: extended_optional_text(row, 9)?
                        .unwrap_or_else(|| "NO ACTION".into()),
                    delete_type: extended_optional_text(row, 10)?
                        .unwrap_or_else(|| "NO ACTION".into()),
                });
            }
            let key = object_keys.last_mut().expect("key was added above");
            if let Some(column) = extended_optional_text(row, 4)? {
                key.columns.push(column);
            }
            if let Some(column) = extended_optional_text(row, 8)? {
                key.pk_columns.push(column);
            }
        }
        let mut comments: HashMap<(String, String), String> = HashMap::new();
        for row in rows(5) {
            let Some(comment) = extended_optional_text(row, 2)? else {
                continue;
            };
            if !comment.trim().is_empty() {
                comments
                    .entry((extended_text(row, 0)?, extended_text(row, 1)?))
                    .or_insert(comment);
            }
        }

        let mut results = Vec::with_capacity(targets.len());
        for (requested_schema, name) in targets {
            let matches = columns
                .keys()
                .filter(|(candidate_schema, candidate_name)| {
                    candidate_name == &name
                        && requested_schema
                            .as_ref()
                            .is_none_or(|schema| schema == candidate_schema)
                })
                .cloned()
                .collect::<Vec<_>>();
            let built = match matches.as_slice() {
                [] => Err(NzError::Config(format!("table {name} not found"))),
                [key] => {
                    let detail = &columns[key];
                    let ddl = build_table_ddl(
                        &database,
                        &key.0,
                        &name,
                        detail,
                        distribution.get(key).map(Vec::as_slice).unwrap_or_default(),
                        organize.get(key).map(Vec::as_slice).unwrap_or_default(),
                        keys.get(key).map(Vec::as_slice).unwrap_or_default(),
                        comments.get(key).map(String::as_str),
                    );
                    Ok((key.0.clone(), ddl))
                }
                _ => Err(NzError::Config(format!(
                    "{name} exists in multiple schemas; pass schema explicitly"
                ))),
            };
            match built {
                Ok((schema, ddl)) => results.push(NzDdlBatchResult {
                    schema,
                    name,
                    ddl,
                    error: None,
                }),
                Err(error) => results.push(NzDdlBatchResult {
                    schema: requested_schema.unwrap_or_else(|| "UNKNOWN".into()),
                    name,
                    ddl: String::new(),
                    error: Some(error.to_string()),
                }),
            }
        }
        Ok(results)
    }

    async fn procedure_ddl_batch(
        &self,
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
            let mut sql = String::from(
                "SELECT schema, proceduresignature FROM _v_procedure WHERE procedure IS NOT NULL",
            );
            if let Some(schema) = schema {
                sql.push_str(&format!(" AND schema = {}", super::literal(schema)?));
            }
            if let Some(pattern) = pattern {
                sql.push_str(&format!(" AND procedure LIKE {}", super::literal(pattern)?));
            }
            sql.push_str(" ORDER BY schema, proceduresignature");
            self.query_rows(&sql)
                .await?
                .iter()
                .map(|row| Ok((Some(extended_text(row, 0)?), extended_text(row, 1)?)))
                .collect::<NzResult<Vec<_>>>()?
        };
        if targets.is_empty() {
            return Ok(Vec::new());
        }
        let clauses = targets
            .iter()
            .map(|(schema, name)| {
                let (column, value) = if name.contains('(') {
                    ("proceduresignature", name.as_str())
                } else {
                    ("procedure", name.as_str())
                };
                let mut clause = format!("{column} = {}", super::literal(value)?);
                if let Some(schema) = schema {
                    clause.push_str(&format!(" AND schema = {}", super::literal(schema)?));
                }
                Ok(format!("({clause})"))
            })
            .collect::<NzResult<Vec<_>>>()?;
        let statements = [
            "SELECT current_catalog".to_owned(),
            format!(
                "SELECT schema, procedure, proceduresignature, arguments, returns, executedasowner, description, proceduresource FROM _v_procedure WHERE {} ORDER BY schema, proceduresignature",
                clauses.join(" OR ")
            ),
        ];
        let result = self.client.query_multi(&statements.join("; "), &[]).await?;
        if result.result_sets.len() != statements.len() {
            return Err(NzError::Protocol(format!(
                "procedure DDL catalog batch expected {} result sets, got {}",
                statements.len(),
                result.result_sets.len()
            )));
        }
        let database = result.result_sets[0]
            .rows
            .first()
            .map(|row| extended_optional_text(row, 0))
            .transpose()?
            .flatten()
            .unwrap_or_else(|| "UNKNOWN".into());
        let catalog = &result.result_sets[1].rows;
        let mut results = Vec::with_capacity(targets.len());
        for (requested_schema, name) in targets {
            let mut matches = Vec::new();
            for row in catalog {
                let row_schema = extended_text(row, 0)?;
                let schema_matches = requested_schema
                    .as_ref()
                    .is_none_or(|schema| schema == &row_schema);
                let row_name = extended_text(row, if name.contains('(') { 2 } else { 1 })?;
                if schema_matches && row_name == name {
                    matches.push(row);
                }
            }
            let built =
                match matches.as_slice() {
                    [] => Err(NzError::Config(format!("procedure {name} not found"))),
                    [row] => Ok((
                        extended_text(row, 0)?,
                        render_procedure_ddl(row, &database)?,
                    )),
                    rows => {
                        let first_schema = extended_text(rows[0], 0)?;
                        if rows.iter().skip(1).any(|row| {
                            extended_text(row, 0).is_ok_and(|schema| schema != first_schema)
                        }) {
                            Err(NzError::Config(format!(
                                "{name} exists in multiple schemas; pass schema explicitly"
                            )))
                        } else {
                            Err(NzError::Config(format!(
                                "procedure {name} has multiple overloads; pass its signature"
                            )))
                        }
                    }
                };
            match built {
                Ok((schema, ddl)) => results.push(NzDdlBatchResult {
                    schema,
                    name,
                    ddl,
                    error: None,
                }),
                Err(error) => results.push(NzDdlBatchResult {
                    schema: requested_schema.unwrap_or_else(|| "UNKNOWN".into()),
                    name,
                    ddl: String::new(),
                    error: Some(error.to_string()),
                }),
            }
        }
        Ok(results)
    }

    async fn view_ddl_batch(
        &self,
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
            let mut sql =
                String::from("SELECT schema, viewname FROM _v_view WHERE viewname IS NOT NULL");
            if let Some(schema) = schema {
                sql.push_str(&format!(" AND schema = {}", super::literal(schema)?));
            }
            if let Some(pattern) = pattern {
                sql.push_str(&format!(" AND viewname LIKE {}", super::literal(pattern)?));
            }
            sql.push_str(" ORDER BY schema, viewname");
            self.query_rows(&sql)
                .await?
                .iter()
                .map(|row| Ok((Some(extended_text(row, 0)?), extended_text(row, 1)?)))
                .collect::<NzResult<Vec<_>>>()?
        };
        if targets.is_empty() {
            return Ok(Vec::new());
        }
        let view_filter = target_filter(&targets, "schema", "viewname")?;
        let column_filter = target_filter(&targets, "schema", "name")?;
        let object_filter = target_filter(&targets, "schema", "objname")?;
        let statements = [
            "SELECT current_catalog".to_owned(),
            format!("SELECT schema, viewname, definition FROM _v_view WHERE {view_filter}"),
            format!("SELECT schema, name, attname, attnum, format_type, attnotnull, coldefault, description FROM _v_relation_column WHERE {column_filter} ORDER BY schema, name, attnum"),
            format!("SELECT schema, objname, description FROM _v_object_data WHERE {object_filter}"),
        ];
        let result = self.client.query_multi(&statements.join("; "), &[]).await?;
        if result.result_sets.len() != statements.len() {
            return Err(NzError::Protocol(format!(
                "view DDL catalog batch expected {} result sets, got {}",
                statements.len(),
                result.result_sets.len()
            )));
        }
        let database = result.result_sets[0]
            .rows
            .first()
            .map(|row| extended_optional_text(row, 0))
            .transpose()?
            .flatten()
            .unwrap_or_else(|| "UNKNOWN".into());
        let mut definitions = HashMap::new();
        for row in &result.result_sets[1].rows {
            definitions.insert(
                (extended_text(row, 0)?, extended_text(row, 1)?),
                extended_optional_text(row, 2)?,
            );
        }
        let mut columns: HashMap<(String, String), Vec<NzDetailedColumnInfo>> = HashMap::new();
        for row in &result.result_sets[2].rows {
            let key = (extended_text(row, 0)?, extended_text(row, 1)?);
            columns.entry(key).or_default().push(NzDetailedColumnInfo {
                schema: extended_text(row, 0)?,
                name: extended_text(row, 2)?,
                ordinal: row.try_get(3)?,
                type_name: extended_text(row, 4)?,
                not_null: bool_value(row, 5)?,
                default_value: extended_optional_text(row, 6)?,
                description: extended_optional_text(row, 7)?,
            });
        }
        let mut comments = HashMap::new();
        for row in &result.result_sets[3].rows {
            if let Some(comment) = extended_optional_text(row, 2)? {
                if !comment.trim().is_empty() {
                    comments
                        .entry((extended_text(row, 0)?, extended_text(row, 1)?))
                        .or_insert(comment);
                }
            }
        }
        let mut results = Vec::with_capacity(targets.len());
        for (requested_schema, name) in targets {
            let matches = definitions
                .keys()
                .filter(|(candidate_schema, candidate_name)| {
                    candidate_name == &name
                        && requested_schema
                            .as_ref()
                            .is_none_or(|schema| schema == candidate_schema)
                })
                .cloned()
                .collect::<Vec<_>>();
            let built = match matches.as_slice() {
                [] => Err(NzError::Config(format!("view {name} not found"))),
                [key] => match definitions.get(key).and_then(Option::as_ref) {
                    None => Err(NzError::Config(format!(
                        "view {}.{} has no definition",
                        key.0, name
                    ))),
                    Some(definition) => Ok((
                        key.0.clone(),
                        build_view_ddl(
                            &database,
                            &key.0,
                            &name,
                            definition,
                            comments.get(key).map(String::as_str),
                            columns.get(key).map(Vec::as_slice).unwrap_or_default(),
                        ),
                    )),
                },
                _ => Err(NzError::Config(format!(
                    "{name} exists in multiple schemas; pass schema explicitly"
                ))),
            };
            match built {
                Ok((schema, ddl)) => results.push(NzDdlBatchResult {
                    schema,
                    name,
                    ddl,
                    error: None,
                }),
                Err(error) => results.push(NzDdlBatchResult {
                    schema: requested_schema.unwrap_or_else(|| "UNKNOWN".into()),
                    name,
                    ddl: String::new(),
                    error: Some(error.to_string()),
                }),
            }
        }
        Ok(results)
    }

    pub async fn tables_ddl(
        &self,
        schema: Option<&str>,
        pattern: Option<&str>,
        tables: Option<&[String]>,
    ) -> NzResult<Vec<NzDdlBatchResult>> {
        self.table_ddl_batch(schema, pattern, tables, None).await
    }

    pub async fn views_ddl(
        &self,
        schema: Option<&str>,
        pattern: Option<&str>,
        views: Option<&[String]>,
    ) -> NzResult<Vec<NzDdlBatchResult>> {
        self.view_ddl_batch(schema, pattern, views).await
    }

    pub async fn procedures_ddl(
        &self,
        schema: Option<&str>,
        pattern: Option<&str>,
        procedures: Option<&[String]>,
    ) -> NzResult<Vec<NzDdlBatchResult>> {
        self.procedure_ddl_batch(schema, pattern, procedures).await
    }
    /// Fetch related lists using one SQL batch, avoiding a request per object.
    pub async fn snapshot(&self, schema: Option<&str>) -> NzResult<CatalogSnapshot> {
        let sql = [
            "SELECT schema FROM _v_schema ORDER BY schema".into(),
            "SELECT database, owner, defschema FROM _v_database ORDER BY database".into(),
            tables_sql(schema, None)?,
            views_sql(schema)?,
            procedures_sql(schema)?,
        ]
        .join("; ");
        let result = self.client.query_multi(&sql, &[]).await?;
        if result.result_sets.len() != 5 {
            return Err(NzError::Protocol(
                "catalog snapshot expected five result sets".into(),
            ));
        }
        Ok(CatalogSnapshot {
            schemas: result.result_sets[0]
                .rows
                .iter()
                .map(|row| row.try_get(0))
                .collect::<NzResult<_>>()?,
            databases: result.result_sets[1]
                .rows
                .iter()
                .map(database_from_row)
                .collect::<NzResult<_>>()?,
            tables: result.result_sets[2]
                .rows
                .iter()
                .map(table_from_row)
                .collect::<NzResult<_>>()?,
            views: result.result_sets[3]
                .rows
                .iter()
                .map(view_from_row)
                .collect::<NzResult<_>>()?,
            procedures: result.result_sets[4]
                .rows
                .iter()
                .map(procedure_from_row)
                .collect::<NzResult<_>>()?,
        })
    }
}

fn render_procedure_ddl(row: &crate::connection::Row, database: &str) -> NzResult<String> {
    let schema = extended_text(row, 0)?;
    let name = extended_text(row, 1)?;
    let signature = extended_optional_text(row, 2)?.unwrap_or_default();
    let args = extended_optional_text(row, 3)?.unwrap_or_default();
    let args = args.trim();
    let args = if args.is_empty() {
        "()".to_owned()
    } else if args.starts_with('(') && args.ends_with(')') {
        args.to_owned()
    } else {
        format!("({args})")
    };
    let returns =
        fixed_return_type(&extended_optional_text(row, 4)?.unwrap_or_else(|| "INTEGER".into()));
    let execute_as_owner = match row.try_get_value(5)? {
        NzValue::Null => true,
        _ => bool_value(row, 5)?,
    };
    let description = extended_optional_text(row, 6)?;
    let source = extended_optional_text(row, 7)?.unwrap_or_default();
    let full_name = qualified(database, &schema, &name);
    let mut ddl = format!(
        "CREATE OR REPLACE PROCEDURE {full_name}{args}\nRETURNS {returns}\nEXECUTE AS {}\nLANGUAGE NZPLSQL AS\nBEGIN_PROC\n{source}\nEND_PROC;",
        if execute_as_owner { "OWNER" } else { "CALLER" }
    );
    if let Some(description) = description.filter(|value| !value.is_empty()) {
        let signature_open = signature.find('(').ok_or_else(|| {
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

fn target_filter(
    targets: &[(Option<String>, String)],
    schema_column: &str,
    name_column: &str,
) -> NzResult<String> {
    let clauses = targets
        .iter()
        .map(|(schema, name)| {
            let name = format!("{name_column} = {}", super::literal(name)?);
            Ok(match schema {
                Some(schema) => {
                    format!("({name} AND {schema_column} = {})", super::literal(schema)?)
                }
                None => format!("({name})"),
            })
        })
        .collect::<NzResult<Vec<_>>>()?;
    Ok(format!("({})", clauses.join(" OR ")))
}
