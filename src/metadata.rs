// Copyright 2026 Krzysztof Duśko.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Netezza catalog helpers used by SQL clients and the editor.

mod extended;
mod native;
pub use extended::{
    NzDdlBatchResult, NzDetailedColumnInfo, NzGroupInfo, NzQueryHistoryInfo, NzSequenceInfo,
    NzTableKeyInfo, NzUserInfo,
};
pub use native::{AsyncMetadata, CatalogSnapshot};

use crate::connection::NzConnection;
use crate::error::{NzError, NzResult};
use crate::params::escape_literal;
use crate::types::value::NzValue;

/// One table row from `_v_table`.
#[derive(Debug, Clone, PartialEq)]
pub struct NzTableInfo {
    /// Schema that owns the table.
    pub schema: String,
    /// Table name.
    pub name: String,
    /// Table owner.
    pub owner: Option<String>,
    /// Catalog object type (`TABLE`, `TEMP TABLE`, …).
    pub object_type: Option<String>,
    /// Catalog object id.
    pub object_id: Option<i64>,
    /// Approximate row count from `reltuples`.
    pub row_count: Option<i64>,
}

/// One column row from `_v_relation_column`.
#[derive(Debug, Clone, PartialEq)]
pub struct NzColumnInfo {
    /// Column name.
    pub name: String,
    /// One-based ordinal position.
    pub ordinal: i32,
    /// Formatted SQL type (`VARCHAR(20)`, `NUMERIC(10,2)`, …).
    pub type_name: String,
    /// Whether the column accepts NULL.
    pub nullable: bool,
    /// Catalog object id of the owning relation.
    pub object_id: Option<i64>,
    /// Column comment.
    pub description: Option<String>,
    /// Default expression, when the catalog exposes it.
    pub default_value: Option<String>,
}

/// One database row from `_v_database`.
#[derive(Debug, Clone, PartialEq)]
pub struct NzDatabaseInfo {
    /// Database name.
    pub name: String,
    /// Database owner.
    pub owner: Option<String>,
    /// Default schema for new sessions.
    pub default_schema: Option<String>,
}

/// One view row from `_v_view`.
#[derive(Debug, Clone, PartialEq)]
pub struct NzViewInfo {
    /// Schema that owns the view.
    pub schema: String,
    /// View name.
    pub name: String,
    /// View owner.
    pub owner: Option<String>,
    /// Catalog object id.
    pub object_id: Option<i64>,
    /// `CREATE VIEW` definition body.
    pub definition: Option<String>,
}

/// One procedure row from `_v_procedure`.
#[derive(Debug, Clone, PartialEq)]
pub struct NzProcedureInfo {
    /// Schema that owns the procedure.
    pub schema: String,
    /// Procedure name.
    pub name: String,
    /// Procedure owner.
    pub owner: Option<String>,
    /// Catalog object id.
    pub object_id: Option<i64>,
    /// Typed signature, e.g. `P(INTEGER)`.
    pub signature: Option<String>,
    /// Return type.
    pub returns: Option<String>,
    /// Whether the procedure is built in.
    pub is_builtin: Option<bool>,
    /// Procedure source body.
    pub source: Option<String>,
    /// Whether the procedure executes with owner rights.
    pub executed_as_owner: Option<bool>,
    /// Argument list.
    pub arguments: Option<String>,
    /// Procedure comment.
    pub description: Option<String>,
}

/// One table storage row from `_v_table_storage_stat`.
#[derive(Debug, Clone, PartialEq)]
pub struct NzTableSizeInfo {
    /// Schema that owns the table.
    pub schema: String,
    /// Table name.
    pub table: String,
    /// Used bytes.
    pub used_bytes: Option<i64>,
    /// Allocated bytes.
    pub allocated_bytes: Option<i64>,
    /// Used size in mebibytes.
    pub size_mb: Option<i64>,
    /// Storage skew factor.
    pub skew: Option<f64>,
}

/// One row from `_v_session`.
#[derive(Debug, Clone, PartialEq)]
pub struct NzSessionInfo {
    /// Backend session id.
    pub session_id: i64,
    /// Login user name.
    pub username: Option<String>,
    /// Connected database.
    pub database: Option<String>,
    /// Connection timestamp in server text form.
    pub connect_time: Option<String>,
    /// Session priority.
    pub priority: Option<String>,
    /// Session status.
    pub status: Option<String>,
    /// Client type description.
    pub client_type: Option<String>,
    /// Client OS user reported during the handshake.
    pub client_os_user: Option<String>,
}

/// One function row from `_v_function`.
#[derive(Debug, Clone, PartialEq)]
pub struct NzFunctionInfo {
    /// Schema that owns the function.
    pub schema: String,
    /// Function name.
    pub name: String,
    /// Function owner.
    pub owner: Option<String>,
    /// Catalog object id.
    pub object_id: Option<i64>,
    /// Typed signature.
    pub signature: Option<String>,
    /// Return type.
    pub returns: Option<String>,
    /// Language or environment descriptor.
    pub language: Option<String>,
    /// Whether this is a fluid/read-launcher function.
    pub is_fluid: Option<bool>,
}

/// One synonym row from `_v_synonym`.
#[derive(Debug, Clone, PartialEq)]
pub struct NzSynonymInfo {
    /// Schema that owns the synonym.
    pub schema: String,
    /// Synonym name.
    pub name: String,
    /// Referenced object name.
    pub referenced_object: Option<String>,
    /// Referenced database.
    pub referenced_database: Option<String>,
    /// Referenced schema.
    pub referenced_schema: Option<String>,
    /// Synonym comment.
    pub description: Option<String>,
}

/// One constraint row from `_v_relation_keydata`.
#[derive(Debug, Clone, PartialEq)]
pub struct NzConstraintInfo {
    /// Schema that owns the table.
    pub schema: String,
    /// Constrained table.
    pub table: String,
    /// Constraint name.
    pub name: String,
    /// Raw constraint type (`p`, `f`, `u`, …).
    pub constraint_type: char,
    /// Constrained column.
    pub column: String,
    /// Referenced database for foreign keys.
    pub primary_key_database: Option<String>,
    /// Referenced schema for foreign keys.
    pub primary_key_schema: Option<String>,
    /// Referenced table for foreign keys.
    pub primary_key_table: Option<String>,
    /// Referenced column for foreign keys.
    pub primary_key_column: Option<String>,
    /// Referential UPDATE action.
    pub update_type: Option<String>,
    /// Referential DELETE action.
    pub delete_type: Option<String>,
}

/// One distribution-key row from `_v_table_dist_map`.
#[derive(Debug, Clone, PartialEq)]
pub struct NzDistributionKeyInfo {
    /// Schema that owns the table.
    pub schema: String,
    /// Table name.
    pub table: String,
    /// Distribution column.
    pub column: String,
    /// Distribution attribute number.
    pub distribution_attribute: i32,
}

/// One organize-key row from `_v_table_organize_column`.
#[derive(Debug, Clone, PartialEq)]
pub struct NzOrganizeKeyInfo {
    /// Schema that owns the table.
    pub schema: String,
    /// Table name.
    pub table: String,
    /// Organize column.
    pub column: String,
    /// Column attribute number.
    pub attribute: i32,
}

/// One row from `_v_object_data`.
#[derive(Debug, Clone, PartialEq)]
pub struct NzObjectDetailInfo {
    /// Object schema.
    pub schema: String,
    /// Object name.
    pub name: String,
    /// Catalog object type.
    pub object_type: String,
    /// Object owner.
    pub owner: Option<String>,
    /// Catalog object id.
    pub object_id: Option<i64>,
    /// Object comment.
    pub description: Option<String>,
    /// Creation timestamp in server text form.
    pub created_date: Option<String>,
}

/// A table, view or procedure returned by [`NzMetadata::objects`].
#[derive(Debug, Clone, PartialEq)]
pub struct NzObjectInfo {
    /// Object schema.
    pub schema: String,
    /// Object name.
    pub name: String,
    /// `TABLE`, `VIEW` or `PROCEDURE`.
    pub object_type: String,
    /// Object owner.
    pub owner: Option<String>,
    /// Catalog object id.
    pub object_id: Option<i64>,
}

/// Catalog operations borrowing a connection for their duration.
pub struct NzMetadata<'a> {
    connection: &'a mut NzConnection,
}

impl<'a> NzMetadata<'a> {
    pub(crate) fn new(connection: &'a mut NzConnection) -> Self {
        Self { connection }
    }

    /// List visible schemas.
    pub fn schemas(&mut self) -> NzResult<Vec<String>> {
        let result = self
            .connection
            .query("SELECT schema FROM _v_schema ORDER BY schema", &[])?;
        result.rows().iter().map(|r| r.try_get(0)).collect()
    }

    /// List databases.
    pub fn databases(&mut self) -> NzResult<Vec<NzDatabaseInfo>> {
        let result = self.connection.query(
            "SELECT database, owner, defschema FROM _v_database ORDER BY database",
            &[],
        )?;
        result.rows().iter().map(database_from_row).collect()
    }

    /// List tables, optionally filtered by schema and a SQL `LIKE` pattern.
    pub fn tables(
        &mut self,
        schema: Option<&str>,
        pattern: Option<&str>,
    ) -> NzResult<Vec<NzTableInfo>> {
        let sql = tables_sql(schema, pattern)?;
        let result = self.connection.query(&sql, &[])?;
        result.rows().iter().map(table_from_row).collect()
    }

    /// List columns of one table.
    pub fn columns(&mut self, table: &str, schema: Option<&str>) -> NzResult<Vec<NzColumnInfo>> {
        let sql = columns_sql(table, schema)?;
        let result = self.connection.query(&sql, &[])?;
        result.rows().iter().map(column_from_row).collect()
    }

    /// List views, optionally limited to a schema.
    pub fn views(&mut self, schema: Option<&str>) -> NzResult<Vec<NzViewInfo>> {
        let sql = views_sql(schema)?;
        let result = self.connection.query(&sql, &[])?;
        result.rows().iter().map(view_from_row).collect()
    }

    /// List stored procedures, optionally limited to a schema.
    pub fn procedures(&mut self, schema: Option<&str>) -> NzResult<Vec<NzProcedureInfo>> {
        let sql = procedures_sql(schema)?;
        let result = self.connection.query(&sql, &[])?;
        result.rows().iter().map(procedure_from_row).collect()
    }

    /// Return the distribution-key columns of one table.
    pub fn distribution_key(&mut self, table: &str, schema: Option<&str>) -> NzResult<Vec<String>> {
        let mut sql = format!(
            "SELECT attname FROM _v_table_dist_map WHERE tablename = {}",
            literal(table)?,
        );
        if let Some(schema) = schema {
            sql.push_str(" AND schema = ");
            sql.push_str(&literal(schema)?);
        }
        sql.push_str(" ORDER BY distattnum");
        let result = self.connection.query(&sql, &[])?;
        result.rows().iter().map(|r| text(r, 0)).collect()
    }

    /// Return storage statistics for user tables.
    pub fn table_sizes(&mut self, schema: Option<&str>) -> NzResult<Vec<NzTableSizeInfo>> {
        let mut sql = String::from(
            "SELECT schema, tablename, used_bytes, allocated_bytes, (used_bytes / 1048576)::BIGINT AS size_mb, skew FROM _v_table_storage_stat WHERE tablename IS NOT NULL",
        );
        if let Some(schema) = schema {
            sql.push_str(" AND schema = ");
            sql.push_str(&literal(schema)?);
        }
        sql.push_str(" ORDER BY used_bytes DESC");
        let result = self.connection.query(&sql, &[])?;
        result.rows().iter().map(table_size_from_row).collect()
    }

    /// List active sessions.
    pub fn sessions(&mut self) -> NzResult<Vec<NzSessionInfo>> {
        let result = self.connection.query(
            "SELECT id, username, dbname, conntime, priority, status, type, client_os_username FROM _v_session ORDER BY conntime DESC",
            &[],
        )?;
        result.rows().iter().map(session_from_row).collect()
    }

    /// List user-defined functions.
    pub fn functions(&mut self, schema: Option<&str>) -> NzResult<Vec<NzFunctionInfo>> {
        let mut sql = String::from(
            "SELECT schema, function, owner, objid, functionsignature, returns, env FROM _v_function WHERE function IS NOT NULL",
        );
        if let Some(schema) = schema {
            sql.push_str(" AND schema = ");
            sql.push_str(&literal(schema)?);
        }
        sql.push_str(" ORDER BY schema, function");
        let result = self.connection.query(&sql, &[])?;
        result.rows().iter().map(function_from_row).collect()
    }

    /// List synonyms.
    pub fn synonyms(&mut self, schema: Option<&str>) -> NzResult<Vec<NzSynonymInfo>> {
        let mut sql = String::from(
            "SELECT schema, synonym_name, refobjname, refdatabase, refschema, description FROM _v_synonym WHERE synonym_name IS NOT NULL",
        );
        if let Some(schema) = schema {
            sql.push_str(" AND schema = ");
            sql.push_str(&literal(schema)?);
        }
        sql.push_str(" ORDER BY schema, synonym_name");
        let result = self.connection.query(&sql, &[])?;
        result.rows().iter().map(synonym_from_row).collect()
    }

    /// List primary, unique and foreign keys.
    pub fn constraints(&mut self, schema: Option<&str>) -> NzResult<Vec<NzConstraintInfo>> {
        let mut sql = String::from(
            "SELECT schema, relation, constraintname, contype, attname, pkdatabase, pkschema, pkrelation, pkattname, updt_type, del_type FROM _v_relation_keydata WHERE relation IS NOT NULL",
        );
        if let Some(schema) = schema {
            sql.push_str(" AND schema = ");
            sql.push_str(&literal(schema)?);
        }
        sql.push_str(" ORDER BY schema, relation, conseq");
        let result = self.connection.query(&sql, &[])?;
        result.rows().iter().map(constraint_from_row).collect()
    }

    /// Distribution keys across every table in a schema.
    pub fn all_distribution_keys(
        &mut self,
        schema: Option<&str>,
    ) -> NzResult<Vec<NzDistributionKeyInfo>> {
        let mut sql = String::from(
            "SELECT schema, tablename, attname, distattnum FROM _v_table_dist_map WHERE tablename IS NOT NULL",
        );
        if let Some(schema) = schema {
            sql.push_str(" AND schema = ");
            sql.push_str(&literal(schema)?);
        }
        sql.push_str(" ORDER BY schema, tablename, distseqno");
        let result = self.connection.query(&sql, &[])?;
        result.rows().iter().map(distribution_from_row).collect()
    }

    /// Organize keys across every table in a schema.
    pub fn organize_keys(&mut self, schema: Option<&str>) -> NzResult<Vec<NzOrganizeKeyInfo>> {
        let mut sql = String::from(
            "SELECT schema, tablename, attname, attnum FROM _v_table_organize_column WHERE tablename IS NOT NULL",
        );
        if let Some(schema) = schema {
            sql.push_str(" AND schema = ");
            sql.push_str(&literal(schema)?);
        }
        sql.push_str(" ORDER BY schema, tablename, orgseqno");
        let result = self.connection.query(&sql, &[])?;
        result.rows().iter().map(organize_from_row).collect()
    }

    /// Object definitions and comments from `_v_object_data`.
    pub fn object_details(&mut self, schema: Option<&str>) -> NzResult<Vec<NzObjectDetailInfo>> {
        let mut sql = String::from(
            "SELECT schema, objname, objtype, owner, objid, description, createdate FROM _v_object_data WHERE objname IS NOT NULL",
        );
        if let Some(schema) = schema {
            sql.push_str(" AND schema = ");
            sql.push_str(&literal(schema)?);
        }
        sql.push_str(" AND objtype NOT IN ('AGGREGATE','CONSTRAINT','DATABASE','DATATYPE','GROUP','MANAGEMENT INDEX','MANAGEMENT SEQ','MANAGEMENT TABLE','MANAGEMENT VIEW','SCHEDULER RULE','SCHEMA','SYSTEM INDEX','SYSTEM SEQ','SYSTEM TABLE','SYSTEM VIEW','USER') ORDER BY schema, objtype, objname");
        let result = self.connection.query(&sql, &[])?;
        result.rows().iter().map(object_detail_from_row).collect()
    }

    /// Search object details by a case-insensitive substring.
    pub fn search_objects_detailed(
        &mut self,
        pattern: &str,
        schema: Option<&str>,
    ) -> NzResult<Vec<NzObjectDetailInfo>> {
        let mut sql = format!(
            "SELECT schema, objname, objtype, owner, objid, description, createdate FROM _v_object_data WHERE UPPER(objname) LIKE UPPER({})",
            like_contains_literal(pattern)?,
        );
        if let Some(schema) = schema {
            sql.push_str(" AND schema = ");
            sql.push_str(&literal(schema)?);
        }
        sql.push_str(" AND objtype NOT IN ('AGGREGATE','CONSTRAINT','DATABASE','DATATYPE','GROUP','MANAGEMENT INDEX','MANAGEMENT SEQ','MANAGEMENT TABLE','MANAGEMENT VIEW','SCHEDULER RULE','SCHEMA','SYSTEM INDEX','SYSTEM SEQ','SYSTEM TABLE','SYSTEM VIEW','USER') ORDER BY schema, objtype, objname");
        let result = self.connection.query(&sql, &[])?;
        let folded_pattern = pattern.to_ascii_lowercase();
        result
            .rows()
            .iter()
            .map(object_detail_from_row)
            .filter(|detail| {
                detail
                    .as_ref()
                    .map(|detail| detail.name.to_ascii_lowercase().contains(&folded_pattern))
                    .unwrap_or(true)
            })
            .collect()
    }

    /// Search tables, views and procedures by a case-insensitive substring.
    pub fn objects(&mut self, pattern: &str, schema: Option<&str>) -> NzResult<Vec<NzObjectInfo>> {
        let like = format!("%{pattern}%");
        let folded_pattern = pattern.to_ascii_lowercase();
        // Match the C# `SearchObjectsAsync` surface: tables use the catalog
        // LIKE predicate, while views and procedures are filtered in memory
        // with ordinal case-insensitive matching. This intentionally excludes
        // functions/synonyms; callers needing every catalog object can use
        // `search_objects_detailed`.
        let mut objects = Vec::new();
        for table in self.tables(schema, Some(&like))? {
            objects.push(NzObjectInfo {
                schema: table.schema,
                name: table.name,
                object_type: "TABLE".into(),
                owner: table.owner,
                object_id: table.object_id,
            });
        }
        for view in self.views(schema)? {
            if view.name.to_ascii_lowercase().contains(&folded_pattern) {
                objects.push(NzObjectInfo {
                    schema: view.schema,
                    name: view.name,
                    object_type: "VIEW".into(),
                    owner: view.owner,
                    object_id: view.object_id,
                });
            }
        }
        for procedure in self.procedures(schema)? {
            if procedure
                .name
                .to_ascii_lowercase()
                .contains(&folded_pattern)
            {
                objects.push(NzObjectInfo {
                    schema: procedure.schema,
                    name: procedure.name,
                    object_type: "PROCEDURE".into(),
                    owner: procedure.owner,
                    object_id: procedure.object_id,
                });
            }
        }
        Ok(objects)
    }
}

impl NzConnection {
    /// Borrow this connection for catalog/metadata queries.
    pub fn metadata(&mut self) -> NzMetadata<'_> {
        NzMetadata::new(self)
    }
}

fn text(row: &crate::connection::Row, ix: usize) -> NzResult<String> {
    row.try_get(ix)
}

fn literal(value: &str) -> NzResult<String> {
    escape_literal(&NzValue::Text(value.into())).map_err(NzError::Config)
}

fn like_contains_literal(value: &str) -> NzResult<String> {
    // Netezza accepts LIKE but rejects PostgreSQL's optional ESCAPE clause on
    // some appliance versions. Use a SQL-safe superset and let the caller
    // apply the exact case-insensitive literal match after decoding rows.
    literal(&format!("%{value}%"))
}

fn optional_text(row: &crate::connection::Row, ix: usize) -> NzResult<Option<String>> {
    match row.try_get_value(ix)? {
        NzValue::Null => Ok(None),
        value => Ok(Some(value.to_display_string())),
    }
}

fn optional_i64(row: &crate::connection::Row, ix: usize) -> NzResult<Option<i64>> {
    row.try_get(ix)
}

fn table_from_row(row: &crate::connection::Row) -> NzResult<NzTableInfo> {
    Ok(NzTableInfo {
        schema: text(row, 0)?,
        name: text(row, 1)?,
        owner: optional_text(row, 2)?,
        object_type: optional_text(row, 3)?,
        object_id: optional_i64(row, 4)?,
        row_count: optional_i64(row, 5)?,
    })
}

fn column_from_row(row: &crate::connection::Row) -> NzResult<NzColumnInfo> {
    Ok(NzColumnInfo {
        name: text(row, 0)?,
        ordinal: row.try_get(1)?,
        type_name: text(row, 2)?,
        nullable: text(row, 3)? == "Y",
        object_id: optional_i64(row, 4)?,
        description: optional_text(row, 5)?,
        // Netezza versions differ on whether `_v_relation_column` exposes a
        // default column; keep the stable C# API shape and leave it absent
        // until a version-specific query is selected.
        default_value: None,
    })
}

fn database_from_row(row: &crate::connection::Row) -> NzResult<NzDatabaseInfo> {
    Ok(NzDatabaseInfo {
        name: text(row, 0)?,
        owner: optional_text(row, 1)?,
        default_schema: optional_text(row, 2)?,
    })
}

fn view_from_row(row: &crate::connection::Row) -> NzResult<NzViewInfo> {
    Ok(NzViewInfo {
        schema: text(row, 0)?,
        name: text(row, 1)?,
        owner: optional_text(row, 2)?,
        object_id: optional_i64(row, 3)?,
        definition: optional_text(row, 4)?,
    })
}

fn procedure_from_row(row: &crate::connection::Row) -> NzResult<NzProcedureInfo> {
    Ok(NzProcedureInfo {
        schema: text(row, 0)?,
        name: text(row, 1)?,
        owner: optional_text(row, 2)?,
        object_id: optional_i64(row, 3)?,
        signature: optional_text(row, 4)?,
        returns: optional_text(row, 5)?,
        is_builtin: optional_bool(row, 6)?,
        source: optional_text(row, 7)?,
        executed_as_owner: optional_bool(row, 8)?,
        arguments: optional_text(row, 9)?,
        description: optional_text(row, 10)?,
    })
}

fn table_size_from_row(row: &crate::connection::Row) -> NzResult<NzTableSizeInfo> {
    Ok(NzTableSizeInfo {
        schema: text(row, 0)?,
        table: text(row, 1)?,
        used_bytes: optional_i64(row, 2)?,
        allocated_bytes: optional_i64(row, 3)?,
        size_mb: optional_i64(row, 4)?,
        skew: row.try_get(5)?,
    })
}

fn session_from_row(row: &crate::connection::Row) -> NzResult<NzSessionInfo> {
    Ok(NzSessionInfo {
        session_id: row.try_get(0)?,
        username: optional_text(row, 1)?,
        database: optional_text(row, 2)?,
        connect_time: optional_text(row, 3)?,
        priority: optional_text(row, 4)?,
        status: optional_text(row, 5)?,
        client_type: optional_text(row, 6)?,
        client_os_user: optional_text(row, 7)?,
    })
}

fn function_from_row(row: &crate::connection::Row) -> NzResult<NzFunctionInfo> {
    let env = optional_text(row, 6)?;
    Ok(NzFunctionInfo {
        schema: text(row, 0)?,
        name: text(row, 1)?,
        owner: optional_text(row, 2)?,
        object_id: optional_i64(row, 3)?,
        signature: optional_text(row, 4)?,
        returns: optional_text(row, 5)?,
        is_fluid: env.as_deref().map(|v| {
            v.to_ascii_lowercase()
                .contains("com.ibm.nz.fq.sqlreadlauncher")
        }),
        language: env,
    })
}

fn synonym_from_row(row: &crate::connection::Row) -> NzResult<NzSynonymInfo> {
    Ok(NzSynonymInfo {
        schema: text(row, 0)?,
        name: text(row, 1)?,
        referenced_object: optional_text(row, 2)?,
        referenced_database: optional_text(row, 3)?,
        referenced_schema: optional_text(row, 4)?,
        description: optional_text(row, 5)?,
    })
}

fn constraint_from_row(row: &crate::connection::Row) -> NzResult<NzConstraintInfo> {
    let kind = text(row, 3)?.chars().next().unwrap_or('\0');
    Ok(NzConstraintInfo {
        schema: text(row, 0)?,
        table: text(row, 1)?,
        name: text(row, 2)?,
        constraint_type: kind,
        column: optional_text(row, 4)?.unwrap_or_default(),
        primary_key_database: optional_text(row, 5)?,
        primary_key_schema: optional_text(row, 6)?,
        primary_key_table: optional_text(row, 7)?,
        primary_key_column: optional_text(row, 8)?,
        update_type: optional_text(row, 9)?,
        delete_type: optional_text(row, 10)?,
    })
}

fn distribution_from_row(row: &crate::connection::Row) -> NzResult<NzDistributionKeyInfo> {
    Ok(NzDistributionKeyInfo {
        schema: text(row, 0)?,
        table: text(row, 1)?,
        column: text(row, 2)?,
        distribution_attribute: row.try_get(3)?,
    })
}

fn organize_from_row(row: &crate::connection::Row) -> NzResult<NzOrganizeKeyInfo> {
    Ok(NzOrganizeKeyInfo {
        schema: text(row, 0)?,
        table: text(row, 1)?,
        column: text(row, 2)?,
        attribute: row.try_get(3)?,
    })
}

fn object_detail_from_row(row: &crate::connection::Row) -> NzResult<NzObjectDetailInfo> {
    Ok(NzObjectDetailInfo {
        schema: optional_text(row, 0)?.unwrap_or_else(|| "ADMIN".into()),
        name: text(row, 1)?,
        object_type: text(row, 2)?,
        owner: optional_text(row, 3)?,
        object_id: optional_i64(row, 4)?,
        description: optional_text(row, 5)?,
        created_date: optional_text(row, 6)?,
    })
}

fn optional_bool(row: &crate::connection::Row, ix: usize) -> NzResult<Option<bool>> {
    match row.try_get_value(ix)? {
        NzValue::Null => Ok(None),
        NzValue::Bool(v) => Ok(Some(*v)),
        NzValue::Text(v) => Ok(Some(matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "t" | "true" | "1" | "y"
        ))),
        NzValue::Int2(v) => Ok(Some(*v != 0)),
        NzValue::Int4(v) => Ok(Some(*v != 0)),
        NzValue::Int8(v) => Ok(Some(*v != 0)),
        NzValue::Decimal(v) => Ok(Some(!v.is_zero())),
        other => Err(NzError::Config(format!(
            "expected boolean metadata value, got {other:?}"
        ))),
    }
}

fn tables_sql(schema: Option<&str>, pattern: Option<&str>) -> NzResult<String> {
    let mut sql = String::from(
            "SELECT schema, tablename, owner, objtype, objid, reltuples FROM _v_table WHERE tablename IS NOT NULL",
        );
    if let Some(schema) = schema {
        sql.push_str(" AND schema = ");
        sql.push_str(&escape_literal(&NzValue::Text(schema.into())).map_err(NzError::Config)?);
    }
    if let Some(pattern) = pattern {
        sql.push_str(" AND tablename LIKE ");
        sql.push_str(&escape_literal(&NzValue::Text(pattern.into())).map_err(NzError::Config)?);
    }
    sql.push_str(" AND schema NOT IN ('DEFINITION_SCHEMA', 'INZA', 'NZ_QUERY_HISTORY') AND objtype <> 'SYSTEM_TABLE' ORDER BY schema, tablename");
    Ok(sql)
}

fn columns_sql(table: &str, schema: Option<&str>) -> NzResult<String> {
    let mut sql = format!(
            "SELECT attname, attnum, format_type, CASE WHEN attnotnull THEN 'N' ELSE 'Y' END, objid, description FROM _v_relation_column WHERE name = {}",
            escape_literal(&NzValue::Text(table.into())).map_err(NzError::Config)?
        );
    if let Some(schema) = schema {
        sql.push_str(" AND schema = ");
        sql.push_str(&escape_literal(&NzValue::Text(schema.into())).map_err(NzError::Config)?);
    }
    sql.push_str(" ORDER BY attnum");
    Ok(sql)
}

fn views_sql(schema: Option<&str>) -> NzResult<String> {
    let mut sql = String::from(
        "SELECT schema, viewname, owner, objid, definition FROM _v_view WHERE viewname IS NOT NULL",
    );
    if let Some(schema) = schema {
        sql.push_str(" AND schema = ");
        sql.push_str(&escape_literal(&NzValue::Text(schema.into())).map_err(NzError::Config)?);
    }
    sql.push_str(" ORDER BY schema, viewname");
    Ok(sql)
}

fn procedures_sql(schema: Option<&str>) -> NzResult<String> {
    let mut sql = String::from(
            "SELECT schema, procedure, owner, objid, proceduresignature, returns, builtin, proceduresource, executedasowner, arguments, description FROM _v_procedure WHERE procedure IS NOT NULL",
        );
    if let Some(schema) = schema {
        sql.push_str(" AND schema = ");
        sql.push_str(&escape_literal(&NzValue::Text(schema.into())).map_err(NzError::Config)?);
    }
    sql.push_str(" ORDER BY schema, procedure");
    Ok(sql)
}
