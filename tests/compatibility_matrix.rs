//! Offline compatibility checks for the public Rust surface.
//!
//! These cases mirror the stable, appliance-independent parts of the C# and
//! Python suites.  Live SQL behavior is covered by `live_driver.rs` and the
//! cross-driver harness under `benchmarks/netezza_cross_driver`.

use nz_rust::params::substitute_parameters;
use nz_rust::{
    parse_connection_string, substitute_bound_parameters, ColumnDataType, ColumnDesc, NzDataReader,
    NzError, NzParameter, NzValue, QueryResult, ResultSet, Row, SecurityLevel,
};
use std::str::FromStr;

fn column(name: &str, oid: i32, type_mod: i32) -> ColumnDesc {
    ColumnDesc {
        name: name.into(),
        type_oid: oid,
        type_len: -1,
        type_mod,
        format: 0,
    }
}

fn reader_with_two_sets() -> NzDataReader {
    let first_columns = vec![column("ONE", 23, -1), column("NAME", 1043, 16 + 12)];
    let second_columns = vec![column("TWO", 20, -1)];
    NzDataReader::from_result(QueryResult {
        result_sets: vec![
            ResultSet::new(
                first_columns.clone(),
                vec![Row::new(
                    first_columns,
                    vec![NzValue::Int4(1), NzValue::Text("rust".into())],
                )],
            ),
            ResultSet::new(
                second_columns.clone(),
                vec![Row::new(second_columns, vec![NzValue::Int8(2)])],
            ),
        ],
        rows_affected: 1,
        notices: vec!["notice".into()],
    })
}

macro_rules! column_type_case {
    ($name:ident, $oid:expr, $expected:expr) => {
        #[test]
        fn $name() {
            assert_eq!(column("VALUE", $oid, -1).type_name(), $expected);
        }
    };
}

column_type_case!(type_name_bool, 16, "BOOL");
column_type_case!(type_name_byteint, 2500, "BYTEINT");
column_type_case!(type_name_smallint, 21, "SMALLINT");
column_type_case!(type_name_integer, 23, "INTEGER");
column_type_case!(type_name_bigint, 20, "BIGINT");
column_type_case!(type_name_real, 700, "REAL");
column_type_case!(type_name_double, 701, "DOUBLE");
column_type_case!(type_name_numeric, 1700, "NUMERIC");
column_type_case!(type_name_date, 1082, "DATE");
column_type_case!(type_name_time, 1083, "TIME");
column_type_case!(type_name_timestamp, 1114, "TIMESTAMP");
column_type_case!(type_name_timetz, 1266, "TIMETZ");
column_type_case!(type_name_interval, 1186, "INTERVAL");
column_type_case!(type_name_nchar, 2522, "NCHAR");
column_type_case!(type_name_nvarchar, 2530, "NVARCHAR");

#[test]
fn declared_character_and_numeric_types_keep_wire_modifiers() {
    assert_eq!(
        column("VC", 1043, 16 + 32).declared_type_name(),
        "VARCHAR(32)"
    );
    assert_eq!(
        column("NC", 2522, 16 + 20).declared_type_name(),
        "NCHAR(20)"
    );
    let numeric_mod = 16 + ((38_i32) << 16) + 8;
    assert_eq!(
        column("N", 1700, numeric_mod).declared_type_name(),
        "NUMERIC(38,8)"
    );
}

#[test]
fn unknown_type_is_explicitly_named() {
    assert_eq!(column("X", 9999, -1).type_name(), "OID(9999)");
}

#[test]
fn value_canonicalization_preserves_nulls_and_decimal_scale() {
    assert_eq!(NzValue::Null.to_node_canonical(), "null");
    assert_eq!(NzValue::Bool(true).to_node_canonical(), "true");
    assert_eq!(
        NzValue::Int8(9_223_372_036_854_775_807).to_node_canonical(),
        "9223372036854775807"
    );
    assert_eq!(
        NzValue::Numeric("3.1400".into()).to_node_canonical(),
        "3.1400"
    );
    assert_eq!(
        NzValue::Decimal(rust_decimal::Decimal::from_str("3.1400").unwrap()).to_node_canonical(),
        "3.1400"
    );
}

#[test]
fn value_canonicalization_uses_stable_temporal_forms() {
    assert_eq!(
        NzValue::Date("2024-01-02".into()).to_node_canonical(),
        "2024-01-02T00:00:00.000Z"
    );
    assert_eq!(
        NzValue::Timestamp("2024-01-02 03:04:05.123456".into()).to_node_canonical(),
        "2024-01-02T03:04:05.123Z"
    );
    assert_eq!(
        NzValue::Bytea(vec![0, 1, 0xab]).to_node_canonical(),
        "E'\\\\x0001ab'"
    );
}

#[test]
fn reader_navigates_rows_and_result_sets() {
    let mut reader = reader_with_two_sets();
    assert!(reader.has_rows());
    assert_eq!(reader.field_count(), 2);
    assert_eq!(reader.get_name(0).unwrap(), "ONE");
    assert_eq!(reader.get_ordinal("name").unwrap(), 1);
    assert_eq!(reader.get_type_name(0).unwrap(), "INT4");
    assert_eq!(reader.get_declared_type_name(1).unwrap(), "VARCHAR(12)");
    assert_eq!(
        reader.get_column_metadata(0).unwrap().data_type,
        ColumnDataType::Number
    );
    assert!(reader.read().unwrap());
    assert_eq!(reader.get_i32(0).unwrap(), 1);
    assert_eq!(reader.get_string(1).unwrap().as_deref(), Some("rust"));
    assert!(!reader.read().unwrap());
    assert!(reader.next_result().unwrap());
    assert_eq!(reader.field_count(), 1);
    assert!(reader.read().unwrap());
    assert_eq!(reader.get_i64(0).unwrap(), 2);
    assert!(!reader.next_result().unwrap());
}

#[test]
fn reader_empty_result_still_has_ado_style_result() {
    let mut reader = NzDataReader::from_result(QueryResult {
        result_sets: vec![],
        rows_affected: -1,
        notices: vec![],
    });
    assert_eq!(reader.field_count(), 0);
    assert!(!reader.has_rows());
    assert!(!reader.read().unwrap());
    assert!(!reader.next_result().unwrap());
}

#[test]
fn reader_schema_table_uses_one_based_ordinals() {
    let reader = reader_with_two_sets();
    let schema = reader.get_schema_table().unwrap();
    assert_eq!(schema.columns_count, 2);
    assert_eq!(schema.rows[0].column_ordinal, 1);
    assert_eq!(schema.rows[1].column_ordinal, 2);
    assert!(schema.rows.iter().all(|row| row.is_read_only));
}

#[test]
fn reader_reports_access_before_read_as_configuration_error() {
    let reader = reader_with_two_sets();
    assert!(matches!(reader.get_value(0), Err(NzError::Config(_))));
}

#[test]
fn positional_parameters_escape_quotes_and_nulls() {
    let sql = substitute_parameters(
        "SELECT $1, $2, '$1', -- $2\n $1 /* $2 */",
        &[NzValue::Text("O'Brien".into()), NzValue::Null],
    )
    .unwrap();
    assert_eq!(
        sql,
        "SELECT 'O''Brien', NULL, '$1', -- $2\n 'O''Brien' /* $2 */"
    );
}

#[test]
fn named_parameters_ignore_literals_comments_and_identifiers() {
    let sql = substitute_bound_parameters(
        "SELECT :name, ':name', \"name\", -- :name\n :name /* :name */",
        &[NzParameter::named("name", NzValue::Text("Ada".into()))],
    )
    .unwrap();
    assert_eq!(
        sql,
        "SELECT 'Ada', ':name', \"name\", -- :name\n 'Ada' /* :name */"
    );
}

#[test]
fn named_parameters_reject_mixed_modes() {
    let result = substitute_bound_parameters(
        "SELECT :name, ?",
        &[
            NzParameter::named("name", NzValue::Int4(1)),
            NzParameter::positional(NzValue::Int4(2)),
        ],
    );
    assert!(result.unwrap_err().contains("cannot be mixed"));
}

#[test]
fn bound_parameters_report_missing_values() {
    assert!(substitute_bound_parameters("SELECT :missing", &[])
        .unwrap_err()
        .contains("Missing value"));
}

#[test]
fn connection_string_aliases_and_security_modes_match_reference() {
    let cfg = parse_connection_string(
        "nz://user:p%40ss@host:5481/DB?sslmode=require&connection_timeout=7&client_hostname=ci",
    )
    .unwrap();
    assert_eq!(cfg.host, "host");
    assert_eq!(cfg.port, 5481);
    assert_eq!(cfg.password, "p@ss");
    assert_eq!(cfg.database, "DB");
    assert_eq!(cfg.connection_timeout, 7);
    assert_eq!(cfg.client_host_name, "ci");
    assert_eq!(cfg.security_level, SecurityLevel::OnlySecuredSession);
    assert!(!cfg.reject_unauthorized);
}

#[test]
fn connection_string_rejects_missing_required_parts() {
    for value in ["postgres://u:p@h/db", "nz://:p@h/db", "nz://u:p@h"] {
        assert!(parse_connection_string(value).is_err(), "accepted {value}");
    }
}
