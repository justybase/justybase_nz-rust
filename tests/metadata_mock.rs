//! Offline catalog coverage: every metadata query is served by the scripted
//! mock backend and decoded through the public API.
//!
//! The catalog modules had no appliance-independent coverage before this test;
//! the wire answers here are synthetic but follow the real `_v_*` column
//! layouts. Live layout drift is covered by `metadata_core.bin` replay in
//! `wire_replay.rs`.

mod support;

use nz_rust::NzMetadata;
use support::*;

const OID_INT8: i32 = 20;
const OID_SMALLINT: i32 = 21;
const OID_BOOL: i32 = 16;
const OID_FLOAT8: i32 = 701;

fn response(columns: &[TextColumn<'_>], rows: &[Vec<Option<&str>>]) -> Vec<u8> {
    let mut wire = row_description(columns);
    for row in rows {
        let cells: Vec<Option<&[u8]>> = row
            .iter()
            .map(|cell| cell.map(|value| value.as_bytes()))
            .collect();
        wire.extend(text_row(&cells));
    }
    wire.extend(command_complete("SELECT"));
    wire.extend(ready());
    wire
}

fn catalog_responses() -> Vec<(String, Vec<u8>)> {
    let mut items = vec![
        (
            "SELECT current_catalog".into(),
            response(
                &[("CURRENT_CATALOG", OID_VARCHAR, -1)],
                &[vec![Some("JUST_DATA")]],
            ),
        ),
        (
            "SELECT current_schema".into(),
            response(
                &[("CURRENT_SCHEMA", OID_VARCHAR, -1)],
                &[vec![Some("ADMIN")]],
            ),
        ),
        (
            "SELECT schema FROM _v_schema ORDER BY schema".into(),
            response(
                &[("SCHEMA", OID_VARCHAR, -1)],
                &[vec![Some("ADMIN")], vec![Some("JUST_DATA")]],
            ),
        ),
    ];
    items.push((
        "SELECT database, owner, defschema FROM _v_database ORDER BY database".into(),
        response(
            &[
                ("DATABASE", OID_VARCHAR, -1),
                ("OWNER", OID_VARCHAR, -1),
                ("DEFSCHEMA", OID_VARCHAR, -1),
            ],
            &[vec![Some("JUST_DATA"), Some("ADMIN"), Some("ADMIN")]],
        ),
    ));
    let table_columns = [
        ("SCHEMA", OID_VARCHAR, -1),
        ("TABLENAME", OID_VARCHAR, -1),
        ("OWNER", OID_VARCHAR, -1),
        ("OBJTYPE", OID_VARCHAR, -1),
        ("OBJID", OID_INT4, 4),
        ("RELTUPLES", OID_INT8, 8),
    ];
    let table_row = vec![
        Some("ADMIN"),
        Some("RUST_META"),
        Some("ADMIN"),
        Some("TABLE"),
        Some("42"),
        Some("7"),
    ];
    for pattern in ["RUST_META%", "%meta%"] {
        items.push((
            format!(
                "SELECT schema, tablename, owner, objtype, objid, reltuples FROM _v_table \
                 WHERE tablename IS NOT NULL AND schema = 'ADMIN' AND tablename LIKE '{pattern}' \
                 AND schema NOT IN ('DEFINITION_SCHEMA', 'INZA', 'NZ_QUERY_HISTORY') \
                 AND objtype <> 'SYSTEM_TABLE' ORDER BY schema, tablename"
            ),
            response(&table_columns, std::slice::from_ref(&table_row)),
        ));
    }
    let column_columns = [
        ("ATTNAME", OID_VARCHAR, -1),
        ("ATTNUM", OID_SMALLINT, 2),
        ("FORMAT_TYPE", OID_VARCHAR, -1),
        ("NULLABLE", OID_VARCHAR, -1),
        ("OBJID", OID_INT4, 4),
        ("DESCRIPTION", OID_VARCHAR, -1),
    ];
    items.push((
        "SELECT attname, attnum, format_type, CASE WHEN attnotnull THEN 'N' ELSE 'Y' END, \
         objid, description FROM _v_relation_column WHERE name = 'RUST_META' AND schema = 'ADMIN' \
         ORDER BY attnum"
            .into(),
        response(
            &column_columns,
            &[
                vec![
                    Some("ID"),
                    Some("1"),
                    Some("INTEGER"),
                    Some("N"),
                    Some("42"),
                    None,
                ],
                vec![
                    Some("NAME"),
                    Some("2"),
                    Some("VARCHAR(20)"),
                    Some("Y"),
                    Some("42"),
                    Some("display name"),
                ],
            ],
        ),
    ));
    let view_columns = [
        ("SCHEMA", OID_VARCHAR, -1),
        ("VIEWNAME", OID_VARCHAR, -1),
        ("OWNER", OID_VARCHAR, -1),
        ("OBJID", OID_INT4, 4),
        ("DEFINITION", OID_VARCHAR, -1),
    ];
    items.push((
        "SELECT schema, viewname, owner, objid, definition FROM _v_view WHERE viewname IS NOT NULL \
         AND schema = 'ADMIN' ORDER BY schema, viewname"
            .into(),
        response(
            &view_columns,
            &[vec![
                Some("ADMIN"),
                Some("RUST_META_V"),
                Some("ADMIN"),
                Some("43"),
                Some("SELECT ID FROM RUST_META"),
            ]],
        ),
    ));
    let procedure_columns = [
        ("SCHEMA", OID_VARCHAR, -1),
        ("PROCEDURE", OID_VARCHAR, -1),
        ("OWNER", OID_VARCHAR, -1),
        ("OBJID", OID_INT4, 4),
        ("PROCEDURESIGNATURE", OID_VARCHAR, -1),
        ("RETURNS", OID_VARCHAR, -1),
        ("BUILTIN", OID_BOOL, 1),
        ("PROCEDURESOURCE", OID_VARCHAR, -1),
        ("EXECUTEDASOWNER", OID_BOOL, 1),
        ("ARGUMENTS", OID_VARCHAR, -1),
        ("DESCRIPTION", OID_VARCHAR, -1),
    ];
    items.push((
        "SELECT schema, procedure, owner, objid, proceduresignature, returns, builtin, \
         proceduresource, executedasowner, arguments, description FROM _v_procedure \
         WHERE procedure IS NOT NULL AND schema = 'ADMIN' ORDER BY schema, procedure"
            .into(),
        response(
            &procedure_columns,
            &[vec![
                Some("ADMIN"),
                Some("RUST_META_P"),
                Some("ADMIN"),
                Some("44"),
                Some("RUST_META_P(INTEGER)"),
                Some("INTEGER"),
                Some("f"),
                Some("BEGIN RETURN; END;"),
                Some("f"),
                Some("INTEGER"),
                Some("meta proc"),
            ]],
        ),
    ));
    items.push((
        "SELECT attname FROM _v_table_dist_map WHERE tablename = 'RUST_META' \
         AND schema = 'ADMIN' ORDER BY distattnum"
            .into(),
        response(&[("ATTNAME", OID_VARCHAR, -1)], &[vec![Some("ID")]]),
    ));
    items.push((
        "SELECT attname FROM _v_table_organize_column WHERE tablename = 'RUST_META' \
         AND schema = 'ADMIN' ORDER BY orgseqno"
            .into(),
        response(
            &[("ATTNAME", OID_VARCHAR, -1)],
            &[vec![Some("ID")], vec![Some("NAME")]],
        ),
    ));
    items.push((
        "SELECT constraintname, contype, attname, pkdatabase, pkschema, pkrelation, pkattname, \
         updt_type, del_type FROM _v_relation_keydata WHERE relation = 'RUST_META' \
         AND schema = 'ADMIN' ORDER BY constraintname, conseq"
            .into(),
        response(
            &[
                ("CONSTRAINTNAME", OID_VARCHAR, -1),
                ("CONTYPE", OID_VARCHAR, -1),
                ("ATTNAME", OID_VARCHAR, -1),
                ("PKDATABASE", OID_VARCHAR, -1),
                ("PKSCHEMA", OID_VARCHAR, -1),
                ("PKRELATION", OID_VARCHAR, -1),
                ("PKATTNAME", OID_VARCHAR, -1),
                ("UPDT_TYPE", OID_VARCHAR, -1),
                ("DEL_TYPE", OID_VARCHAR, -1),
            ],
            &[
                vec![
                    Some("PK_RUST_META"),
                    Some("p"),
                    Some("ID"),
                    Some("JUST_DATA"),
                    Some("ADMIN"),
                    Some("RUST_META"),
                    Some("ID"),
                    Some("NO ACTION"),
                    Some("NO ACTION"),
                ],
                vec![
                    Some("PK_RUST_META"),
                    Some("p"),
                    Some("NAME"),
                    Some("JUST_DATA"),
                    Some("ADMIN"),
                    Some("RUST_META"),
                    Some("NAME"),
                    Some("NO ACTION"),
                    Some("NO ACTION"),
                ],
            ],
        ),
    ));
    items.push((
        "SELECT description FROM _v_object_data WHERE objname = 'RUST_META' \
         AND schema = 'ADMIN' AND objtype = 'TABLE'"
            .into(),
        response(
            &[("DESCRIPTION", OID_VARCHAR, -1)],
            &[vec![Some("meta table comment")]],
        ),
    ));
    items.push((
        "SELECT owner FROM _v_table WHERE tablename = 'RUST_META' AND schema = 'ADMIN'".into(),
        response(&[("OWNER", OID_VARCHAR, -1)], &[vec![Some("ADMIN")]]),
    ));
    items.push((
        "SELECT schema, attname, attnum, format_type, attnotnull, coldefault, description \
         FROM _v_relation_column WHERE name = 'RUST_META' AND schema = 'ADMIN' ORDER BY attnum"
            .into(),
        response(
            &[
                ("SCHEMA", OID_VARCHAR, -1),
                ("ATTNAME", OID_VARCHAR, -1),
                ("ATTNUM", OID_SMALLINT, 2),
                ("FORMAT_TYPE", OID_VARCHAR, -1),
                ("ATTNOTNULL", OID_BOOL, 1),
                ("COLDEFAULT", OID_VARCHAR, -1),
                ("DESCRIPTION", OID_VARCHAR, -1),
            ],
            &[
                vec![
                    Some("ADMIN"),
                    Some("ID"),
                    Some("1"),
                    Some("INTEGER"),
                    Some("t"),
                    None,
                    None,
                ],
                vec![
                    Some("ADMIN"),
                    Some("NAME"),
                    Some("2"),
                    Some("VARCHAR(20)"),
                    Some("f"),
                    None,
                    Some("display name"),
                ],
            ],
        ),
    ));
    items.push((
        "SELECT schema, tablename, used_bytes, allocated_bytes, \
         (used_bytes / 1048576)::BIGINT AS size_mb, skew FROM _v_table_storage_stat \
         WHERE tablename IS NOT NULL AND schema = 'ADMIN' ORDER BY used_bytes DESC"
            .into(),
        response(
            &[
                ("SCHEMA", OID_VARCHAR, -1),
                ("TABLENAME", OID_VARCHAR, -1),
                ("USED_BYTES", OID_INT8, 8),
                ("ALLOCATED_BYTES", OID_INT8, 8),
                ("SIZE_MB", OID_INT8, 8),
                ("SKEW", OID_FLOAT8, 8),
            ],
            &[vec![
                Some("ADMIN"),
                Some("RUST_META"),
                Some("1048576"),
                Some("2097152"),
                Some("1"),
                Some("0.25"),
            ]],
        ),
    ));
    items.push((
        "SELECT id, username, dbname, conntime, priority, status, type, client_os_username \
         FROM _v_session ORDER BY conntime DESC"
            .into(),
        response(
            &[
                ("ID", OID_INT8, 8),
                ("USERNAME", OID_VARCHAR, -1),
                ("DBNAME", OID_VARCHAR, -1),
                ("CONNTIME", OID_VARCHAR, -1),
                ("PRIORITY", OID_VARCHAR, -1),
                ("STATUS", OID_VARCHAR, -1),
                ("TYPE", OID_VARCHAR, -1),
                ("CLIENT_OS_USERNAME", OID_VARCHAR, -1),
            ],
            &[vec![
                Some("12345"),
                Some("ADMIN"),
                Some("JUST_DATA"),
                Some("2026-01-02 03:04:05"),
                Some("normal"),
                Some("active"),
                Some("JDBC"),
                Some("ADMIN"),
            ]],
        ),
    ));
    items.push((
        "SELECT schema, function, owner, objid, functionsignature, returns, env FROM _v_function \
         WHERE function IS NOT NULL AND schema = 'ADMIN' ORDER BY schema, function"
            .into(),
        response(
            &[
                ("SCHEMA", OID_VARCHAR, -1),
                ("FUNCTION", OID_VARCHAR, -1),
                ("OWNER", OID_VARCHAR, -1),
                ("OBJID", OID_INT4, 4),
                ("FUNCTIONSIGNATURE", OID_VARCHAR, -1),
                ("RETURNS", OID_VARCHAR, -1),
                ("ENV", OID_VARCHAR, -1),
            ],
            &[vec![
                Some("ADMIN"),
                Some("F_META"),
                Some("ADMIN"),
                Some("45"),
                Some("F_META(INTEGER)"),
                Some("INTEGER"),
                Some("com.ibm.nz.fq.sqlreadlauncher"),
            ]],
        ),
    ));
    items.push((
        "SELECT schema, synonym_name, refobjname, refdatabase, refschema, description \
         FROM _v_synonym WHERE synonym_name IS NOT NULL AND schema = 'ADMIN' \
         ORDER BY schema, synonym_name"
            .into(),
        response(
            &[
                ("SCHEMA", OID_VARCHAR, -1),
                ("SYNONYM_NAME", OID_VARCHAR, -1),
                ("REFOBJNAME", OID_VARCHAR, -1),
                ("REFDATABASE", OID_VARCHAR, -1),
                ("REFSCHEMA", OID_VARCHAR, -1),
                ("DESCRIPTION", OID_VARCHAR, -1),
            ],
            &[vec![
                Some("ADMIN"),
                Some("SYN_META"),
                Some("RUST_META"),
                Some("JUST_DATA"),
                Some("ADMIN"),
                Some("synonym comment"),
            ]],
        ),
    ));
    items.push((
        "SELECT schema, relation, constraintname, contype, attname, pkdatabase, pkschema, \
         pkrelation, pkattname, updt_type, del_type FROM _v_relation_keydata \
         WHERE relation IS NOT NULL AND schema = 'ADMIN' ORDER BY schema, relation, conseq"
            .into(),
        response(
            &[
                ("SCHEMA", OID_VARCHAR, -1),
                ("RELATION", OID_VARCHAR, -1),
                ("CONSTRAINTNAME", OID_VARCHAR, -1),
                ("CONTYPE", OID_VARCHAR, -1),
                ("ATTNAME", OID_VARCHAR, -1),
                ("PKDATABASE", OID_VARCHAR, -1),
                ("PKSCHEMA", OID_VARCHAR, -1),
                ("PKRELATION", OID_VARCHAR, -1),
                ("PKATTNAME", OID_VARCHAR, -1),
                ("UPDT_TYPE", OID_VARCHAR, -1),
                ("DEL_TYPE", OID_VARCHAR, -1),
            ],
            &[vec![
                Some("ADMIN"),
                Some("RUST_META"),
                Some("PK_RUST_META"),
                Some("p"),
                Some("ID"),
                Some("JUST_DATA"),
                Some("ADMIN"),
                Some("RUST_META"),
                Some("ID"),
                Some("NO ACTION"),
                Some("NO ACTION"),
            ]],
        ),
    ));
    items.push((
        "SELECT schema, tablename, attname, distattnum FROM _v_table_dist_map \
         WHERE tablename IS NOT NULL AND schema = 'ADMIN' \
         ORDER BY schema, tablename, distseqno"
            .into(),
        response(
            &[
                ("SCHEMA", OID_VARCHAR, -1),
                ("TABLENAME", OID_VARCHAR, -1),
                ("ATTNAME", OID_VARCHAR, -1),
                ("DISTATTNUM", OID_INT4, 4),
            ],
            &[vec![
                Some("ADMIN"),
                Some("RUST_META"),
                Some("ID"),
                Some("1"),
            ]],
        ),
    ));
    items.push((
        "SELECT schema, tablename, attname, attnum FROM _v_table_organize_column \
         WHERE tablename IS NOT NULL AND schema = 'ADMIN' \
         ORDER BY schema, tablename, orgseqno"
            .into(),
        response(
            &[
                ("SCHEMA", OID_VARCHAR, -1),
                ("TABLENAME", OID_VARCHAR, -1),
                ("ATTNAME", OID_VARCHAR, -1),
                ("ATTNUM", OID_INT4, 4),
            ],
            &[vec![
                Some("ADMIN"),
                Some("RUST_META"),
                Some("ID"),
                Some("1"),
            ]],
        ),
    ));
    let object_columns = [
        ("SCHEMA", OID_VARCHAR, -1),
        ("OBJNAME", OID_VARCHAR, -1),
        ("OBJTYPE", OID_VARCHAR, -1),
        ("OWNER", OID_VARCHAR, -1),
        ("OBJID", OID_INT4, 4),
        ("DESCRIPTION", OID_VARCHAR, -1),
        ("CREATEDATE", OID_VARCHAR, -1),
    ];
    let object_row = vec![
        Some("ADMIN"),
        Some("RUST_META"),
        Some("TABLE"),
        Some("ADMIN"),
        Some("42"),
        None,
        Some("2026-01-02 03:04:05"),
    ];
    let object_filter = " AND objtype NOT IN ('AGGREGATE','CONSTRAINT','DATABASE','DATATYPE',\
                         'GROUP','MANAGEMENT INDEX','MANAGEMENT SEQ','MANAGEMENT TABLE',\
                         'MANAGEMENT VIEW','SCHEDULER RULE','SCHEMA','SYSTEM INDEX','SYSTEM SEQ',\
                         'SYSTEM TABLE','SYSTEM VIEW','USER') ORDER BY schema, objtype, objname";
    for (prefix, pattern) in [
        (
            "SELECT schema, objname, objtype, owner, objid, description, createdate \
             FROM _v_object_data WHERE objname IS NOT NULL AND schema = 'ADMIN'",
            "",
        ),
        (
            "SELECT schema, objname, objtype, owner, objid, description, createdate \
             FROM _v_object_data WHERE UPPER(objname) LIKE UPPER('%meta%') AND schema = 'ADMIN'",
            "",
        ),
    ] {
        items.push((
            format!("{prefix}{object_filter}"),
            response(&object_columns, std::slice::from_ref(&object_row)),
        ));
        let _ = pattern;
    }
    items.push((
        "SELECT schema, seqname, owner, objid FROM _v_sequence WHERE seqname IS NOT NULL \
         AND schema = 'ADMIN' ORDER BY schema, seqname"
            .into(),
        response(
            &[
                ("SCHEMA", OID_VARCHAR, -1),
                ("SEQNAME", OID_VARCHAR, -1),
                ("OWNER", OID_VARCHAR, -1),
                ("OBJID", OID_INT4, 4),
            ],
            &[vec![
                Some("ADMIN"),
                Some("SEQ_META"),
                Some("ADMIN"),
                Some("46"),
            ]],
        ),
    ));
    items.push((
        "SELECT username, objid FROM _v_user ORDER BY username".into(),
        response(
            &[("USERNAME", OID_VARCHAR, -1), ("OBJID", OID_INT4, 4)],
            &[vec![Some("ADMIN"), Some("1")]],
        ),
    ));
    items.push((
        "SELECT groupname, objid FROM _v_group ORDER BY groupname".into(),
        response(
            &[("GROUPNAME", OID_VARCHAR, -1), ("OBJID", OID_INT4, 4)],
            &[vec![Some("PUBLIC"), Some("2")]],
        ),
    ));
    items.push((
        "SELECT qh_sessionid, qh_user, qh_database, qh_sql, qh_tsubmit, qh_tstart, qh_resrows \
         FROM _v_qryhist WHERE 1=1 AND qh_user = 'ADMIN' ORDER BY qh_tsubmit DESC LIMIT 5"
            .into(),
        response(
            &[
                ("QH_SESSIONID", OID_INT8, 8),
                ("QH_USER", OID_VARCHAR, -1),
                ("QH_DATABASE", OID_VARCHAR, -1),
                ("QH_SQL", OID_VARCHAR, -1),
                ("QH_TSUBMIT", OID_VARCHAR, -1),
                ("QH_TSTART", OID_VARCHAR, -1),
                ("QH_RESROWS", OID_INT8, 8),
            ],
            &[vec![
                Some("12345"),
                Some("ADMIN"),
                Some("JUST_DATA"),
                Some("SELECT 1"),
                Some("2026-01-02 03:04:05"),
                Some("2026-01-02 03:04:06"),
                Some("1"),
            ]],
        ),
    ));

    items
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_metadata_surface_decodes_catalog_rows() {
    let answers = catalog_responses();
    let server = MockServer::start(HandshakeScript::default(), move |session| {
        while let Some(sql) = session.read_query() {
            let Some((_, response)) = answers.iter().find(|(expected, _)| *expected == sql) else {
                panic!("unexpected metadata query: {sql}");
            };
            if session.send(response).is_err() {
                return;
            }
        }
    });

    let client = nz_rust::Client::connect(&server.config()).await.unwrap();
    let metadata = client.metadata();

    assert_eq!(
        metadata.current_database().await.unwrap(),
        Some("JUST_DATA".into())
    );
    assert_eq!(
        metadata.schemas().await.unwrap(),
        ["ADMIN".to_string(), "JUST_DATA".to_string()]
    );
    let databases = metadata.databases().await.unwrap();
    assert_eq!(databases.len(), 1);
    assert_eq!(databases[0].name, "JUST_DATA");
    assert_eq!(databases[0].default_schema.as_deref(), Some("ADMIN"));

    let tables = metadata
        .tables(Some("ADMIN"), Some("RUST_META%"))
        .await
        .unwrap();
    assert_eq!(tables.len(), 1);
    assert_eq!(tables[0].name, "RUST_META");
    assert_eq!(tables[0].row_count, Some(7));
    assert_eq!(tables[0].object_id, Some(42));

    let columns = metadata.columns("RUST_META", Some("ADMIN")).await.unwrap();
    assert_eq!(columns.len(), 2);
    assert_eq!(columns[0].ordinal, 1);
    assert!(!columns[0].nullable);
    assert_eq!(columns[1].description.as_deref(), Some("display name"));
    assert_eq!(columns[1].type_name, "VARCHAR(20)");

    let views = metadata.views(Some("ADMIN")).await.unwrap();
    assert_eq!(views[0].name, "RUST_META_V");
    assert_eq!(
        views[0].definition.as_deref(),
        Some("SELECT ID FROM RUST_META")
    );

    let procedures = metadata.procedures(Some("ADMIN")).await.unwrap();
    assert_eq!(procedures[0].name, "RUST_META_P");
    assert_eq!(procedures[0].is_builtin, Some(false));
    assert_eq!(procedures[0].arguments.as_deref(), Some("INTEGER"));

    assert_eq!(
        metadata
            .distribution_key("RUST_META", Some("ADMIN"))
            .await
            .unwrap(),
        ["ID"]
    );
    assert_eq!(
        metadata
            .organize_columns("RUST_META", Some("ADMIN"))
            .await
            .unwrap(),
        ["ID", "NAME"]
    );
    let keys = metadata
        .table_keys("RUST_META", Some("ADMIN"))
        .await
        .unwrap();
    assert_eq!(keys.len(), 1);
    assert_eq!(keys[0].key_type, "PRIMARY KEY");
    assert_eq!(keys[0].columns, ["ID", "NAME"]);
    assert_eq!(
        metadata
            .table_comment("RUST_META", Some("ADMIN"))
            .await
            .unwrap()
            .as_deref(),
        Some("meta table comment")
    );

    let detailed = metadata
        .detailed_columns("RUST_META", Some("ADMIN"))
        .await
        .unwrap();
    assert_eq!(detailed.len(), 2);
    assert!(detailed[0].not_null);
    assert_eq!(detailed[1].default_value, None);
    assert_eq!(detailed[1].description.as_deref(), Some("display name"));

    assert_eq!(
        metadata.current_schema().await.unwrap().as_deref(),
        Some("ADMIN")
    );
    assert_eq!(
        metadata
            .table_owner("RUST_META", Some("ADMIN"))
            .await
            .unwrap()
            .as_deref(),
        Some("ADMIN")
    );
    let sizes = metadata.table_sizes(Some("ADMIN")).await.unwrap();
    assert_eq!(sizes[0].size_mb, Some(1));
    assert_eq!(sizes[0].skew, Some(0.25));
    let sessions = metadata.sessions().await.unwrap();
    assert_eq!(sessions[0].session_id, 12345);
    let functions = metadata.functions(Some("ADMIN")).await.unwrap();
    assert_eq!(functions[0].is_fluid, Some(true));
    let synonyms = metadata.synonyms(Some("ADMIN")).await.unwrap();
    assert_eq!(synonyms[0].name, "SYN_META");
    let constraints = metadata.constraints(Some("ADMIN")).await.unwrap();
    assert_eq!(constraints[0].constraint_type, 'p');
    assert_eq!(
        metadata.all_distribution_keys(Some("ADMIN")).await.unwrap()[0].distribution_attribute,
        1
    );
    assert_eq!(
        metadata.organize_keys(Some("ADMIN")).await.unwrap()[0].column,
        "ID"
    );
    assert_eq!(
        metadata.object_details(Some("ADMIN")).await.unwrap()[0].name,
        "RUST_META"
    );
    let searched = metadata
        .search_objects_detailed("meta", Some("ADMIN"))
        .await
        .unwrap();
    assert_eq!(searched.len(), 1);
    let objects = metadata.objects("meta", Some("ADMIN")).await.unwrap();
    assert_eq!(
        objects
            .iter()
            .map(|object| (object.name.as_str(), object.object_type.as_str()))
            .collect::<Vec<_>>(),
        [
            ("RUST_META", "TABLE"),
            ("RUST_META_V", "VIEW"),
            ("RUST_META_P", "PROCEDURE"),
        ]
    );
    let sequences = metadata.sequences(Some("ADMIN")).await.unwrap();
    assert_eq!(sequences[0].name, "SEQ_META");
    assert_eq!(metadata.users().await.unwrap()[0].name, "ADMIN");
    assert_eq!(metadata.groups().await.unwrap()[0].name, "PUBLIC");
    let history = metadata.query_history(5, Some("ADMIN")).await.unwrap();
    assert_eq!(history[0].result_rows, Some(1));

    client.close().await.unwrap();
    server.assert_no_handler_panics();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn legacy_metadata_surface_decodes_extended_catalog_rows() {
    let answers = catalog_responses();
    let server = MockServer::start(HandshakeScript::default(), move |session| {
        while let Some(sql) = session.read_query() {
            let Some((_, response)) = answers.iter().find(|(expected, _)| *expected == sql) else {
                panic!("unexpected metadata query: {sql}");
            };
            if session.send(response).is_err() {
                return;
            }
        }
    });

    let mut connection = nz_rust::NzConnection::connect(&server.config()).unwrap();
    let mut metadata: NzMetadata<'_> = connection.metadata();

    assert_eq!(
        metadata.current_database().unwrap(),
        Some("JUST_DATA".into())
    );
    assert_eq!(metadata.current_schema().unwrap(), Some("ADMIN".into()));
    assert_eq!(
        metadata.schemas().unwrap(),
        ["ADMIN".to_string(), "JUST_DATA".to_string()]
    );
    let databases = metadata.databases().unwrap();
    assert_eq!(databases[0].owner.as_deref(), Some("ADMIN"));

    let tables = metadata.tables(Some("ADMIN"), Some("RUST_META%")).unwrap();
    assert_eq!(tables[0].object_type.as_deref(), Some("TABLE"));
    assert_eq!(tables[0].row_count, Some(7));

    let columns = metadata.columns("RUST_META", Some("ADMIN")).unwrap();
    assert_eq!(columns[0].name, "ID");
    assert!(!columns[0].nullable);

    let views = metadata.views(Some("ADMIN")).unwrap();
    assert_eq!(views[0].name, "RUST_META_V");
    let procedures = metadata.procedures(Some("ADMIN")).unwrap();
    assert_eq!(procedures[0].source.as_deref(), Some("BEGIN RETURN; END;"));

    assert_eq!(
        metadata
            .distribution_key("RUST_META", Some("ADMIN"))
            .unwrap(),
        ["ID"]
    );
    assert_eq!(
        metadata
            .organize_columns("RUST_META", Some("ADMIN"))
            .unwrap(),
        ["ID", "NAME"]
    );
    assert_eq!(
        metadata
            .table_owner("RUST_META", Some("ADMIN"))
            .unwrap()
            .as_deref(),
        Some("ADMIN")
    );
    assert_eq!(
        metadata
            .table_comment("RUST_META", Some("ADMIN"))
            .unwrap()
            .as_deref(),
        Some("meta table comment")
    );
    let keys = metadata.table_keys("RUST_META", Some("ADMIN")).unwrap();
    assert_eq!(keys[0].key_type, "PRIMARY KEY");
    assert_eq!(keys[0].columns, ["ID", "NAME"]);
    assert_eq!(keys[0].pk_relation.as_deref(), Some("RUST_META"));

    let sizes = metadata.table_sizes(Some("ADMIN")).unwrap();
    assert_eq!(sizes[0].table, "RUST_META");
    assert_eq!(sizes[0].used_bytes, Some(1_048_576));
    assert_eq!(sizes[0].size_mb, Some(1));
    assert_eq!(sizes[0].skew, Some(0.25));

    let sessions = metadata.sessions().unwrap();
    assert_eq!(sessions[0].session_id, 12345);
    assert_eq!(sessions[0].status.as_deref(), Some("active"));

    let functions = metadata.functions(Some("ADMIN")).unwrap();
    assert_eq!(functions[0].name, "F_META");
    assert_eq!(functions[0].is_fluid, Some(true));

    let synonyms = metadata.synonyms(Some("ADMIN")).unwrap();
    assert_eq!(synonyms[0].name, "SYN_META");
    assert_eq!(synonyms[0].referenced_object.as_deref(), Some("RUST_META"));

    let constraints = metadata.constraints(Some("ADMIN")).unwrap();
    assert_eq!(constraints[0].constraint_type, 'p');
    assert_eq!(constraints[0].column, "ID");

    let distribution = metadata.all_distribution_keys(Some("ADMIN")).unwrap();
    assert_eq!(distribution[0].distribution_attribute, 1);
    let organize = metadata.organize_keys(Some("ADMIN")).unwrap();
    assert_eq!(organize[0].column, "ID");
    let details = metadata.object_details(Some("ADMIN")).unwrap();
    assert_eq!(details[0].name, "RUST_META");
    assert_eq!(details[0].object_id, Some(42));

    let searched = metadata
        .search_objects_detailed("meta", Some("ADMIN"))
        .unwrap();
    assert_eq!(searched.len(), 1);
    assert_eq!(searched[0].object_type, "TABLE");

    let objects = metadata.objects("meta", Some("ADMIN")).unwrap();
    assert_eq!(
        objects
            .iter()
            .map(|object| (object.name.as_str(), object.object_type.as_str()))
            .collect::<Vec<_>>(),
        [
            ("RUST_META", "TABLE"),
            ("RUST_META_V", "VIEW"),
            ("RUST_META_P", "PROCEDURE"),
        ]
    );

    let sequences = metadata.sequences(Some("ADMIN")).unwrap();
    assert_eq!(sequences[0].name, "SEQ_META");
    let users = metadata.users().unwrap();
    assert_eq!(users[0].name, "ADMIN");
    let groups = metadata.groups().unwrap();
    assert_eq!(groups[0].name, "PUBLIC");
    let history = metadata.query_history(5, Some("ADMIN")).unwrap();
    assert_eq!(history[0].result_rows, Some(1));
    assert_eq!(history[0].query_text.as_deref(), Some("SELECT 1"));

    let detailed = metadata
        .detailed_columns("RUST_META", Some("ADMIN"))
        .unwrap();
    assert!(detailed[0].not_null);
    assert_eq!(detailed[1].description.as_deref(), Some("display name"));

    connection.close();
    server.assert_no_handler_panics();
}
