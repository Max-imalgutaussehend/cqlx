use cqlx_cli::{diff, render_schema, scan};

const SRC: &str = r#"
use cqlx::CqlEntity;

#[derive(Debug, CqlEntity)]
#[cqlx(table_name = "events", keyspace = "app")]
pub struct Event {
    #[cqlx(partition_key)] tenant: String,
    #[cqlx(clustering_key(order = "desc"))] ts: i64,
    #[cqlx(rename = "payload")] body: Option<String>,
    tags: std::collections::HashSet<String>,
}

mod inner {
    #[derive(cqlx::CqlEntity)]
    pub struct UserProfile { #[cqlx(primary_key)] id: uuid::Uuid, age: i32 }
}

struct NotAnEntity { x: i32 }
"#;

fn fixture() -> tempfile::TempDir {
    let d = tempfile::tempdir().unwrap();
    std::fs::write(d.path().join("model.rs"), SRC).unwrap();
    d
}

#[test]
fn scans_nested_and_ignores_plain_structs() {
    let d = fixture();
    let found = scan(d.path()).unwrap();
    let names: Vec<_> = found.iter().map(|f| f.meta.qualified_name()).collect();
    assert_eq!(names, ["app.events", "user_profile"]);
}

#[test]
fn generated_ddl_is_accepted_by_the_cqlc_front_end() {
    let d = fixture();
    let ddl = render_schema(&scan(d.path()).unwrap(), true);
    assert!(ddl.contains("CREATE KEYSPACE IF NOT EXISTS app"));
    let c = cqlc_core::compile(&[cqlc_core::Input::new("gen.cql", ddl)], &[]);
    let errors: Vec<_> = c
        .diagnostics
        .iter()
        .filter(|d| d.is_error())
        .map(|d| d.message.clone())
        .collect();
    assert!(errors.is_empty(), "{errors:?}");
    assert_eq!(c.schema.tables.len(), 2);
    let ev = c.schema.tables.iter().find(|t| t.name == "events").unwrap();
    assert_eq!(ev.partition_key, ["tenant"]);
    assert_eq!(ev.clustering[0].name, "ts");
}

#[test]
fn diff_reports_additions_and_warnings() {
    let d = fixture();
    let found = scan(d.path()).unwrap();
    let existing = "CREATE KEYSPACE app WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1};\n\
        CREATE TABLE app.events (tenant text, ts bigint, payload int, legacy text, PRIMARY KEY (tenant, ts));";
    let out = diff(&found, existing).unwrap();
    assert!(
        out.contains("ALTER TABLE app.events ADD tags set<text>;"),
        "{out}"
    );
    assert!(
        out.contains("WARNING: app.events.payload changes type int -> text"),
        "{out}"
    );
    assert!(
        out.contains("app.events.legacy exists in the schema but not in code"),
        "{out}"
    );
    assert!(
        out.contains("CREATE TABLE IF NOT EXISTS user_profile"),
        "{out}"
    );
}

#[test]
fn up_to_date_schema_yields_empty_diff() {
    let d = fixture();
    let found = scan(d.path()).unwrap();
    let ddl = render_schema(&found, false);
    assert_eq!(diff(&found, &ddl).unwrap(), "");
}
