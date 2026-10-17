//! Offline tests: macro expansion, generated CQL and the type-state builder's rendered statements.

use cqlx::scylla::DeserializeRow;
use cqlx::{CqlEntity, CqlModel};

#[derive(CqlEntity, DeserializeRow, Debug, PartialEq)]
#[cqlx(table_name = "events", keyspace = "app")]
struct Event {
    #[cqlx(partition_key)]
    tenant: String,
    #[cqlx(partition_key)]
    day: i32,
    #[cqlx(clustering_key(order = "desc"))]
    ts: i64,
    #[cqlx(clustering_key)]
    seq: i32,
    #[cqlx(rename = "payload")]
    body: String,
}

#[derive(CqlEntity, DeserializeRow)]
struct Plain {
    #[cqlx(primary_key)]
    id: i32,
}

#[test]
fn constants_are_rendered_at_compile_time() {
    assert_eq!(Event::TABLE, "app.events");
    assert_eq!(
        Event::INSERT_CQL,
        "INSERT INTO app.events (tenant, day, ts, seq, payload) VALUES (?, ?, ?, ?, ?)"
    );
    assert!(Event::CREATE_TABLE_CQL.contains("PRIMARY KEY ((tenant, day), ts, seq)"));
    assert!(Event::CREATE_TABLE_CQL.contains("WITH CLUSTERING ORDER BY (ts DESC, seq ASC)"));
    assert!(Event::CREATE_KEYSPACE_CQL.unwrap().contains("app"));
    assert_eq!(Plain::TABLE, "plain");
    assert_eq!(Event::meta().columns[4].name, "payload");
}

#[test]
fn partition_keys_in_any_order_then_clustering() {
    let built = Event::query()
        .day(3) // order of partition setters is free
        .tenant("acme".into())
        .ts_eq(10)
        .seq_gt(1)
        .seq_le(9)
        .limit(5)
        .build()
        .unwrap();
    assert_eq!(
        built.cql,
        "SELECT tenant, day, ts, seq, payload AS body FROM app.events \
         WHERE tenant = ? AND day = ? AND ts = ? AND seq > ? AND seq <= ? LIMIT 5"
    );
    assert_eq!(built.values.len(), 5);
}

#[test]
fn delete_statement() {
    let built = Plain::query().id(1).build_delete().unwrap();
    assert_eq!(built.cql, "DELETE FROM plain WHERE id = ?");
}

#[test]
fn clustering_gap_is_rejected_at_build() {
    let err = Event::query()
        .tenant("a".into())
        .day(1)
        .seq_eq(1)
        .build()
        .err()
        .unwrap();
    assert!(err.to_string().contains("skips"), "{err}");
    let err = Event::query()
        .tenant("a".into())
        .day(1)
        .ts_gt(1)
        .seq_eq(1)
        .build()
        .err()
        .unwrap();
    assert!(err.to_string().contains("follows a range"), "{err}");
}

#[test]
fn compile_fail_cases() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/ui/*.rs");
}
