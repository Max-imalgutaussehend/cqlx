//! Offline tests: macro expansion, generated CQL and the type-state builders' rendered statements.

use cqlx::scylla::DeserializeRow;
use cqlx::scylla::value::Counter;
use cqlx::{CqlEntity, CqlModel};
use std::collections::HashSet;

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
    level: Option<i32>,
    tags: HashSet<String>,
}

#[derive(CqlEntity, DeserializeRow)]
struct Plain {
    #[cqlx(primary_key)]
    id: i32,
}

#[derive(CqlEntity, DeserializeRow)]
#[cqlx(table_name = "page_hits", keyspace = "app")]
struct Hits {
    #[cqlx(partition_key)]
    page: String,
    views: Counter,
    clicks: Counter,
}

#[test]
fn constants_are_rendered_at_compile_time() {
    assert_eq!(Event::TABLE, "app.events");
    assert_eq!(
        Event::INSERT_CQL,
        "INSERT INTO app.events (tenant, day, ts, seq, payload, level, tags) VALUES (?, ?, ?, ?, ?, ?, ?)"
    );
    assert_eq!(
        Event::DELETE_CQL,
        "DELETE FROM app.events WHERE tenant = ? AND day = ? AND ts = ? AND seq = ?"
    );
    assert!(Event::CREATE_TABLE_CQL.contains("PRIMARY KEY ((tenant, day), ts, seq)"));
    assert!(Event::CREATE_TABLE_CQL.contains("WITH CLUSTERING ORDER BY (ts DESC, seq ASC)"));
    assert!(Event::CREATE_TABLE_CQL.contains("level int,"));
    assert!(Event::CREATE_TABLE_CQL.contains("tags set<text>"));
    assert_eq!(Plain::TABLE, "plain");
    assert_eq!(Event::meta().columns[4].name, "payload");
}

#[test]
fn select_in_any_partition_order_then_clustering() {
    let built = Event::query()
        .day(3)
        .tenant("acme".into())
        .ts_eq(10)
        .seq_gt(1)
        .seq_le(9)
        .limit(5)
        .build()
        .unwrap();
    assert_eq!(
        built.cql,
        "SELECT tenant, day, ts, seq, payload AS body, level, tags FROM app.events \
         WHERE tenant = ? AND day = ? AND ts = ? AND seq > ? AND seq <= ? LIMIT 5"
    );
    assert_eq!(built.values.len(), 5);
    assert_eq!(built.limit, Some(5));
}

#[test]
fn in_restrictions_reverse_order_count_and_options() {
    let built = Event::query()
        .tenant_in(vec!["a".into(), "b".into()])
        .day(1)
        .ts_in(vec![1, 2])
        .reverse()
        .build()
        .unwrap();
    assert!(
        built
            .cql
            .ends_with("WHERE tenant IN ? AND day = ? AND ts IN ? ORDER BY ts ASC, seq DESC"),
        "{}",
        built.cql
    );
    let built = Event::query()
        .tenant("a".into())
        .day(1)
        .consistency(cqlx::Consistency::Quorum)
        .page_size(50)
        .build()
        .unwrap();
    assert_eq!(built.consistency, Some(cqlx::Consistency::Quorum));
    assert_eq!(built.page_size, Some(50));
    let count = Event::query()
        .tenant("a".into())
        .day(1)
        .build_count()
        .unwrap();
    assert_eq!(
        count.cql,
        "SELECT COUNT(*) FROM app.events WHERE tenant = ? AND day = ?"
    );
}

#[test]
fn allow_filtering_unlocks_non_key_filters() {
    let built = Event::query()
        .tenant("a".into())
        .day(1)
        .allow_filtering()
        .where_level_eq(3)
        .where_body_eq("x".into())
        .build()
        .unwrap();
    assert!(
        built.cql.ends_with(
            "WHERE tenant = ? AND day = ? AND level = ? AND payload = ? ALLOW FILTERING"
        ),
        "{}",
        built.cql
    );
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
fn update_builder_renders_every_assignment_kind() {
    let built = Event::update()
        .tenant("a".into())
        .day(1)
        .ts(2)
        .seq(3)
        .set_body("x".into())
        .set_level(5)
        .add_tags(HashSet::from(["t".to_string()]))
        .remove_tags(HashSet::from(["u".to_string()]))
        .ttl(60)
        .timestamp(7)
        .build()
        .unwrap();
    assert_eq!(
        built.cql,
        "UPDATE app.events USING TTL 60 AND TIMESTAMP 7 SET payload = ?, level = ?, tags = tags + ?, tags = tags - ? \
         WHERE tenant = ? AND day = ? AND ts = ? AND seq = ?"
    );
    assert_eq!(built.values.len(), 8);
    assert!(!built.lwt);

    // keys may be provided in any order
    let built = Event::update()
        .seq(3)
        .ts(2)
        .day(1)
        .tenant("a".into())
        .set_level_null()
        .if_body_eq("x".into())
        .build()
        .unwrap();
    assert!(
        built
            .cql
            .ends_with("WHERE tenant = ? AND day = ? AND ts = ? AND seq = ? IF payload = ?"),
        "{}",
        built.cql
    );
    assert!(built.lwt);
    let built = Event::update()
        .seq(3)
        .ts(2)
        .day(1)
        .tenant("a".into())
        .set_level(1)
        .if_exists()
        .build()
        .unwrap();
    assert!(built.cql.ends_with(" IF EXISTS") && built.lwt);
}

#[test]
fn counter_updates() {
    let built = Hits::update()
        .page("/home".into())
        .increment_views(2)
        .decrement_clicks(1)
        .build()
        .unwrap();
    assert_eq!(
        built.cql,
        "UPDATE app.page_hits SET views = views + ?, clicks = clicks + ? WHERE page = ?"
    );
}

#[test]
fn compile_fail_cases() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/ui/*.rs");
}
