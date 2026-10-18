//! Integration tests against a real database: ScyllaDB in Docker (testcontainers), or any node named by
//! `CQLX_TEST_NODE` (CI runs ScyllaDB and Cassandra 5.0 this way).
//! Run with `cargo test -p cqlx --test scylla_integration -- --ignored`.

use cqlx::futures::StreamExt;
use cqlx::scylla::DeserializeRow;
use cqlx::scylla::value::Counter;
use cqlx::{BatchKind, Consistency, CqlEntity, CqlModel, InsertOptions, SchemaIssue, Session};
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

#[derive(CqlEntity, DeserializeRow, Debug, PartialEq, Clone)]
#[cqlx(table_name = "tickets", keyspace = "cqlx_it")]
struct Ticket {
    #[cqlx(partition_key)]
    tenant: String,
    #[cqlx(clustering_key(order = "desc"))]
    id: i32,
    title: String,
    #[cqlx(rename = "points")]
    score: Option<i32>,
    labels: HashSet<String>,
    props: HashMap<String, i32>,
    history: Vec<i32>,
}

#[derive(CqlEntity, DeserializeRow, Debug)]
#[cqlx(table_name = "page_hits", keyspace = "cqlx_it")]
struct Hits {
    #[cqlx(partition_key)]
    page: String,
    views: Counter,
    clicks: Counter,
}

#[derive(CqlEntity, DeserializeRow, Debug, Clone)]
#[cqlx(table_name = "stream_rows", keyspace = "cqlx_it")]
struct Row {
    #[cqlx(partition_key)]
    bucket: i32,
    #[cqlx(clustering_key)]
    seq: i32,
    data: String,
}

// An older shape of `tickets` to exercise validation and migration.
#[derive(CqlEntity, DeserializeRow, Debug)]
#[cqlx(table_name = "evolving", keyspace = "cqlx_it")]
struct EvolvingV1 {
    #[cqlx(partition_key)]
    id: i32,
    name: String,
}

#[derive(CqlEntity, DeserializeRow, Debug)]
#[cqlx(table_name = "evolving", keyspace = "cqlx_it")]
struct EvolvingV2 {
    #[cqlx(partition_key)]
    id: i32,
    name: String,
    email: Option<String>,
}

#[derive(CqlEntity, DeserializeRow, Debug)]
#[cqlx(table_name = "evolving", keyspace = "cqlx_it")]
struct EvolvingWrongType {
    #[cqlx(partition_key)]
    id: i32,
    name: i64,
}

fn ticket(tenant: &str, id: i32) -> Ticket {
    Ticket {
        tenant: tenant.into(),
        id,
        title: format!("ticket {id}"),
        score: Some(id * 10),
        labels: ["new".to_string()].into(),
        props: [("sev".to_string(), 3)].into(),
        history: vec![1, 2],
    }
}

async fn connect(node: &str) -> Session {
    let mut last = None;
    for _ in 0..120 {
        match Session::connect_single_node(node).await {
            Ok(s) => return s,
            Err(e) => {
                last = Some(e);
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    }
    panic!("database never accepted connections: {last:?}");
}

async fn start() -> (Option<ContainerAsync<GenericImage>>, Session) {
    // CI starts the database itself and points us at it; locally testcontainers does it.
    if let Ok(node) = std::env::var("CQLX_TEST_NODE") {
        return (None, connect(&node).await);
    }
    let mut container = None;
    for attempt in 1..=4 {
        let started = GenericImage::new("scylladb/scylla", "latest")
            .with_exposed_port(9042.tcp())
            .with_wait_for(WaitFor::message_on_stderr("serving"))
            .with_cmd([
                "--smp",
                "1",
                "--memory",
                "750M",
                "--overprovisioned",
                "1",
                "--developer-mode",
                "1",
            ])
            .with_startup_timeout(Duration::from_secs(180))
            .start()
            .await;
        match started {
            Ok(c) => {
                container = Some(c);
                break;
            }
            Err(e) if attempt < 4 => {
                eprintln!("container start failed (attempt {attempt}): {e}");
                tokio::time::sleep(Duration::from_secs(3)).await;
            }
            Err(e) => panic!("start scylla container: {e}"),
        }
    }
    let container = container.unwrap();
    let node = format!(
        "{}:{}",
        container.get_host().await.unwrap(),
        container.get_host_port_ipv4(9042).await.unwrap()
    );
    let session = connect(&node).await;
    (Some(container), session)
}

#[tokio::test]
#[ignore = "requires a database (docker or CQLX_TEST_NODE)"]
async fn production_feature_suite() {
    let (_container, session) = start().await;
    session.create_table::<Ticket>().await.unwrap();
    session.create_table::<Hits>().await.unwrap();
    session.create_table::<Row>().await.unwrap();

    // --- insert, select, range, first, count, reverse, IN
    for id in 1..=5 {
        session.insert(&ticket("acme", id)).await.unwrap();
    }
    session.insert(&ticket("globex", 1)).await.unwrap();
    let all = Ticket::query()
        .tenant("acme".into())
        .fetch(&session)
        .await
        .unwrap();
    assert_eq!(
        all.iter().map(|t| t.id).collect::<Vec<_>>(),
        [5, 4, 3, 2, 1],
        "clustering order DESC"
    );
    assert_eq!(all[0].score, Some(50));
    let mid = Ticket::query()
        .tenant("acme".into())
        .id_ge(2)
        .id_lt(5)
        .limit(2)
        .fetch(&session)
        .await
        .unwrap();
    assert_eq!(mid.iter().map(|t| t.id).collect::<Vec<_>>(), [4, 3]);
    let rev = Ticket::query()
        .tenant("acme".into())
        .reverse()
        .fetch(&session)
        .await
        .unwrap();
    assert_eq!(
        rev.iter().map(|t| t.id).collect::<Vec<_>>(),
        [1, 2, 3, 4, 5]
    );
    assert_eq!(
        Ticket::query()
            .tenant("acme".into())
            .count(&session)
            .await
            .unwrap(),
        5
    );
    let multi = Ticket::query()
        .tenant_in(vec!["acme".into(), "globex".into()])
        .id_eq(1)
        .fetch(&session)
        .await
        .unwrap();
    assert_eq!(multi.len(), 2);
    let ids_in = Ticket::query()
        .tenant("acme".into())
        .id_in(vec![1, 3])
        .fetch(&session)
        .await
        .unwrap();
    assert_eq!(ids_in.len(), 2);
    assert!(
        Ticket::query()
            .tenant("nobody".into())
            .first(&session)
            .await
            .unwrap()
            .is_none()
    );

    // --- partial UPDATE: only the named columns change; collection append/remove without rewriting
    Ticket::update()
        .tenant("acme".into())
        .id(3)
        .set_title("renamed".into())
        .add_labels(["urgent".to_string()].into())
        .execute(&session)
        .await
        .unwrap();
    let t3 = Ticket::query()
        .tenant("acme".into())
        .id_eq(3)
        .first(&session)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(t3.title, "renamed");
    assert_eq!(t3.score, Some(30), "untouched column keeps its value");
    assert_eq!(t3.labels, ["new".to_string(), "urgent".to_string()].into());
    Ticket::update()
        .tenant("acme".into())
        .id(3)
        .remove_labels(["new".to_string()].into())
        .set_score_null()
        .execute(&session)
        .await
        .unwrap();
    let t3 = Ticket::query()
        .tenant("acme".into())
        .id_eq(3)
        .first(&session)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(t3.labels, ["urgent".to_string()].into());
    assert_eq!(t3.score, None);

    // --- TTL
    session
        .insert_with(&ticket("ttl", 1), InsertOptions::new().ttl(2))
        .await
        .unwrap();
    assert!(
        Ticket::query()
            .tenant("ttl".into())
            .first(&session)
            .await
            .unwrap()
            .is_some()
    );
    tokio::time::sleep(Duration::from_secs(4)).await;
    assert!(
        Ticket::query()
            .tenant("ttl".into())
            .first(&session)
            .await
            .unwrap()
            .is_none(),
        "row expired"
    );

    // --- lightweight transactions
    assert!(
        session
            .insert_with(&ticket("lwt", 1), InsertOptions::new().if_not_exists())
            .await
            .unwrap()
    );
    assert!(
        !session
            .insert_with(&ticket("lwt", 1), InsertOptions::new().if_not_exists())
            .await
            .unwrap(),
        "second insert not applied"
    );
    let ok = Ticket::update()
        .tenant("lwt".into())
        .id(1)
        .set_title("new title".into())
        .if_title_eq("ticket 1".into())
        .execute_lwt(&session)
        .await
        .unwrap();
    assert!(ok);
    let stale = Ticket::update()
        .tenant("lwt".into())
        .id(1)
        .set_title("again".into())
        .if_title_eq("ticket 1".into())
        .execute_lwt(&session)
        .await
        .unwrap();
    assert!(!stale, "the condition no longer holds");
    let missing = Ticket::update()
        .tenant("lwt".into())
        .id(99)
        .set_title("x".into())
        .if_exists()
        .execute_lwt(&session)
        .await
        .unwrap();
    assert!(!missing);

    // --- counters
    Hits::update()
        .page("/home".into())
        .increment_views(5)
        .increment_clicks(2)
        .execute(&session)
        .await
        .unwrap();
    Hits::update()
        .page("/home".into())
        .decrement_views(2)
        .execute(&session)
        .await
        .unwrap();
    let h = Hits::query()
        .page("/home".into())
        .first(&session)
        .await
        .unwrap()
        .unwrap();
    assert_eq!((h.views.0, h.clicks.0), (3, 2));

    // --- batch (logged): inserts and a delete, atomically
    let (b1, b2, b3) = (ticket("batch", 1), ticket("batch", 2), ticket("batch", 3));
    session.insert(&ticket("batch", 9)).await.unwrap();
    let doomed = ticket("batch", 9);
    session
        .batch(BatchKind::Logged)
        .insert(&b1)
        .insert(&b2)
        .insert(&b3)
        .delete(&doomed)
        .execute()
        .await
        .unwrap();
    let batch = Ticket::query()
        .tenant("batch".into())
        .fetch(&session)
        .await
        .unwrap();
    assert_eq!(batch.iter().map(|t| t.id).collect::<Vec<_>>(), [3, 2, 1]);

    // --- delete by entity and by builder
    session.delete_entity(&ticket("acme", 1)).await.unwrap();
    Ticket::query()
        .tenant("acme".into())
        .id_eq(2)
        .delete(&session)
        .await
        .unwrap();
    assert_eq!(
        Ticket::query()
            .tenant("acme".into())
            .count(&session)
            .await
            .unwrap(),
        3
    );

    // --- ALLOW FILTERING on a non-key column (explicit opt-in)
    let filtered = Ticket::query()
        .tenant("acme".into())
        .allow_filtering()
        .where_title_eq("renamed".into())
        .fetch(&session)
        .await
        .unwrap();
    assert_eq!(filtered.len(), 1);

    // --- paging and streaming over 1,000 rows
    for chunk in (0..1000).collect::<Vec<_>>().chunks(100) {
        let rows: Vec<Row> = chunk
            .iter()
            .map(|&i| Row {
                bucket: 1,
                seq: i,
                data: format!("row {i}"),
            })
            .collect();
        let mut batch = session.batch(BatchKind::Unlogged);
        for r in &rows {
            batch = batch.insert(r);
        }
        batch.execute().await.unwrap();
    }
    let mut stream = std::pin::pin!(
        Row::query()
            .bucket(1)
            .page_size(100)
            .stream(&session)
            .await
            .unwrap()
    );
    let (mut n, mut last) = (0, -1);
    while let Some(r) = stream.next().await {
        let r = r.unwrap();
        assert!(r.seq > last, "stream keeps clustering order");
        last = r.seq;
        n += 1;
    }
    assert_eq!(n, 1000);
    assert_eq!(
        Row::query()
            .bucket(1)
            .page_size(64)
            .fetch(&session)
            .await
            .unwrap()
            .len(),
        1000,
        "paged fetch collects everything"
    );
    assert_eq!(Row::query().bucket(1).count(&session).await.unwrap(), 1000);

    // --- session-level and per-query consistency
    let one = session.with_consistency(Consistency::One);
    assert_eq!(
        Row::query()
            .bucket(1)
            .seq_eq(5)
            .consistency(Consistency::One)
            .first(&one)
            .await
            .unwrap()
            .unwrap()
            .data,
        "row 5"
    );
    one.insert(&ticket("cl", 1)).await.unwrap();

    // --- schema validation and migration
    assert_eq!(session.validate_schema::<Ticket>().await.unwrap(), vec![]);
    let issues = session.validate_schema::<EvolvingV1>().await.unwrap();
    assert_eq!(issues, vec![SchemaIssue::TableMissing]);
    let ran = session.migrate::<EvolvingV1>().await.unwrap();
    assert!(!ran.is_empty());
    assert_eq!(
        session.validate_schema::<EvolvingV1>().await.unwrap(),
        vec![]
    );
    let ran = session.migrate::<EvolvingV2>().await.unwrap();
    assert_eq!(ran.len(), 1);
    assert!(
        ran[0].starts_with("ALTER TABLE cqlx_it.evolving ADD email"),
        "{ran:?}"
    );
    assert_eq!(
        session.validate_schema::<EvolvingV2>().await.unwrap(),
        vec![]
    );
    assert_eq!(
        session.migrate::<EvolvingV2>().await.unwrap(),
        Vec::<String>::new(),
        "idempotent"
    );
    let issues = session
        .validate_schema::<EvolvingWrongType>()
        .await
        .unwrap();
    assert!(
        issues
            .iter()
            .any(|i| matches!(i, SchemaIssue::TypeMismatch { column, .. } if column == "name")),
        "{issues:?}"
    );
    assert!(
        session.migrate::<EvolvingWrongType>().await.is_err(),
        "type changes are not migratable"
    );
}
