//! Integration tests against a real ScyllaDB in Docker (via testcontainers).
//! Run with `cargo test -p cqlx --test scylla_integration -- --ignored`.

use cqlx::scylla::DeserializeRow;
use cqlx::{CqlEntity, CqlModel, Session};
use std::time::Duration;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

#[derive(CqlEntity, DeserializeRow, Debug, PartialEq, Clone)]
#[cqlx(table_name = "events", keyspace = "cqlx_it")]
struct Event {
    #[cqlx(partition_key)]
    tenant: String,
    #[cqlx(partition_key)]
    day: i32,
    #[cqlx(clustering_key(order = "desc"))]
    ts: i64,
    #[cqlx(rename = "payload")]
    body: String,
    tags: Option<Vec<String>>,
}

async fn start() -> (ContainerAsync<GenericImage>, Session) {
    // Docker occasionally hands out a host port that is already taken ("address already in use"); retry.
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
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(9042).await.unwrap();
    let node = format!("{host}:{port}");
    let mut last = None;
    for _ in 0..60 {
        match Session::connect_single_node(&node).await {
            Ok(s) => return (container, s),
            Err(e) => {
                last = Some(e);
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    }
    panic!("scylla never accepted connections: {last:?}");
}

#[tokio::test]
#[ignore = "requires docker"]
async fn insert_select_and_typestates() {
    let (_c, session) = start().await;
    session.create_table::<Event>().await.unwrap();

    for ts in 1..=5 {
        let e = Event {
            tenant: "acme".into(),
            day: 7,
            ts,
            body: format!("event {ts}"),
            tags: (ts % 2 == 0).then(|| vec!["even".to_string()]),
        };
        session.insert(&e).await.unwrap();
    }
    session
        .insert(&Event {
            tenant: "other".into(),
            day: 7,
            ts: 1,
            body: "x".into(),
            tags: None,
        })
        .await
        .unwrap();

    // Whole partition, descending by clustering order.
    let all = Event::query()
        .tenant("acme".into())
        .day(7)
        .fetch(&session)
        .await
        .unwrap();
    assert_eq!(
        all.iter().map(|e| e.ts).collect::<Vec<_>>(),
        [5, 4, 3, 2, 1]
    );
    assert_eq!(all[1].tags.as_deref(), Some(&["even".to_string()][..]));
    assert_eq!(all[0].body, "event 5"); // renamed column round-trips

    // Range + limit.
    let some = Event::query()
        .tenant("acme".into())
        .day(7)
        .ts_ge(2)
        .ts_lt(5)
        .limit(2)
        .fetch(&session)
        .await
        .unwrap();
    assert_eq!(some.iter().map(|e| e.ts).collect::<Vec<_>>(), [4, 3]);

    // first()
    let one = Event::query()
        .tenant("acme".into())
        .day(7)
        .ts_eq(3)
        .first(&session)
        .await
        .unwrap();
    assert_eq!(one.unwrap().body, "event 3");
    let none = Event::query()
        .tenant("nobody".into())
        .day(7)
        .first(&session)
        .await
        .unwrap();
    assert!(none.is_none());

    // delete
    Event::query()
        .tenant("acme".into())
        .day(7)
        .ts_eq(3)
        .delete(&session)
        .await
        .unwrap();
    let left = Event::query()
        .tenant("acme".into())
        .day(7)
        .fetch(&session)
        .await
        .unwrap();
    assert_eq!(left.len(), 4);

    // The create-table statement is idempotent.
    session.create_table::<Event>().await.unwrap();
}
