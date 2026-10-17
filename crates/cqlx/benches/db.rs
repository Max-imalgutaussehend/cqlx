//! cqlx vs the raw ScyllaDB driver vs charybdis against a real ScyllaDB (Docker, testcontainers).
//!
//!     cargo bench -p cqlx --bench db
//!
//! All three read/write the same table with the same prepared-statement strategy, so differences
//! are the abstraction cost (statement assembly, decoding path), not the network.

use charybdis::macros::charybdis_model;
use charybdis::operations::{Find, Insert};
use charybdis::types::{BigInt, Int, Text};
use cqlx::scylla::DeserializeRow;
use cqlx::scylla::client::caching_session::CachingSession;
use cqlx::{CqlEntity, CqlModel, Session};
use criterion::{Criterion, criterion_group, criterion_main};
use std::hint::black_box;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

#[derive(CqlEntity, DeserializeRow, Debug, Clone)]
#[cqlx(table_name = "events", keyspace = "bench")]
struct Event {
    #[cqlx(partition_key)]
    tenant: String,
    #[cqlx(partition_key)]
    day: i32,
    #[cqlx(clustering_key(order = "desc"))]
    ts: i64,
    body: String,
}

#[derive(DeserializeRow)]
#[allow(dead_code)] // fields exist so decoding does the same work as the other contenders
struct RawRow {
    tenant: String,
    day: i32,
    ts: i64,
    body: String,
}

#[charybdis_model(
    table_name = events,
    partition_keys = [tenant, day],
    clustering_keys = [ts],
    global_secondary_indexes = [],
    local_secondary_indexes = [],
    static_columns = []
)]
#[derive(Default)]
struct CEvent {
    tenant: Text,
    day: Int,
    ts: BigInt,
    body: Text,
}

async fn start() -> (ContainerAsync<GenericImage>, Session, String) {
    let container = GenericImage::new("scylladb/scylla", "latest")
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
        .await
        .unwrap();
    let node = format!(
        "{}:{}",
        container.get_host().await.unwrap(),
        container.get_host_port_ipv4(9042).await.unwrap()
    );
    let mut last = None;
    for _ in 0..60 {
        match Session::connect_single_node(&node).await {
            Ok(s) => return (container, s, node),
            Err(e) => {
                last = Some(e);
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    }
    panic!("scylla did not come up: {last:?}");
}

fn bench(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (_container, session, node) = rt.block_on(start());
    rt.block_on(async {
        session.create_table::<Event>().await.unwrap();
        for ts in 0..10 {
            session
                .insert(&Event {
                    tenant: "acme".into(),
                    day: 1,
                    ts,
                    body: "x".repeat(64),
                })
                .await
                .unwrap();
        }
        session.driver().use_keyspace("bench", false).await.unwrap();
    });
    // charybdis wants an owned driver session wrapped in a CachingSession (prepared-statement cache).
    let charybdis_session = rt.block_on(async {
        let inner = Session::connect_single_node(&node)
            .await
            .unwrap()
            .try_into_driver()
            .ok()
            .unwrap();
        inner.use_keyspace("bench", false).await.unwrap();
        CachingSession::from(inner, 100)
    });
    let raw_select =
        rt.block_on(session.driver().prepare(
            "SELECT tenant, day, ts, body FROM bench.events WHERE tenant = ? AND day = ?",
        ))
        .unwrap();
    let raw_insert = rt
        .block_on(
            session
                .driver()
                .prepare("INSERT INTO bench.events (tenant, day, ts, body) VALUES (?, ?, ?, ?)"),
        )
        .unwrap();

    let mut g = c.benchmark_group("select_partition_10_rows");
    g.sample_size(50).measurement_time(Duration::from_secs(10));
    g.bench_function("cqlx", |b| {
        b.to_async(&rt).iter(|| async {
            let rows = Event::query()
                .tenant("acme".into())
                .day(1)
                .fetch(&session)
                .await
                .unwrap();
            black_box(rows)
        })
    });
    g.bench_function("raw_scylla_driver", |b| {
        b.to_async(&rt).iter(|| async {
            let res = session
                .driver()
                .execute_unpaged(&raw_select, ("acme", 1i32))
                .await
                .unwrap();
            let rows: Vec<RawRow> = res
                .into_rows_result()
                .unwrap()
                .rows::<RawRow>()
                .unwrap()
                .map(|r| r.unwrap())
                .collect();
            black_box(rows)
        })
    });
    g.bench_function("charybdis", |b| {
        b.to_async(&rt).iter(|| async {
            let rows = CEvent::find_by_partition_key_value((String::from("acme"), 1i32))
                .execute(&charybdis_session)
                .await
                .unwrap()
                .try_collect()
                .await
                .unwrap();
            black_box(rows)
        })
    });
    g.finish();

    let counter = AtomicI64::new(1_000);
    let mut g = c.benchmark_group("insert_row");
    g.sample_size(50).measurement_time(Duration::from_secs(10));
    g.bench_function("cqlx", |b| {
        b.to_async(&rt).iter(|| async {
            let ts = counter.fetch_add(1, Ordering::Relaxed);
            session
                .insert(&Event {
                    tenant: "w1".into(),
                    day: 1,
                    ts,
                    body: "y".repeat(64),
                })
                .await
                .unwrap();
        })
    });
    g.bench_function("raw_scylla_driver", |b| {
        b.to_async(&rt).iter(|| async {
            let ts = counter.fetch_add(1, Ordering::Relaxed);
            session
                .driver()
                .execute_unpaged(&raw_insert, ("w2", 1i32, ts, "y".repeat(64)))
                .await
                .unwrap();
        })
    });
    g.bench_function("charybdis", |b| {
        b.to_async(&rt).iter(|| async {
            let ts = counter.fetch_add(1, Ordering::Relaxed);
            let e = CEvent {
                tenant: "w3".into(),
                day: 1,
                ts,
                body: "y".repeat(64),
            };
            e.insert().execute(&charybdis_session).await.unwrap();
        })
    });
    g.finish();
    // Drop the container inside the runtime (testcontainers cleans up asynchronously).
    rt.block_on(async move { drop(_container) });
}

criterion_group!(benches, bench);
criterion_main!(benches);
