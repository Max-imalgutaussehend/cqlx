//! Offline overhead of the generated type-state builder: building a statement + bind values.
//!
//! Baseline: the same statement assembled by hand (`format!` + boxed values), i.e. what you would
//! write without an ORM. No network involved, so this isolates the cost cqlx adds in-process.

use cqlx::scylla::DeserializeRow;
use cqlx::{BoundValues, CqlEntity, CqlModel};
use criterion::{Criterion, criterion_group, criterion_main};
use std::hint::black_box;

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

fn bench(c: &mut Criterion) {
    let mut g = c.benchmark_group("build_statement");

    g.bench_function("cqlx_select_typestate", |b| {
        b.iter(|| {
            let built = Event::query()
                .tenant(black_box("acme".to_string()))
                .day(black_box(7))
                .ts_gt(black_box(100))
                .limit(10)
                .build()
                .unwrap();
            black_box(built)
        })
    });

    g.bench_function("handwritten_select", |b| {
        b.iter(|| {
            let mut values = BoundValues::new();
            values.push(black_box("acme".to_string()));
            values.push(black_box(7i32));
            values.push(black_box(100i64));
            let cql = format!(
                "SELECT tenant, day, ts, body FROM bench.events WHERE tenant = ? AND day = ? AND ts > ? LIMIT {}",
                black_box(10)
            );
            black_box((cql, values))
        })
    });

    let e = Event {
        tenant: "acme".into(),
        day: 7,
        ts: 1,
        body: "hello".into(),
    };
    g.bench_function("cqlx_insert_values", |b| {
        b.iter(|| black_box(black_box(&e).insert_values()))
    });

    g.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
