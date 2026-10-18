# cqlx

**Typesafe, zero-overhead, code-first ORM for ScyllaDB and Apache Cassandra.**

> Part of the **cqlz family**
>
> | Project | Direction | What it does |
> |---|---|---|
> | [`cqlc`](https://github.com/Max-imalgutaussehend/cqlc) | schema-first | CQL → typed Rust code, checks Cassandra 3.x–5.0 CQL offline |
> | **`cqlx`** | **code-first** | Rust structs → schema, writes and type-state queries (this repo) |
> | [`cql-ai`](https://github.com/Max-imalgutaussehend/cql-ai) | performance | Neural bottleneck detector for CQL in CI |

You write a struct. `#[derive(CqlEntity)]` generates the table, the statements and **type-state builders** that
make mistakes compile errors instead of production incidents: a read without its partition key does not
compile, neither does an `UPDATE` without its full primary key or without any assignment, nor an `INSERT`
into a counter table, nor a filter on a non-key column without an explicit `allow_filtering()`.

```rust
use cqlx::scylla::DeserializeRow;
use cqlx::{CqlEntity, CqlModel, Session};

#[derive(CqlEntity, DeserializeRow, Debug)]
#[cqlx(table_name = "tickets", keyspace = "support")]
struct Ticket {
    #[cqlx(partition_key)] tenant: String,
    #[cqlx(clustering_key(order = "desc"))] id: i32,
    title: String,
    #[cqlx(rename = "points")] score: Option<i32>,
    labels: std::collections::HashSet<String>,
}

#[tokio::main]
async fn main() -> cqlx::Result<()> {
    let session = Session::connect("127.0.0.1:9042").await?;
    session.migrate::<Ticket>().await?;                       // create the table or add missing columns

    session.insert(&Ticket { tenant: "acme".into(), id: 1, title: "login broken".into(), score: Some(3), labels: ["bug".into()].into() }).await?;

    // type-state read: `fetch` exists only once every partition key is provided
    let open = Ticket::query().tenant("acme".into()).id_ge(1).limit(20).fetch(&session).await?;

    // partial update: only the named columns change, collections are appended without being rewritten
    Ticket::update().tenant("acme".into()).id(1).set_title("login fixed".into()).add_labels(["done".into()].into()).execute(&session).await?;
    Ok(())
}
```

These do not compile:

```text
Ticket::query().fetch(&session);                                          // no `fetch`: the partition key `tenant` is Missing
Ticket::update().tenant("acme".into()).set_title("x".into()).build();     // no `build`: the clustering key `id` is Missing
Ticket::update().tenant("acme".into()).id(1).build();                     // no `build`: no assignment
Ticket::query().tenant("acme".into()).where_title_eq("x".into());         // no `where_title_eq`: needs `allow_filtering()`
```

## Features

| Area | API |
|---|---|
| Reads | `Entity::query()` type-state builder: partition keys (`=` / `_in`), clustering `_eq/_gt/_ge/_lt/_le/_in`, `limit`, `reverse`, `fetch`, `first`, `count`, `stream`, `consistency`, `page_size` |
| Non-key filters | `allow_filtering()` unlocks `where_<column>_*`; without it the methods do not exist |
| Writes | `session.insert`, `insert_with(InsertOptions)` (TTL, timestamp, `IF NOT EXISTS`), `delete_entity`, builder `delete` |
| Partial updates | `Entity::update()`: needs the full primary key and one assignment; `set_*`, `set_*_null`, `add_*`/`remove_*` for collections, `increment_*`/`decrement_*` for counters, `ttl`, `timestamp`, `if_exists`, `if_<col>_eq` |
| Lightweight transactions | `insert_with(.. if_not_exists)`, `update().if_*().execute_lwt()` return whether the write was applied |
| Counters | counter tables only expose `update()`; `insert` does not compile |
| Batches | `session.batch(BatchKind::Logged\|Unlogged\|Counter).insert(&a).delete(&b).execute()` |
| Paging | `fetch` pages large results; `stream()` yields rows without holding them in memory |
| Consistency | per session (`with_consistency`), per statement (`.consistency(..)`), serial consistency for LWTs |
| Schema | `validate_schema::<T>()` reports drift; `migrate::<T>()` creates tables and adds missing columns, never drops or retypes |
| Tooling | `cqlx schema` prints `CREATE TABLE` for all entities, `cqlx diff` compares them with a `.cql` file |

Everything static is rendered **at compile time**: the `SELECT` list, the `INSERT` and `DELETE` text and the
`CREATE TABLE` text are `&'static str` constants. At runtime a statement only assembles restrictions and bind
values; statements are prepared once per session.

## Compatibility

CI runs the integration suite (inserts, partial updates, collections, counters, TTL, LWTs, batches, streaming
1,000 rows, schema migration, consistency) against **ScyllaDB (latest)**, **Apache Cassandra 5.0** and **4.1**.
Built on the ScyllaDB Rust driver 1.x. MSRV 1.88.

## Attributes

| Where | Attribute | Meaning |
|---|---|---|
| struct | `table_name`, `keyspace` | Defaults: snake_case struct name, no keyspace |
| field | `partition_key` / `primary_key` | Partition key column; several make a composite key |
| field | `clustering_key(order = "asc"\|"desc")` | Clustering column, in declaration order |
| field | `rename` | CQL column name (selected `AS field`, so `DeserializeRow` needs no extra attribute) |
| field | `cql_type` | Override the inferred CQL type, e.g. for user-defined types |

`Option<T>` maps like `T`. Built in: `String`, `i8`–`i64`, `f32`/`f64`, `bool`, `Uuid`, `Vec<u8>`→`blob`, `Vec<T>`→`list<T>`,
`HashSet<T>`→`set<T>`, `HashMap<K,V>`→`map<K,V>`, `Counter`, and date/time/inet/duration/decimal/varint by type name.

## Stability and limits

1.0 means semantic versioning for the public API. Not in the box yet: materialized-view and secondary-index queries
(use the driver through `session.driver()`), user-defined-type derive helpers (provide `SerializeValue` and set
`cql_type`), and multi-entity joins (CQL has none).

## Workspace

| Crate | Role |
|---|---|
| `cqlx` | Facade: re-exports everything plus the driver (`cqlx::scylla`) |
| `cqlx-core` | `CqlModel` trait, metadata, query/update plumbing, session |
| `cqlx-macros` | `#[derive(CqlEntity)]` |
| `cqlx-cli` | `cqlx` binary: schema extraction and diffing |

## License

Apache-2.0
