# cqlx

**Typesafe, zero-overhead, code-first ORM for ScyllaDB and Cassandra.**

> Part of the **cqlz family**
>
> | Project | Direction | What it does |
> |---|---|---|
> | [`cqlc`](https://github.com/Max-imalgutaussehend/cqlc) | schema-first | Static CQL → typed Rust code generator (like sqlc) |
> | **`cqlx`** | **code-first** | Rust structs → schema + type-state query builders (this repo) |
> | [`cql-ai`](https://github.com/Max-imalgutaussehend/cql-ai) | analysis | Tiny neural net that flags CQL anti-patterns in CI |

You write a struct. `#[derive(CqlEntity)]` generates the table definition, the insert statement
and a **type-state query builder** that makes "forgot the partition key" a compile error instead
of a production full-cluster scan.

```rust
use cqlx::scylla::DeserializeRow;
use cqlx::{CqlEntity, CqlModel, Session};

#[derive(CqlEntity, DeserializeRow, Debug)]
#[cqlx(table_name = "events", keyspace = "app")]
struct Event {
    #[cqlx(partition_key)] tenant: String,
    #[cqlx(partition_key)] day: i32,
    #[cqlx(clustering_key(order = "desc"))] ts: i64,
    #[cqlx(rename = "payload")] body: String,
}

#[tokio::main]
async fn main() -> cqlx::Result<()> {
    let session = Session::connect("127.0.0.1:9042").await?;
    session.create_table::<Event>().await?;
    session.insert(&Event { tenant: "acme".into(), day: 7, ts: 1, body: "hi".into() }).await?;

    let rows = Event::query()
        .tenant("acme".into())   // EventQuery<Missing, Missing> -> <Provided, Missing>
        .day(7)                  //                              -> <Provided, Provided>
        .ts_gt(0)                // clustering restrictions: _eq _gt _ge _lt _le
        .limit(10)
        .fetch(&session)         // only exists once every partition key is Provided
        .await?;
    println!("{rows:?}");
    Ok(())
}
```

```rust,compile_fail
Event::query().tenant("acme".into()).fetch(&session); // error[E0599]: no method `fetch` on `EventQuery<Provided, Missing>`
```

## Why it is fast

Everything static is computed **at compile time** by the macro: the `SELECT` column list, the
`INSERT` text and the `CREATE TABLE` text are `&'static str` constants. At runtime a query only
assembles `WHERE` restrictions and bind values; statements are prepared once and cached per
session. There is no reflection and no intermediate row type.

## Attributes

| Where | Attribute | Meaning |
|---|---|---|
| struct | `table_name = "..."`, `keyspace = "..."` | Defaults: snake_case struct name, no keyspace |
| field | `partition_key` / `primary_key` | Partition key column (several → composite, declaration order). `primary_key` is an alias for a simple key |
| field | `clustering_key(order = "asc"\|"desc")` | Clustering column (declaration order) |
| field | `rename = "other_name"` | CQL column name (selected `AS field`, so `DeserializeRow` needs no extra attribute) |
| field | `cql_type = "frozen<my_udt>"` | Override the inferred CQL type |

`Option<T>` maps like `T`. Built-in mappings: `String`→`text`, `i8/i16/i32/i64`, `f32/f64`, `bool`,
`Uuid`→`uuid`, `Vec<u8>`→`blob`, `Vec<T>`→`list<T>`, `HashSet<T>`→`set<T>`, `HashMap<K,V>`→`map<K,V>`,
dates/times/inet/duration/decimal/varint by type name. Anything else needs `cql_type`.

Clustering restrictions are validated when the statement is built (`=` on a prefix of the clustering
columns, optional range on the next one); violations return `Error::InvalidQuery` before touching the network.

## Schema tooling (`cqlx-cli`)

```sh
cargo install cqlx-cli          # installs the `cqlx` binary

cqlx schema src/ --keyspaces            # CREATE KEYSPACE/TABLE for every #[derive(CqlEntity)]
cqlx schema src/ -o schema.cql
cqlx diff src/ --schema schema.cql      # ALTER TABLE ... ADD + warnings for what CQL cannot alter
```

The existing schema is read with `cqlc-core`'s CQL parser, so the same `.cql` files work with `cqlc`.

## Workspace

| Crate | Role |
|---|---|
| `cqlx` | Facade: re-exports everything plus the driver (`cqlx::scylla`) |
| `cqlx-core` | `CqlModel` trait, metadata, query plumbing, session wrapper |
| `cqlx-macros` | `#[derive(CqlEntity)]` |
| `cqlx-cli` | `cqlx` binary: schema extraction and diffing |

## Testing

```sh
cargo test --workspace                                              # unit + trybuild compile-fail tests
cargo test -p cqlx --test scylla_integration -- --ignored          # needs Docker (scylladb/scylla via testcontainers)
```

## License

Apache-2.0
