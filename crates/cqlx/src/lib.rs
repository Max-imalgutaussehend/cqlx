//! # cqlx
//!
//! Code-first, zero-overhead ORM for ScyllaDB/Cassandra. Part of the **cqlz family**: the
//! code-first counterpart to the schema-first [`cqlc`](https://github.com/Max-imalgutaussehend/cqlc).
//!
//! ```ignore
//! use cqlx::{CqlEntity, CqlModel, Session};
//! use cqlx::scylla::DeserializeRow;
//!
//! #[derive(CqlEntity, DeserializeRow, Debug)]
//! #[cqlx(table_name = "events", keyspace = "app")]
//! struct Event {
//!     #[cqlx(partition_key)] tenant: String,
//!     #[cqlx(clustering_key(order = "desc"))] ts: i64,
//!     body: String,
//! }
//!
//! # async fn demo(session: Session) -> cqlx::Result<()> {
//! let rows = Event::query().tenant("acme".into()).ts_gt(0).limit(10).fetch(&session).await?;
//! # Ok(()) }
//! ```
//!
//! Forgetting a partition key is a *compile error*, because `fetch`/`build`/`delete` only exist on
//! the builder once every partition key has moved from [`Missing`] to [`Provided`].
//!
//! ## What you get
//!
//! * `Entity::query()`: type-state reads (`fetch`, `first`, `count`, `stream`), `IN`, ordering, paging, consistency.
//! * `Entity::update()`: partial updates, collection append/remove, counters, TTL, lightweight transactions.
//! * `Session`: inserts with options, batches, streaming, `validate_schema` and `migrate`.
//!
//! See the [README](https://github.com/Max-imalgutaussehend/cqlx) for the full feature table and the attribute reference.

pub use cqlx_core::*;
pub use cqlx_macros::CqlEntity;
/// Re-export of `futures`, for the `Stream` returned by `stream()`.
pub use futures;
/// Re-export of the driver, so users can `use cqlx::scylla::DeserializeRow`.
pub use scylla;
