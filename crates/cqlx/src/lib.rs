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
//! ## Attributes
//!
//! | Where  | Attribute | Meaning |
//! |--------|-----------|---------|
//! | struct | `table_name = "..."`, `keyspace = "..."` | Table (default: snake_case struct name) and keyspace |
//! | field  | `partition_key` / `primary_key` | Part of the partition key (declaration order) |
//! | field  | `clustering_key(order = "asc"\|"desc")` | Clustering column (declaration order) |
//! | field  | `rename = "..."` | CQL column name |
//! | field  | `cql_type = "..."` | Override the inferred CQL type |

pub use cqlx_core::*;
pub use cqlx_macros::CqlEntity;
/// Re-export of the driver, so users can `use cqlx::scylla::DeserializeRow`.
pub use scylla;
