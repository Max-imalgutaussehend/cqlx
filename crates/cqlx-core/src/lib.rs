//! Core of cqlx: entity metadata, the [`CqlModel`] trait, type-state query plumbing and a
//! thin session wrapper over the ScyllaDB driver.
//!
//! Most users depend on the `cqlx` facade crate instead of this one.

pub mod error;
pub mod meta;
#[cfg(feature = "parse")]
pub mod parse;
pub mod query;
pub mod session;

pub use error::{Error, Result};
pub use meta::{ColumnMeta, EntityMeta, KeyRole, Order};
pub use query::{BoundValues, Built, Missing, Op, Provided, QueryState, Shape};
pub use session::Session;

use scylla::deserialize::row::DeserializeRow;

/// Implemented by `#[derive(CqlEntity)]`.
///
/// `query()` returns the generated type-state builder, which starts in a state where every
/// partition key column is still [`Missing`].
pub trait CqlModel: Sized + 'static + for<'f, 'm> DeserializeRow<'f, 'm> {
    /// The generated entry builder (all partition keys `Missing`).
    type Query;

    /// Qualified table name (`keyspace.table` or `table`).
    const TABLE: &'static str;
    /// `INSERT INTO ... VALUES (?, ...)` text.
    const INSERT_CQL: &'static str;
    /// `CREATE TABLE IF NOT EXISTS ...` text.
    const CREATE_TABLE_CQL: &'static str;
    /// `CREATE KEYSPACE IF NOT EXISTS ...` text, if the entity has a keyspace.
    const CREATE_KEYSPACE_CQL: Option<&'static str>;

    /// Starts a type-state query.
    fn query() -> Self::Query;

    /// Bind values for [`Self::INSERT_CQL`], in column order.
    fn insert_values(&self) -> BoundValues<'_>;

    /// Static metadata for this entity.
    fn meta() -> &'static EntityMeta;
}
