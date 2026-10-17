//! Runtime half of the generated type-state query builders.

use crate::error::{Error, Result};
use scylla::errors::SerializationError;
use scylla::serialize::row::{RowSerializationContext, SerializeRow};
use scylla::serialize::value::SerializeValue;
use scylla::serialize::writers::RowWriter;
use std::fmt::Write as _;
use std::marker::PhantomData;

/// Type-state marker: a partition key column has not been provided yet.
pub struct Missing;
/// Type-state marker: a partition key column has been provided.
pub struct Provided;

/// A boxed, thread-safe bind value.
pub type BoxedValue<'a> = Box<dyn SerializeValue + Send + Sync + 'a>;

/// Positional bind values for a statement.
#[derive(Default)]
pub struct BoundValues<'a>(Vec<BoxedValue<'a>>);

impl<'a> BoundValues<'a> {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn push<T: SerializeValue + Send + Sync + 'a>(&mut self, v: T) {
        self.0.push(Box::new(v));
    }
    pub fn len(&self) -> usize {
        self.0.len()
    }
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl SerializeRow for BoundValues<'_> {
    fn serialize(
        &self,
        ctx: &RowSerializationContext<'_>,
        writer: &mut RowWriter,
    ) -> Result<(), SerializationError> {
        let cols = ctx.columns();
        if cols.len() != self.0.len() {
            return Err(SerializationError::new(Error::InvalidQuery(format!(
                "statement expects {} bind values, got {}",
                cols.len(),
                self.0.len()
            ))));
        }
        for (v, col) in self.0.iter().zip(cols) {
            let cell = writer.make_cell_writer();
            v.serialize(col.typ(), cell)?;
        }
        Ok(())
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// Comparison operator of a clustering restriction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Op {
    Eq,
    Gt,
    Ge,
    Lt,
    Le,
}

impl Op {
    fn as_cql(self) -> &'static str {
        match self {
            Op::Eq => "=",
            Op::Gt => ">",
            Op::Ge => ">=",
            Op::Lt => "<",
            Op::Le => "<=",
        }
    }
}

struct Restriction {
    col: &'static str,
    op: Op,
    value: BoxedValue<'static>,
}

/// Compile-time constants of an entity, passed to [`QueryState`] by generated code.
#[derive(Clone, Copy)]
pub struct Shape {
    pub table: &'static str,
    pub select_list: &'static str,
    pub partition: &'static [&'static str],
    pub clustering: &'static [&'static str],
}

/// A finished statement: CQL text plus bind values.
pub struct Built {
    pub cql: String,
    pub values: BoundValues<'static>,
}

/// Untyped accumulator behind every generated builder.
pub struct QueryState {
    shape: Shape,
    restrictions: Vec<Restriction>,
    limit: Option<i32>,
}

impl QueryState {
    pub fn new(shape: Shape) -> Self {
        Self {
            shape,
            restrictions: Vec::new(),
            limit: None,
        }
    }

    pub fn restrict<T: SerializeValue + Send + Sync + 'static>(
        mut self,
        col: &'static str,
        op: Op,
        v: T,
    ) -> Self {
        self.restrictions.push(Restriction {
            col,
            op,
            value: Box::new(v),
        });
        self
    }

    pub fn limit(mut self, n: i32) -> Self {
        self.limit = Some(n);
        self
    }

    fn where_clause(self) -> Result<(String, Option<i32>, BoundValues<'static>, Shape)> {
        let Self {
            shape,
            mut restrictions,
            limit,
        } = self;
        let rank = |c: &str| -> usize {
            shape
                .partition
                .iter()
                .position(|p| *p == c)
                .or_else(|| {
                    shape
                        .clustering
                        .iter()
                        .position(|p| *p == c)
                        .map(|i| i + shape.partition.len())
                })
                .unwrap_or(usize::MAX)
        };
        if !restrictions.is_sorted_by_key(|r| rank(r.col)) {
            restrictions.sort_by_key(|r| rank(r.col));
        }

        for p in shape.partition {
            let n = restrictions.iter().filter(|r| r.col == *p).count();
            if n != 1 || restrictions.iter().any(|r| r.col == *p && r.op != Op::Eq) {
                return Err(Error::InvalidQuery(format!(
                    "partition key `{p}` needs exactly one `=` restriction"
                )));
            }
        }
        // Clustering restrictions: a prefix of the clustering columns, `=` on all but the last,
        // which may carry up to two range bounds.
        let mut expected = 0usize;
        let mut ranged = false;
        for r in restrictions
            .iter()
            .filter(|r| rank(r.col) >= shape.partition.len())
        {
            let idx = rank(r.col) - shape.partition.len();
            let second_bound = ranged && idx + 1 == expected && r.op != Op::Eq;
            if second_bound {
                continue;
            }
            if idx != expected || ranged {
                return Err(Error::InvalidQuery(format!(
                    "clustering restriction on `{}` skips an earlier clustering column, repeats one, or follows a range",
                    r.col
                )));
            }
            ranged = r.op != Op::Eq;
            expected += 1;
        }
        let mut sql = String::with_capacity(32 * restrictions.len());
        let mut values = BoundValues(Vec::with_capacity(restrictions.len()));
        for (i, r) in restrictions.into_iter().enumerate() {
            let _ = write!(
                sql,
                "{}{} {} ?",
                if i == 0 { "" } else { " AND " },
                r.col,
                r.op.as_cql()
            );
            values.0.push(r.value);
        }
        Ok((sql, limit, values, shape))
    }

    /// Builds `SELECT ... WHERE ... [LIMIT n]`.
    pub fn build_select(self) -> Result<Built> {
        let (wh, limit, values, shape) = self.where_clause()?;
        let mut cql = format!(
            "SELECT {} FROM {} WHERE {}",
            shape.select_list, shape.table, wh
        );
        if let Some(n) = limit {
            let _ = write!(cql, " LIMIT {n}");
        }
        Ok(Built { cql, values })
    }

    /// Builds `DELETE FROM ... WHERE ...`.
    pub fn build_delete(self) -> Result<Built> {
        let (wh, _, values, shape) = self.where_clause()?;
        Ok(Built {
            cql: format!("DELETE FROM {} WHERE {}", shape.table, wh),
            values,
        })
    }
}

/// Zero-sized helper to carry the type-state parameters in generated builders.
pub struct States<T>(pub PhantomData<T>);

impl<T> Default for States<T> {
    fn default() -> Self {
        States(PhantomData)
    }
}
