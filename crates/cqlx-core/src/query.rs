//! Runtime half of the generated type-state builders: bind values, `SELECT`/`DELETE` and `UPDATE` assembly.

use crate::error::{Error, Result};
use scylla::errors::SerializationError;
use scylla::serialize::row::{RowSerializationContext, SerializeRow};
use scylla::serialize::value::SerializeValue;
use scylla::serialize::writers::RowWriter;
use scylla::statement::{Consistency, SerialConsistency};
use std::fmt::Write as _;

/// Type-state marker: a key column has not been provided yet.
pub struct Missing;
/// Type-state marker: a key column has been provided.
pub struct Provided;
/// Type-state marker: the query may not filter on non-key columns.
pub struct NoFilter;
/// Type-state marker: `allow_filtering()` was called, non-key `where_*` methods exist.
pub struct Filtering;
/// Type-state marker of an `UPDATE` builder without assignments yet.
pub struct NoAssignment;
/// Type-state marker of an `UPDATE` builder with at least one assignment.
pub struct HasAssignment;

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
    pub fn push_boxed(&mut self, v: BoxedValue<'a>) {
        self.0.push(v);
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

/// Comparison operator of a restriction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Op {
    Eq,
    Gt,
    Ge,
    Lt,
    Le,
    /// `IN ?`, the bound value is a list.
    In,
}

impl Op {
    fn as_cql(self) -> &'static str {
        match self {
            Op::Eq => "=",
            Op::Gt => ">",
            Op::Ge => ">=",
            Op::Lt => "<",
            Op::Le => "<=",
            Op::In => "IN",
        }
    }
    fn is_eq_like(self) -> bool {
        matches!(self, Op::Eq | Op::In)
    }
}

struct Restriction {
    col: &'static str,
    op: Op,
    value: BoxedValue<'static>,
}

/// Compile-time constants of an entity, passed to the builders by generated code.
#[derive(Clone, Copy)]
pub struct Shape {
    pub table: &'static str,
    pub select_list: &'static str,
    pub partition: &'static [&'static str],
    pub clustering: &'static [&'static str],
    /// `true` for `DESC` clustering columns, parallel to `clustering`.
    pub clustering_desc: &'static [bool],
}

/// A finished statement: CQL text, bind values and per-statement execution options.
pub struct Built {
    pub cql: String,
    pub values: BoundValues<'static>,
    pub consistency: Option<Consistency>,
    pub serial_consistency: Option<SerialConsistency>,
    pub page_size: Option<i32>,
    /// The `LIMIT` of a `SELECT`, if any.
    pub limit: Option<i32>,
    /// A lightweight transaction (`IF ...`): the result row starts with `[applied]`.
    pub lwt: bool,
}

/// Untyped accumulator behind every generated query builder.
pub struct QueryState {
    shape: Shape,
    restrictions: Vec<Restriction>,
    filters: Vec<Restriction>,
    limit: Option<i32>,
    reverse: bool,
    allow_filtering: bool,
    consistency: Option<Consistency>,
    page_size: Option<i32>,
}

impl QueryState {
    pub fn new(shape: Shape) -> Self {
        Self {
            shape,
            restrictions: Vec::with_capacity(shape.partition.len() + shape.clustering.len().min(2)),
            filters: Vec::new(),
            limit: None,
            reverse: false,
            allow_filtering: false,
            consistency: None,
            page_size: None,
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

    /// A restriction on a non-key column; only reachable after `allow_filtering()`.
    pub fn filter<T: SerializeValue + Send + Sync + 'static>(
        mut self,
        col: &'static str,
        op: Op,
        v: T,
    ) -> Self {
        self.filters.push(Restriction {
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

    /// Reads clustering columns in the opposite of their declared order.
    pub fn reverse(mut self) -> Self {
        self.reverse = !self.reverse;
        self
    }

    pub fn allow_filtering(mut self) -> Self {
        self.allow_filtering = true;
        self
    }

    pub fn consistency(mut self, c: Consistency) -> Self {
        self.consistency = Some(c);
        self
    }

    pub fn page_size(mut self, n: i32) -> Self {
        self.page_size = Some(n);
        self
    }

    fn where_clause(&mut self) -> Result<(String, BoundValues<'static>)> {
        let shape = self.shape;
        let mut restrictions = std::mem::take(&mut self.restrictions);
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
            if n != 1
                || restrictions
                    .iter()
                    .any(|r| r.col == *p && !r.op.is_eq_like())
            {
                return Err(Error::InvalidQuery(format!(
                    "partition key `{p}` needs exactly one `=` or `IN` restriction"
                )));
            }
        }
        // Clustering restrictions: a prefix of the clustering columns with `=`/`IN`, optionally ending in a range
        // (up to two bounds on the last restricted column).
        let mut expected = 0usize;
        let mut ranged = false;
        for r in restrictions
            .iter()
            .filter(|r| rank(r.col) >= shape.partition.len())
        {
            let idx = rank(r.col) - shape.partition.len();
            if ranged && idx + 1 == expected && !r.op.is_eq_like() {
                continue; // second bound of the same range column
            }
            if idx != expected || ranged {
                return Err(Error::InvalidQuery(format!(
                    "clustering restriction on `{}` skips an earlier clustering column, repeats one, or follows a range",
                    r.col
                )));
            }
            ranged = !r.op.is_eq_like();
            expected += 1;
        }
        let mut sql = String::with_capacity(32 * (restrictions.len() + self.filters.len()));
        let mut values = BoundValues(Vec::with_capacity(restrictions.len() + self.filters.len()));
        let filters = std::mem::take(&mut self.filters);
        for (i, r) in restrictions.into_iter().chain(filters).enumerate() {
            let _ = write!(
                sql,
                "{}{} {} ?",
                if i == 0 { "" } else { " AND " },
                r.col,
                r.op.as_cql()
            );
            values.0.push(r.value);
        }
        Ok((sql, values))
    }

    fn finish(self, cql: String, values: BoundValues<'static>) -> Built {
        Built {
            cql,
            values,
            consistency: self.consistency,
            serial_consistency: None,
            page_size: self.page_size,
            limit: self.limit,
            lwt: false,
        }
    }

    fn order_by(&self) -> String {
        if !self.reverse || self.shape.clustering.is_empty() {
            return String::new();
        }
        let cols: Vec<String> = self
            .shape
            .clustering
            .iter()
            .zip(self.shape.clustering_desc)
            .map(|(c, desc)| format!("{c} {}", if *desc { "ASC" } else { "DESC" }))
            .collect();
        format!(" ORDER BY {}", cols.join(", "))
    }

    /// Builds `SELECT ... WHERE ... [ORDER BY ...] [LIMIT n] [ALLOW FILTERING]`.
    pub fn build_select(mut self) -> Result<Built> {
        let (wh, values) = self.where_clause()?;
        let shape = self.shape;
        let mut cql =
            String::with_capacity(64 + shape.select_list.len() + shape.table.len() + wh.len());
        cql.push_str("SELECT ");
        cql.push_str(shape.select_list);
        cql.push_str(" FROM ");
        cql.push_str(shape.table);
        cql.push_str(" WHERE ");
        cql.push_str(&wh);
        cql.push_str(&self.order_by());
        if let Some(n) = self.limit {
            let _ = write!(cql, " LIMIT {n}");
        }
        if self.allow_filtering {
            cql.push_str(" ALLOW FILTERING");
        }
        Ok(self.finish(cql, values))
    }

    /// Builds `SELECT COUNT(*) FROM ... WHERE ...`.
    pub fn build_count(mut self) -> Result<Built> {
        let (wh, values) = self.where_clause()?;
        let mut cql = format!("SELECT COUNT(*) FROM {} WHERE {}", self.shape.table, wh);
        if self.allow_filtering {
            cql.push_str(" ALLOW FILTERING");
        }
        Ok(self.finish(cql, values))
    }

    /// Builds `DELETE FROM ... WHERE ...`.
    pub fn build_delete(mut self) -> Result<Built> {
        let (wh, values) = self.where_clause()?;
        let cql = format!("DELETE FROM {} WHERE {}", self.shape.table, wh);
        Ok(self.finish(cql, values))
    }
}

/// How an `UPDATE` assignment changes a column.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SetOp {
    /// `col = ?`
    Set,
    /// `col = col + ?` (counter increment, collection append)
    Add,
    /// `col = col - ?` (counter decrement, collection removal)
    Remove,
}

struct Assignment {
    col: &'static str,
    op: SetOp,
    value: BoxedValue<'static>,
}

/// Untyped accumulator behind every generated `UPDATE` builder.
pub struct UpdateState {
    shape: Shape,
    keys: Vec<(&'static str, BoxedValue<'static>)>,
    sets: Vec<Assignment>,
    conditions: Vec<(&'static str, BoxedValue<'static>)>,
    if_exists: bool,
    ttl: Option<i32>,
    timestamp: Option<i64>,
    consistency: Option<Consistency>,
    serial_consistency: Option<SerialConsistency>,
}

impl UpdateState {
    pub fn new(shape: Shape) -> Self {
        Self {
            shape,
            keys: Vec::new(),
            sets: Vec::new(),
            conditions: Vec::new(),
            if_exists: false,
            ttl: None,
            timestamp: None,
            consistency: None,
            serial_consistency: None,
        }
    }

    pub fn key<T: SerializeValue + Send + Sync + 'static>(
        mut self,
        col: &'static str,
        v: T,
    ) -> Self {
        self.keys.push((col, Box::new(v)));
        self
    }

    pub fn set<T: SerializeValue + Send + Sync + 'static>(
        mut self,
        col: &'static str,
        op: SetOp,
        v: T,
    ) -> Self {
        self.sets.push(Assignment {
            col,
            op,
            value: Box::new(v),
        });
        self
    }

    pub fn condition<T: SerializeValue + Send + Sync + 'static>(
        mut self,
        col: &'static str,
        v: T,
    ) -> Self {
        self.conditions.push((col, Box::new(v)));
        self
    }

    pub fn if_exists(mut self) -> Self {
        self.if_exists = true;
        self
    }

    pub fn ttl(mut self, seconds: i32) -> Self {
        self.ttl = Some(seconds);
        self
    }

    pub fn timestamp(mut self, micros: i64) -> Self {
        self.timestamp = Some(micros);
        self
    }

    pub fn consistency(mut self, c: Consistency) -> Self {
        self.consistency = Some(c);
        self
    }

    pub fn serial_consistency(mut self, c: SerialConsistency) -> Self {
        self.serial_consistency = Some(c);
        self
    }

    /// Builds `UPDATE t [USING TTL n AND TIMESTAMP m] SET ... WHERE <full primary key> [IF ...]`.
    pub fn build(mut self) -> Result<Built> {
        if self.sets.is_empty() {
            return Err(Error::InvalidQuery(
                "an UPDATE needs at least one assignment".into(),
            ));
        }
        let shape = self.shape;
        let key_order: Vec<&str> = shape
            .partition
            .iter()
            .chain(shape.clustering)
            .copied()
            .collect();
        for k in &key_order {
            if self.keys.iter().filter(|(c, _)| c == k).count() != 1 {
                return Err(Error::InvalidQuery(format!(
                    "UPDATE needs exactly one value for key column `{k}`"
                )));
            }
        }
        self.keys
            .sort_by_key(|(c, _)| key_order.iter().position(|k| k == c).unwrap_or(usize::MAX));
        let mut cql = format!("UPDATE {}", shape.table);
        match (self.ttl, self.timestamp) {
            (Some(t), Some(ts)) => {
                let _ = write!(cql, " USING TTL {t} AND TIMESTAMP {ts}");
            }
            (Some(t), None) => {
                let _ = write!(cql, " USING TTL {t}");
            }
            (None, Some(ts)) => {
                let _ = write!(cql, " USING TIMESTAMP {ts}");
            }
            (None, None) => {}
        }
        let mut values = BoundValues::new();
        cql.push_str(" SET ");
        for (i, a) in self.sets.into_iter().enumerate() {
            if i > 0 {
                cql.push_str(", ");
            }
            match a.op {
                SetOp::Set => {
                    let _ = write!(cql, "{} = ?", a.col);
                }
                SetOp::Add => {
                    let _ = write!(cql, "{0} = {0} + ?", a.col);
                }
                SetOp::Remove => {
                    let _ = write!(cql, "{0} = {0} - ?", a.col);
                }
            }
            values.push_boxed(a.value);
        }
        cql.push_str(" WHERE ");
        for (i, (c, v)) in self.keys.into_iter().enumerate() {
            let _ = write!(cql, "{}{c} = ?", if i == 0 { "" } else { " AND " });
            values.push_boxed(v);
        }
        let lwt = self.if_exists || !self.conditions.is_empty();
        if self.if_exists {
            cql.push_str(" IF EXISTS");
        } else if !self.conditions.is_empty() {
            cql.push_str(" IF ");
            for (i, (c, v)) in self.conditions.into_iter().enumerate() {
                let _ = write!(cql, "{}{c} = ?", if i == 0 { "" } else { " AND " });
                values.push_boxed(v);
            }
        }
        Ok(Built {
            cql,
            values,
            consistency: self.consistency,
            serial_consistency: self.serial_consistency,
            page_size: None,
            limit: None,
            lwt,
        })
    }
}
