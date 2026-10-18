//! Session wrapper over the driver: prepared-statement cache, typed reads, streaming, writes with options,
//! lightweight transactions, batches and schema validation/migration.

use crate::error::{Error, Result};
use crate::query::{BoundValues, Built};
use crate::{CqlModel, Insertable};
use futures::{Stream, StreamExt};
use scylla::client::session::Session as Inner;
use scylla::client::session_builder::SessionBuilder;
use scylla::errors::TranslationError;
use scylla::policies::address_translator::{AddressTranslator, UntranslatedPeer};
use scylla::statement::batch::{Batch, BatchType};
use scylla::statement::prepared::PreparedStatement;
use scylla::statement::{Consistency, SerialConsistency};
use scylla::value::{CqlValue, Row};
use std::collections::HashMap;
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::{Arc, RwLock};

/// Maps every node the cluster advertises to one fixed address (Docker port mappings, SSH tunnels).
struct SingleNode(SocketAddr);

#[async_trait::async_trait]
impl AddressTranslator for SingleNode {
    async fn translate_address(
        &self,
        _: &UntranslatedPeer,
    ) -> Result<SocketAddr, TranslationError> {
        Ok(self.0)
    }
}

/// Options of a single insert.
#[derive(Clone, Copy, Debug, Default)]
pub struct InsertOptions {
    /// `USING TTL n` (seconds).
    pub ttl: Option<i32>,
    /// `USING TIMESTAMP n` (microseconds).
    pub timestamp: Option<i64>,
    /// `IF NOT EXISTS`: a lightweight transaction; the call returns whether the row was inserted.
    pub if_not_exists: bool,
    pub consistency: Option<Consistency>,
    pub serial_consistency: Option<SerialConsistency>,
}

impl InsertOptions {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn ttl(mut self, seconds: i32) -> Self {
        self.ttl = Some(seconds);
        self
    }
    pub fn timestamp(mut self, micros: i64) -> Self {
        self.timestamp = Some(micros);
        self
    }
    pub fn if_not_exists(mut self) -> Self {
        self.if_not_exists = true;
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
}

/// One difference between an entity and the table in the cluster.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SchemaIssue {
    TableMissing,
    ColumnMissing {
        column: String,
    },
    ColumnNotInEntity {
        column: String,
    },
    TypeMismatch {
        column: String,
        expected: String,
        found: String,
    },
    KeyMismatch {
        column: String,
        expected: String,
        found: String,
    },
}

impl std::fmt::Display for SchemaIssue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SchemaIssue::TableMissing => write!(f, "table does not exist"),
            SchemaIssue::ColumnMissing { column } => {
                write!(f, "column `{column}` is missing in the table")
            }
            SchemaIssue::ColumnNotInEntity { column } => write!(
                f,
                "table has column `{column}` that the entity does not map"
            ),
            SchemaIssue::TypeMismatch {
                column,
                expected,
                found,
            } => write!(
                f,
                "column `{column}` is `{found}` in the table but `{expected}` in the entity"
            ),
            SchemaIssue::KeyMismatch {
                column,
                expected,
                found,
            } => write!(
                f,
                "column `{column}` is `{found}` in the table but `{expected}` in the entity"
            ),
        }
    }
}

/// A connected session. Cheap to clone.
#[derive(Clone)]
pub struct Session {
    inner: Arc<Inner>,
    prepared: Arc<RwLock<HashMap<String, PreparedStatement>>>,
    consistency: Option<Consistency>,
}

impl Session {
    /// Connects to `host:port` (e.g. `127.0.0.1:9042`).
    pub async fn connect(node: &str) -> Result<Self> {
        let inner = SessionBuilder::new()
            .known_node(node)
            .build()
            .await
            .map_err(Error::driver)?;
        Ok(Self::from_driver(inner))
    }

    /// Like [`connect`](Self::connect) for a single node behind NAT: every address the node
    /// advertises is rewritten to `node`. Meant for Docker/testcontainers and tunnels.
    pub async fn connect_single_node(node: &str) -> Result<Self> {
        let addr = node
            .to_socket_addrs()
            .map_err(Error::driver)?
            .next()
            .ok_or_else(|| {
                Error::InvalidQuery(format!("`{node}` did not resolve to an address"))
            })?;
        let inner = SessionBuilder::new()
            .known_node_addr(addr)
            .address_translator(Arc::new(SingleNode(addr)))
            .build()
            .await
            .map_err(Error::driver)?;
        Ok(Self::from_driver(inner))
    }

    /// Wraps an existing driver session.
    pub fn from_driver(inner: Inner) -> Self {
        Self {
            inner: Arc::new(inner),
            prepared: Default::default(),
            consistency: None,
        }
    }

    /// Unwraps the driver session if this is the only clone (for APIs that take ownership, e.g. `CachingSession`).
    pub fn try_into_driver(self) -> Result<Inner, Self> {
        match Arc::try_unwrap(self.inner) {
            Ok(inner) => Ok(inner),
            Err(inner) => Err(Self {
                inner,
                prepared: self.prepared,
                consistency: self.consistency,
            }),
        }
    }

    /// The underlying driver session.
    pub fn driver(&self) -> &Inner {
        &self.inner
    }

    /// A clone of this session whose statements default to consistency level `c`
    /// (per-statement options still win).
    pub fn with_consistency(&self, c: Consistency) -> Self {
        Self {
            consistency: Some(c),
            ..self.clone()
        }
    }

    async fn prepare(&self, cql: &str) -> Result<PreparedStatement> {
        if let Some(p) = self.prepared.read().expect("poisoned").get(cql) {
            return Ok(p.clone());
        }
        let p = self.inner.prepare(cql).await.map_err(Error::driver)?;
        self.prepared
            .write()
            .expect("poisoned")
            .insert(cql.to_owned(), p.clone());
        Ok(p)
    }

    async fn prepare_for(&self, built: &Built) -> Result<PreparedStatement> {
        let mut p = self.prepare(&built.cql).await?;
        if let Some(c) = built.consistency.or(self.consistency) {
            p.set_consistency(c);
        }
        if let Some(c) = built.serial_consistency {
            p.set_serial_consistency(Some(c));
        }
        if let Some(n) = built.page_size {
            p.set_page_size(n);
        }
        Ok(p)
    }

    /// Executes a statement (prepared and cached) and discards the result rows.
    pub async fn execute(&self, cql: &str, values: BoundValues<'_>) -> Result<()> {
        let mut p = self.prepare(cql).await?;
        if let Some(c) = self.consistency {
            p.set_consistency(c);
        }
        self.inner
            .execute_unpaged(&p, values)
            .await
            .map_err(Error::driver)?;
        Ok(())
    }

    /// Runs a raw statement without bind values (DDL).
    pub async fn execute_raw(&self, cql: &str) -> Result<()> {
        self.inner
            .query_unpaged(cql, [])
            .await
            .map_err(Error::driver)?;
        Ok(())
    }

    /// Creates the keyspace (if the entity names one, `NetworkTopologyStrategy` RF=1: a development convenience) and the table.
    pub async fn create_table<M: CqlModel>(&self) -> Result<()> {
        if let Some(ks) = M::CREATE_KEYSPACE_CQL {
            self.execute_raw(ks).await?;
        }
        self.execute_raw(M::CREATE_TABLE_CQL).await
    }

    /// Inserts (upserts) one entity.
    pub async fn insert<M: CqlModel + Insertable>(&self, entity: &M) -> Result<()> {
        self.execute(M::INSERT_CQL, entity.insert_values()).await
    }

    /// Inserts one entity with TTL, timestamp, consistency and/or `IF NOT EXISTS`.
    /// Returns whether the row was written (always `true` without `IF NOT EXISTS`).
    pub async fn insert_with<M: CqlModel + Insertable>(
        &self,
        entity: &M,
        opts: InsertOptions,
    ) -> Result<bool> {
        let mut cql = M::INSERT_CQL.to_string();
        if opts.if_not_exists {
            cql.push_str(" IF NOT EXISTS");
        }
        match (opts.ttl, opts.timestamp) {
            (Some(t), Some(ts)) => cql.push_str(&format!(" USING TTL {t} AND TIMESTAMP {ts}")),
            (Some(t), None) => cql.push_str(&format!(" USING TTL {t}")),
            (None, Some(ts)) => cql.push_str(&format!(" USING TIMESTAMP {ts}")),
            (None, None) => {}
        }
        let mut p = self.prepare(&cql).await?;
        if let Some(c) = opts.consistency.or(self.consistency) {
            p.set_consistency(c);
        }
        if let Some(c) = opts.serial_consistency {
            p.set_serial_consistency(Some(c));
        }
        let res = self
            .inner
            .execute_unpaged(&p, entity.insert_values())
            .await
            .map_err(Error::driver)?;
        if opts.if_not_exists {
            applied(res)
        } else {
            Ok(true)
        }
    }

    /// Deletes the row with the entity's primary key.
    pub async fn delete_entity<M: CqlModel>(&self, entity: &M) -> Result<()> {
        self.execute(M::DELETE_CQL, entity.key_values()).await
    }

    /// Runs a built `SELECT` and decodes all rows. Statements with a small `LIMIT` run unpaged (one round trip);
    /// everything else is paged by the driver.
    pub async fn fetch<M: CqlModel>(&self, built: Built) -> Result<Vec<M>> {
        let p = self.prepare_for(&built).await?;
        if built.limit.is_some_and(|l| l <= 10_000) {
            let res = self
                .inner
                .execute_unpaged(&p, built.values)
                .await
                .map_err(Error::driver)?;
            let rows = res.into_rows_result().map_err(Error::driver)?;
            return rows
                .rows::<M>()
                .map_err(Error::driver)?
                .collect::<std::result::Result<Vec<M>, _>>()
                .map_err(Error::driver);
        }
        let mut out = Vec::new();
        let mut s = std::pin::pin!(self.stream_prepared::<M>(p, built.values).await?);
        while let Some(row) = s.next().await {
            out.push(row?);
        }
        Ok(out)
    }

    /// Streams rows page by page without holding the whole result in memory.
    pub async fn stream<M: CqlModel>(
        &self,
        built: Built,
    ) -> Result<impl Stream<Item = Result<M>> + use<M>> {
        let p = self.prepare_for(&built).await?;
        self.stream_prepared::<M>(p, built.values).await
    }

    async fn stream_prepared<M: CqlModel>(
        &self,
        p: PreparedStatement,
        values: BoundValues<'_>,
    ) -> Result<impl Stream<Item = Result<M>> + use<M>> {
        let pager = self
            .inner
            .execute_iter(p, values)
            .await
            .map_err(Error::driver)?;
        let rows = pager.rows_stream::<M>().map_err(Error::driver)?;
        Ok(rows.map(|r| r.map_err(Error::driver)))
    }

    /// Runs a built `SELECT COUNT(*)`.
    pub async fn count(&self, built: Built) -> Result<i64> {
        let p = self.prepare_for(&built).await?;
        let res = self
            .inner
            .execute_unpaged(&p, built.values)
            .await
            .map_err(Error::driver)?;
        let rows = res.into_rows_result().map_err(Error::driver)?;
        let (n,): (i64,) = rows.first_row().map_err(Error::driver)?;
        Ok(n)
    }

    /// Runs a built write (`DELETE`, `UPDATE`).
    pub async fn run(&self, built: Built) -> Result<()> {
        let p = self.prepare_for(&built).await?;
        self.inner
            .execute_unpaged(&p, built.values)
            .await
            .map_err(Error::driver)?;
        Ok(())
    }

    /// Runs a built conditional write (`IF ...`) and returns whether it was applied.
    pub async fn run_lwt(&self, built: Built) -> Result<bool> {
        let p = self.prepare_for(&built).await?;
        let res = self
            .inner
            .execute_unpaged(&p, built.values)
            .await
            .map_err(Error::driver)?;
        applied(res)
    }

    /// Starts a batch of inserts and deletes.
    pub fn batch<'e>(&self, kind: BatchKind) -> BatchBuilder<'_, 'e> {
        BatchBuilder {
            session: self,
            kind,
            statements: Vec::new(),
            values: Vec::new(),
        }
    }

    /// Compares an entity with the table in the cluster.
    pub async fn validate_schema<M: CqlModel>(&self) -> Result<Vec<SchemaIssue>> {
        let meta = M::meta();
        let keyspace = match &meta.keyspace {
            Some(k) => k.clone(),
            None => self
                .inner
                .get_keyspace()
                .map(|k| k.to_string())
                .ok_or_else(|| {
                    Error::Schema(
                        "the entity has no keyspace and the session has no current keyspace".into(),
                    )
                })?,
        };
        let res = self
            .inner
            .query_unpaged(
                "SELECT column_name, kind, type, clustering_order FROM system_schema.columns WHERE keyspace_name = ? AND table_name = ?",
                (keyspace, meta.table.clone()),
            )
            .await
            .map_err(Error::driver)?;
        let rows: Vec<(String, String, String, String)> = res
            .into_rows_result()
            .map_err(Error::driver)?
            .rows::<(String, String, String, String)>()
            .map_err(Error::driver)?
            .collect::<std::result::Result<_, _>>()
            .map_err(Error::driver)?;
        if rows.is_empty() {
            return Ok(vec![SchemaIssue::TableMissing]);
        }
        let norm = |t: &str| {
            t.to_ascii_lowercase()
                .replace("frozen<", "")
                .replace(['>', ' '], "")
                .replace("varchar", "text")
        };
        let mut issues = Vec::new();
        for c in &meta.columns {
            let Some((_, kind, ty, order)) = rows.iter().find(|(n, ..)| *n == c.name) else {
                issues.push(SchemaIssue::ColumnMissing {
                    column: c.name.clone(),
                });
                continue;
            };
            let want_kind = match c.role {
                crate::KeyRole::Partition => "partition_key".to_string(),
                crate::KeyRole::Clustering(_) => "clustering".to_string(),
                crate::KeyRole::Regular => "regular".to_string(),
            };
            let kind_ok =
                kind == &want_kind || (c.role == crate::KeyRole::Regular && kind == "static");
            if !kind_ok {
                issues.push(SchemaIssue::KeyMismatch {
                    column: c.name.clone(),
                    expected: want_kind.clone(),
                    found: kind.clone(),
                });
            } else if let crate::KeyRole::Clustering(o) = c.role
                && !order.eq_ignore_ascii_case(o.as_cql())
            {
                issues.push(SchemaIssue::KeyMismatch {
                    column: c.name.clone(),
                    expected: format!("clustering {}", o.as_cql()),
                    found: format!("clustering {order}"),
                });
            }
            if norm(&c.cql_type) != norm(ty) {
                issues.push(SchemaIssue::TypeMismatch {
                    column: c.name.clone(),
                    expected: c.cql_type.clone(),
                    found: ty.clone(),
                });
            }
        }
        for (n, ..) in &rows {
            if !meta.columns.iter().any(|c| &c.name == n) {
                issues.push(SchemaIssue::ColumnNotInEntity { column: n.clone() });
            }
        }
        Ok(issues)
    }

    /// Brings the cluster up to the entity: creates the table if it is missing and adds missing regular columns
    /// (`ALTER TABLE ... ADD`). Never drops or retypes anything. Returns the statements it ran.
    /// Fails if the primary key or a column type differs, since those cannot be migrated in CQL.
    pub async fn migrate<M: CqlModel>(&self) -> Result<Vec<String>> {
        let issues = self.validate_schema::<M>().await?;
        let meta = M::meta();
        let mut ran = Vec::new();
        if issues.contains(&SchemaIssue::TableMissing) {
            if let Some(ks) = M::CREATE_KEYSPACE_CQL {
                self.execute_raw(ks).await?;
                ran.push(ks.to_string());
            }
            self.execute_raw(M::CREATE_TABLE_CQL).await?;
            ran.push(M::CREATE_TABLE_CQL.to_string());
            return Ok(ran);
        }
        for issue in &issues {
            match issue {
                SchemaIssue::ColumnMissing { column } => {
                    let c = meta
                        .columns
                        .iter()
                        .find(|c| &c.name == column)
                        .expect("from meta");
                    if c.role != crate::KeyRole::Regular {
                        return Err(Error::Schema(format!(
                            "key column `{column}` is missing; keys cannot be added to an existing table"
                        )));
                    }
                    let stmt = format!(
                        "ALTER TABLE {} ADD {} {}",
                        meta.qualified_name(),
                        c.name,
                        c.cql_type
                    );
                    self.execute_raw(&stmt).await?;
                    ran.push(stmt);
                }
                SchemaIssue::ColumnNotInEntity { .. } => {}
                other => return Err(Error::Schema(format!("{}: {other}", meta.qualified_name()))),
            }
        }
        Ok(ran)
    }
}

/// Kind of a batch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BatchKind {
    /// Atomic across partitions (with a batch-log write).
    Logged,
    /// No batch log; atomic only within one partition.
    Unlogged,
    /// Counter updates only.
    Counter,
}

/// Collects statements for one `BATCH`.
pub struct BatchBuilder<'s, 'e> {
    session: &'s Session,
    kind: BatchKind,
    statements: Vec<String>,
    values: Vec<BoundValues<'e>>,
}

impl<'e> BatchBuilder<'_, 'e> {
    /// Adds an insert of `entity`.
    pub fn insert<M: CqlModel + Insertable>(mut self, entity: &'e M) -> Self {
        self.statements.push(M::INSERT_CQL.to_string());
        self.values.push(entity.insert_values());
        self
    }

    /// Adds a delete of the row with the entity's primary key.
    pub fn delete<M: CqlModel>(mut self, entity: &'e M) -> Self {
        self.statements.push(M::DELETE_CQL.to_string());
        self.values.push(entity.key_values());
        self
    }

    /// Adds a built statement (`DELETE`/`UPDATE` from a builder).
    pub fn push(mut self, built: Built) -> Self {
        self.statements.push(built.cql);
        self.values.push(built.values);
        self
    }

    /// Executes the batch.
    pub async fn execute(self) -> Result<()> {
        if self.statements.is_empty() {
            return Ok(());
        }
        let mut batch = Batch::new(match self.kind {
            BatchKind::Logged => BatchType::Logged,
            BatchKind::Unlogged => BatchType::Unlogged,
            BatchKind::Counter => BatchType::Counter,
        });
        for s in &self.statements {
            batch.append_statement(self.session.prepare(s).await?);
        }
        if let Some(c) = self.session.consistency {
            batch.set_consistency(c);
        }
        self.session
            .inner
            .batch(&batch, self.values)
            .await
            .map_err(Error::driver)?;
        Ok(())
    }
}

fn applied(res: scylla::response::query_result::QueryResult) -> Result<bool> {
    let rows = res.into_rows_result().map_err(Error::driver)?;
    let row: Row = rows.first_row().map_err(Error::driver)?;
    match row.columns.first() {
        Some(Some(CqlValue::Boolean(b))) => Ok(*b),
        _ => Err(Error::InvalidQuery(
            "conditional statement did not return `[applied]`".into(),
        )),
    }
}
