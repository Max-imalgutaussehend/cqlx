//! Thin wrapper over the driver session with a prepared-statement cache.

use crate::CqlModel;
use crate::error::{Error, Result};
use crate::query::{BoundValues, Built};
use scylla::client::session::Session as Inner;
use scylla::client::session_builder::SessionBuilder;
use scylla::errors::TranslationError;
use scylla::policies::address_translator::{AddressTranslator, UntranslatedPeer};
use scylla::statement::prepared::PreparedStatement;
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

/// A connected session. Cheap to clone.
#[derive(Clone)]
pub struct Session {
    inner: Arc<Inner>,
    prepared: Arc<RwLock<HashMap<String, PreparedStatement>>>,
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
        }
    }

    /// The underlying driver session.
    pub fn driver(&self) -> &Inner {
        &self.inner
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

    /// Executes a statement (prepared and cached) and discards the result rows.
    pub async fn execute(&self, cql: &str, values: BoundValues<'_>) -> Result<()> {
        let p = self.prepare(cql).await?;
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

    /// Creates the keyspace (if the entity names one, `NetworkTopologyStrategy` RF=1 — dev convenience) and the table.
    pub async fn create_table<M: CqlModel>(&self) -> Result<()> {
        if let Some(ks) = M::CREATE_KEYSPACE_CQL {
            self.execute_raw(ks).await?;
        }
        self.execute_raw(M::CREATE_TABLE_CQL).await
    }

    /// Inserts (upserts) one entity.
    pub async fn insert<M: CqlModel>(&self, entity: &M) -> Result<()> {
        self.execute(M::INSERT_CQL, entity.insert_values()).await
    }

    /// Runs a built `SELECT` and decodes all rows.
    pub async fn fetch<M: CqlModel>(&self, built: Built) -> Result<Vec<M>> {
        let p = self.prepare(&built.cql).await?;
        let res = self
            .inner
            .execute_unpaged(&p, built.values)
            .await
            .map_err(Error::driver)?;
        let rows = res.into_rows_result().map_err(Error::driver)?;
        rows.rows::<M>()
            .map_err(Error::driver)?
            .collect::<std::result::Result<Vec<M>, _>>()
            .map_err(Error::driver)
    }

    /// Runs a built `DELETE`.
    pub async fn run(&self, built: Built) -> Result<()> {
        self.execute(&built.cql, built.values).await
    }
}
