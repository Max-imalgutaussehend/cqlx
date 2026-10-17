use thiserror::Error;

type Source = Box<dyn std::error::Error + Send + Sync>;

/// Errors returned by cqlx.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    /// The driver failed (connect, prepare, execute or row decoding).
    #[error("driver error: {0}")]
    Driver(#[source] Source),
    /// The query is not valid CQL for this table (for example a clustering gap).
    #[error("invalid query: {0}")]
    InvalidQuery(String),
}

impl Error {
    pub(crate) fn driver(e: impl std::error::Error + Send + Sync + 'static) -> Self {
        Error::Driver(Box::new(e))
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
