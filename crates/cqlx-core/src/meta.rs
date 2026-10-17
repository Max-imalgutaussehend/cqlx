//! Static entity metadata and `CREATE TABLE` rendering.

use std::fmt::Write as _;

/// Clustering order of a clustering column.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Order {
    Asc,
    Desc,
}

impl Order {
    pub fn as_cql(self) -> &'static str {
        match self {
            Order::Asc => "ASC",
            Order::Desc => "DESC",
        }
    }
}

/// Role of a column inside the primary key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KeyRole {
    Partition,
    Clustering(Order),
    Regular,
}

/// One column of an entity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnMeta {
    /// Column name in CQL (after `rename`).
    pub name: String,
    /// Rust field name.
    pub field: String,
    /// CQL type, e.g. `text` or `list<int>`.
    pub cql_type: String,
    pub role: KeyRole,
}

/// Everything known about an entity at compile time.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EntityMeta {
    pub keyspace: Option<String>,
    pub table: String,
    /// All columns in declaration order.
    pub columns: Vec<ColumnMeta>,
}

impl EntityMeta {
    /// Partition key columns in declaration order.
    pub fn partition_key(&self) -> Vec<&ColumnMeta> {
        self.columns
            .iter()
            .filter(|c| c.role == KeyRole::Partition)
            .collect()
    }

    /// Clustering columns in declaration order.
    pub fn clustering(&self) -> Vec<&ColumnMeta> {
        self.columns
            .iter()
            .filter(|c| matches!(c.role, KeyRole::Clustering(_)))
            .collect()
    }

    /// `keyspace.table` or just `table`.
    pub fn qualified_name(&self) -> String {
        match &self.keyspace {
            Some(k) => format!("{k}.{}", self.table),
            None => self.table.clone(),
        }
    }

    /// Renders an idempotent `CREATE TABLE IF NOT EXISTS` statement (with trailing `;`).
    pub fn create_table_cql(&self) -> String {
        let mut s = format!("CREATE TABLE IF NOT EXISTS {} (\n", self.qualified_name());
        for c in &self.columns {
            let _ = writeln!(s, "    {} {},", c.name, c.cql_type);
        }
        let pk: Vec<_> = self
            .partition_key()
            .iter()
            .map(|c| c.name.clone())
            .collect();
        let part = if pk.len() == 1 {
            pk[0].clone()
        } else {
            format!("({})", pk.join(", "))
        };
        let clustering = self.clustering();
        let mut key = part;
        for c in &clustering {
            key.push_str(", ");
            key.push_str(&c.name);
        }
        let _ = write!(s, "    PRIMARY KEY ({key})\n)");
        if !clustering.is_empty() {
            let order: Vec<_> = clustering
                .iter()
                .map(|c| match c.role {
                    KeyRole::Clustering(o) => format!("{} {}", c.name, o.as_cql()),
                    _ => unreachable!(),
                })
                .collect();
            let _ = write!(s, " WITH CLUSTERING ORDER BY ({})", order.join(", "));
        }
        s.push(';');
        s
    }

    /// `CREATE KEYSPACE IF NOT EXISTS` for this entity's keyspace, if it has one.
    pub fn create_keyspace_cql(&self) -> Option<String> {
        self.keyspace.as_ref().map(|k| {
            format!(
                "CREATE KEYSPACE IF NOT EXISTS {k} WITH replication = \
                 {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}};"
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(name: &str, ty: &str, role: KeyRole) -> ColumnMeta {
        ColumnMeta {
            name: name.into(),
            field: name.into(),
            cql_type: ty.into(),
            role,
        }
    }

    #[test]
    fn renders_composite_key_and_ordering() {
        let m = EntityMeta {
            keyspace: Some("app".into()),
            table: "events".into(),
            columns: vec![
                col("tenant", "text", KeyRole::Partition),
                col("day", "date", KeyRole::Partition),
                col("ts", "timestamp", KeyRole::Clustering(Order::Desc)),
                col("body", "text", KeyRole::Regular),
            ],
        };
        let cql = m.create_table_cql();
        assert!(cql.contains("PRIMARY KEY ((tenant, day), ts)"));
        assert!(cql.contains("WITH CLUSTERING ORDER BY (ts DESC);"));
        assert!(cql.starts_with("CREATE TABLE IF NOT EXISTS app.events ("));
    }

    #[test]
    fn single_partition_key_has_no_extra_parens() {
        let m = EntityMeta {
            keyspace: None,
            table: "users".into(),
            columns: vec![
                col("id", "uuid", KeyRole::Partition),
                col("n", "text", KeyRole::Regular),
            ],
        };
        assert!(m.create_table_cql().contains("PRIMARY KEY (id)\n);"));
    }
}
