# cqlx-cli

Installs the `cqlx` binary: extracts `#[derive(CqlEntity)]` schemas from Rust source and prints
`CREATE TABLE` CQL, or diffs them against an existing `.cql` schema (`ALTER TABLE … ADD` plus warnings
for what CQL cannot alter).

```sh
cargo install cqlx-cli
cqlx schema src/ --keyspaces
cqlx diff src/ --schema schema.cql
```

Part of the **cqlz family** with [cqlc](https://github.com/Max-imalgutaussehend/cqlc) and
[cqlx](https://github.com/Max-imalgutaussehend/cqlx).
