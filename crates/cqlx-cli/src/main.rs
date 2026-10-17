use anyhow::Result;
use clap::{Parser, Subcommand};
use std::path::PathBuf;

/// cqlx: extract schemas from `#[derive(CqlEntity)]` structs. Part of the cqlz family.
#[derive(Parser)]
#[command(version, about)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Print `CREATE TABLE` statements for every entity found under PATH.
    Schema {
        /// Rust source file or directory.
        path: PathBuf,
        /// Also emit `CREATE KEYSPACE IF NOT EXISTS` (RF=1, development only).
        #[arg(long)]
        keyspaces: bool,
        /// Write to a file instead of stdout.
        #[arg(short, long)]
        out: Option<PathBuf>,
    },
    /// Diff entities against an existing schema file and print migration CQL.
    Diff {
        /// Rust source file or directory.
        path: PathBuf,
        /// Existing schema (`.cql` DDL).
        #[arg(long)]
        schema: PathBuf,
    },
}

fn main() -> Result<()> {
    match Cli::parse().cmd {
        Cmd::Schema {
            path,
            keyspaces,
            out,
        } => {
            let text = cqlx_cli::render_schema(&cqlx_cli::scan(&path)?, keyspaces);
            match out {
                Some(p) => std::fs::write(p, text)?,
                None => print!("{text}"),
            }
        }
        Cmd::Diff { path, schema } => {
            let existing = std::fs::read_to_string(&schema)?;
            let out = cqlx_cli::diff(&cqlx_cli::scan(&path)?, &existing)?;
            if out.is_empty() {
                eprintln!("schema is up to date");
            } else {
                print!("{out}");
            }
        }
    }
    Ok(())
}
