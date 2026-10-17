//! Library half of the `cqlx` CLI: source scanning and schema diffing.

use anyhow::{Context, Result};
use cqlc_core::{Input, compile};
use cqlx_core::EntityMeta;
use cqlx_core::parse::parse_entity;
use std::collections::BTreeSet;
use std::path::Path;
use syn::{Item, ItemStruct};
use walkdir::WalkDir;

/// An entity found in a source file.
#[derive(Debug, Clone)]
pub struct Found {
    pub file: String,
    pub meta: EntityMeta,
}

/// Scans a file or directory tree for `#[derive(CqlEntity)]` structs. Output order is deterministic.
pub fn scan(path: &Path) -> Result<Vec<Found>> {
    let mut files: Vec<_> = WalkDir::new(path)
        .into_iter()
        .filter_entry(|e| !matches!(e.file_name().to_str(), Some("target" | ".git")))
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file() && e.path().extension().is_some_and(|x| x == "rs"))
        .map(|e| e.into_path())
        .collect();
    files.sort();
    let mut out = Vec::new();
    for f in files {
        let src =
            std::fs::read_to_string(&f).with_context(|| format!("reading {}", f.display()))?;
        let ast = syn::parse_file(&src).with_context(|| format!("parsing {}", f.display()))?;
        let mut structs = Vec::new();
        collect(&ast.items, &mut structs);
        for s in structs {
            let input: syn::DeriveInput = syn::parse2(quote::quote!(#s))?;
            let parsed = parse_entity(&input)
                .map_err(|e| anyhow::anyhow!("{}: struct `{}`: {e}", f.display(), s.ident))?;
            out.push(Found {
                file: f.display().to_string(),
                meta: parsed.meta,
            });
        }
    }
    Ok(out)
}

fn collect<'a>(items: &'a [Item], out: &mut Vec<&'a ItemStruct>) {
    for item in items {
        match item {
            Item::Struct(s) if derives_entity(s) => out.push(s),
            Item::Mod(m) => {
                if let Some((_, inner)) = &m.content {
                    collect(inner, out);
                }
            }
            _ => {}
        }
    }
}

fn derives_entity(s: &ItemStruct) -> bool {
    s.attrs
        .iter()
        .filter(|a| a.path().is_ident("derive"))
        .any(|a| {
            let mut hit = false;
            let _ = a.parse_nested_meta(|m| {
                if m.path
                    .segments
                    .last()
                    .is_some_and(|s| s.ident == "CqlEntity")
                {
                    hit = true;
                }
                Ok(())
            });
            hit
        })
}

/// All `CREATE KEYSPACE` / `CREATE TABLE` statements for the entities.
pub fn render_schema(found: &[Found], with_keyspaces: bool) -> String {
    let mut out = String::new();
    if with_keyspaces {
        let ks: BTreeSet<_> = found
            .iter()
            .filter_map(|f| f.meta.create_keyspace_cql())
            .collect();
        for k in ks {
            out.push_str(&k);
            out.push_str("\n\n");
        }
    }
    for f in found {
        out.push_str(&format!("-- {}\n{}\n\n", f.file, f.meta.create_table_cql()));
    }
    out
}

fn norm(t: &str) -> String {
    t.to_ascii_lowercase().replace(' ', "")
}

/// Compares entities against an existing schema (`.cql` DDL) and returns migration CQL.
/// Changes CQL cannot express (dropped columns, type or key changes) become `-- WARNING` comments.
pub fn diff(found: &[Found], existing_cql: &str) -> Result<String> {
    let c = compile(&[Input::new("existing.cql", existing_cql)], &[]);
    if let Some(d) = c.diagnostics.iter().find(|d| d.is_error()) {
        anyhow::bail!("existing schema does not parse: {}", d.message);
    }
    let mut out = String::new();
    for f in found {
        let m = &f.meta;
        let name = m.qualified_name();
        let table = c.schema.tables.iter().find(|t| {
            t.name == m.table
                && (t.keyspace == m.keyspace || t.keyspace.is_none() || m.keyspace.is_none())
        });
        let Some(t) = table else {
            out.push_str(&format!("{}\n\n", m.create_table_cql()));
            continue;
        };
        for col in &m.columns {
            match t.column(&col.name) {
                None => out.push_str(&format!("ALTER TABLE {name} ADD {} {};\n", col.name, col.cql_type)),
                Some(old) if norm(&old.cql_type.to_string()) != norm(&col.cql_type) => out.push_str(&format!(
                    "-- WARNING: {name}.{} changes type {} -> {}; CQL cannot alter this, create a new column\n",
                    col.name, old.cql_type, col.cql_type
                )),
                Some(_) => {}
            }
        }
        for old in &t.columns {
            if !m.columns.iter().any(|c| c.name == old.name) {
                out.push_str(&format!(
                    "-- WARNING: {name}.{} exists in the schema but not in code (not dropped)\n",
                    old.name
                ));
            }
        }
        let want_pk: Vec<_> = m.partition_key().iter().map(|c| c.name.clone()).collect();
        let want_ck: Vec<_> = m.clustering().iter().map(|c| c.name.clone()).collect();
        let have_ck: Vec<_> = t.clustering.iter().map(|c| c.name.clone()).collect();
        if want_pk != t.partition_key || want_ck != have_ck {
            out.push_str(&format!("-- WARNING: {name} primary key differs from the schema; keys are immutable in CQL\n"));
        }
    }
    Ok(out)
}
