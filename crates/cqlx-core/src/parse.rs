//! Parsing of `#[derive(CqlEntity)]` input into [`EntityMeta`]. Shared by the proc-macro and the CLI.

use crate::meta::{ColumnMeta, EntityMeta, KeyRole, Order};
use syn::{Data, DeriveInput, Error, Fields, GenericArgument, Ident, LitStr, PathArguments, Type};

/// A struct field together with the metadata derived from it.
pub struct ParsedField {
    pub ident: Ident,
    pub ty: Type,
    pub column: ColumnMeta,
}

/// Result of parsing one entity struct.
pub struct ParsedEntity {
    pub ident: Ident,
    pub meta: EntityMeta,
    pub fields: Vec<ParsedField>,
}

/// Parses a `DeriveInput` carrying `#[cqlx(...)]` attributes.
pub fn parse_entity(input: &DeriveInput) -> syn::Result<ParsedEntity> {
    let Data::Struct(data) = &input.data else {
        return Err(Error::new_spanned(
            &input.ident,
            "CqlEntity can only be derived for structs",
        ));
    };
    let Fields::Named(named) = &data.fields else {
        return Err(Error::new_spanned(
            &input.ident,
            "CqlEntity requires a struct with named fields",
        ));
    };
    if !input.generics.params.is_empty() {
        return Err(Error::new_spanned(
            &input.generics,
            "CqlEntity does not support generic structs",
        ));
    }

    let mut keyspace = None;
    let mut table = None;
    for attr in input.attrs.iter().filter(|a| a.path().is_ident("cqlx")) {
        attr.parse_nested_meta(|m| {
            if m.path.is_ident("table_name") {
                table = Some(m.value()?.parse::<LitStr>()?.value());
            } else if m.path.is_ident("keyspace") {
                keyspace = Some(m.value()?.parse::<LitStr>()?.value());
            } else {
                return Err(
                    m.error("unknown struct attribute; expected `table_name` or `keyspace`")
                );
            }
            Ok(())
        })?;
    }

    let mut fields = Vec::new();
    for f in &named.named {
        let ident = f.ident.clone().expect("named field");
        let mut name = ident.to_string();
        let mut role = KeyRole::Regular;
        let mut cql_override = None;
        for attr in f.attrs.iter().filter(|a| a.path().is_ident("cqlx")) {
            attr.parse_nested_meta(|m| {
                if m.path.is_ident("partition_key") || m.path.is_ident("primary_key") {
                    set_role(&mut role, KeyRole::Partition, &m.path)?;
                } else if m.path.is_ident("clustering_key") {
                    let mut order = Order::Asc;
                    if m.input.peek(syn::token::Paren) {
                        m.parse_nested_meta(|o| {
                            if !o.path.is_ident("order") {
                                return Err(o.error("expected `order = \"asc\" | \"desc\"`"));
                            }
                            let lit: LitStr = o.value()?.parse()?;
                            order = match lit.value().to_ascii_lowercase().as_str() {
                                "asc" => Order::Asc,
                                "desc" => Order::Desc,
                                _ => {
                                    return Err(Error::new_spanned(
                                        lit,
                                        "order must be \"asc\" or \"desc\"",
                                    ));
                                }
                            };
                            Ok(())
                        })?;
                    }
                    set_role(&mut role, KeyRole::Clustering(order), &m.path)?;
                } else if m.path.is_ident("rename") {
                    name = m.value()?.parse::<LitStr>()?.value();
                } else if m.path.is_ident("cql_type") {
                    cql_override = Some(m.value()?.parse::<LitStr>()?.value());
                } else {
                    return Err(m.error(
                        "unknown field attribute; expected `partition_key`, `primary_key`, \
                         `clustering_key`, `rename` or `cql_type`",
                    ));
                }
                Ok(())
            })?;
        }
        let cql_type = match cql_override {
            Some(t) => t,
            None => cql_type_of(&f.ty).map_err(|e| Error::new_spanned(&f.ty, e))?,
        };
        let column = ColumnMeta {
            name,
            field: ident.to_string(),
            cql_type,
            role,
        };
        fields.push(ParsedField {
            ident,
            ty: f.ty.clone(),
            column,
        });
    }

    let meta = EntityMeta {
        keyspace,
        table: table.unwrap_or_else(|| snake_case(&input.ident.to_string())),
        columns: fields.iter().map(|f| f.column.clone()).collect(),
    };

    if meta.partition_key().is_empty() {
        return Err(Error::new_spanned(
            &input.ident,
            "entity needs at least one `#[cqlx(partition_key)]` (or `primary_key`) field",
        ));
    }
    for (i, c) in meta.columns.iter().enumerate() {
        if meta.columns[..i].iter().any(|o| o.name == c.name) {
            return Err(Error::new_spanned(
                &input.ident,
                format!("duplicate column name `{}`", c.name),
            ));
        }
    }
    Ok(ParsedEntity {
        ident: input.ident.clone(),
        meta,
        fields,
    })
}

fn set_role(slot: &mut KeyRole, new: KeyRole, at: &syn::Path) -> syn::Result<()> {
    if *slot != KeyRole::Regular {
        return Err(Error::new_spanned(
            at,
            "a field can only have one key attribute",
        ));
    }
    *slot = new;
    Ok(())
}

/// `EventLog` -> `event_log`.
pub fn snake_case(s: &str) -> String {
    let mut out = String::new();
    for (i, ch) in s.chars().enumerate() {
        if ch.is_uppercase() {
            if i > 0 {
                out.push('_');
            }
            out.extend(ch.to_lowercase());
        } else {
            out.push(ch);
        }
    }
    out
}

/// Maps a Rust type to its CQL type. `Option<T>` maps like `T`.
pub fn cql_type_of(ty: &Type) -> Result<String, String> {
    match ty {
        Type::Reference(r) => cql_type_of(&r.elem),
        Type::Paren(p) => cql_type_of(&p.elem),
        Type::Group(g) => cql_type_of(&g.elem),
        Type::Path(p) => {
            let seg = p.path.segments.last().ok_or("empty type path")?;
            let args: Vec<&Type> = match &seg.arguments {
                PathArguments::AngleBracketed(a) => a
                    .args
                    .iter()
                    .filter_map(|g| {
                        if let GenericArgument::Type(t) = g {
                            Some(t)
                        } else {
                            None
                        }
                    })
                    .collect(),
                _ => vec![],
            };
            let name = seg.ident.to_string();
            let simple = match name.as_str() {
                "String" | "str" => "text",
                "i8" => "tinyint",
                "i16" => "smallint",
                "i32" => "int",
                "i64" => "bigint",
                "f32" => "float",
                "f64" => "double",
                "bool" => "boolean",
                "Uuid" => "uuid",
                "CqlTimeuuid" | "TimeUuid" => "timeuuid",
                "NaiveDate" | "CqlDate" | "Date" => "date",
                "DateTime" | "CqlTimestamp" | "OffsetDateTime" => "timestamp",
                "NaiveTime" | "CqlTime" => "time",
                "IpAddr" | "Ipv4Addr" | "Ipv6Addr" => "inet",
                "CqlDuration" => "duration",
                "BigDecimal" | "CqlDecimal" => "decimal",
                "BigInt" | "CqlVarint" => "varint",
                "Counter" => "counter",
                _ => "",
            };
            if !simple.is_empty() {
                return Ok(simple.into());
            }
            let one = |what: &str| {
                args.first()
                    .copied()
                    .ok_or(format!("`{what}` needs a type argument"))
            };
            match name.as_str() {
                "Option" | "Box" => cql_type_of(one(&name)?),
                "Vec" => {
                    let inner = one("Vec")?;
                    if matches!(inner, Type::Path(p) if p.path.is_ident("u8")) {
                        Ok("blob".into())
                    } else {
                        Ok(format!("list<{}>", cql_type_of(inner)?))
                    }
                }
                "HashSet" | "BTreeSet" => Ok(format!("set<{}>", cql_type_of(one(&name)?)?)),
                "HashMap" | "BTreeMap" => {
                    let (k, v) = (args.first(), args.get(1));
                    match (k, v) {
                        (Some(k), Some(v)) => {
                            Ok(format!("map<{}, {}>", cql_type_of(k)?, cql_type_of(v)?))
                        }
                        _ => Err(format!("`{name}` needs key and value type arguments")),
                    }
                }
                other => Err(format!(
                    "no CQL type known for `{other}`; add `#[cqlx(cql_type = \"...\")]` to the field"
                )),
            }
        }
        _ => Err("unsupported field type; add `#[cqlx(cql_type = \"...\")]`".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(src: &str) -> syn::Result<ParsedEntity> {
        parse_entity(&syn::parse_str::<DeriveInput>(src).unwrap())
    }

    #[test]
    fn parses_attributes_and_types() {
        let e = parse(
            r#"
            #[cqlx(table_name = "events", keyspace = "app")]
            struct Event {
                #[cqlx(partition_key)] tenant: String,
                #[cqlx(clustering_key(order = "desc"))] ts: i64,
                #[cqlx(rename = "payload")] body: Option<Vec<u8>>,
                tags: std::collections::HashSet<String>,
            }"#,
        )
        .unwrap();
        assert_eq!(e.meta.qualified_name(), "app.events");
        let cols: Vec<_> = e
            .meta
            .columns
            .iter()
            .map(|c| (c.name.as_str(), c.cql_type.as_str()))
            .collect();
        assert_eq!(
            cols,
            [
                ("tenant", "text"),
                ("ts", "bigint"),
                ("payload", "blob"),
                ("tags", "set<text>")
            ]
        );
        assert_eq!(e.meta.columns[1].role, KeyRole::Clustering(Order::Desc));
    }

    #[test]
    fn default_table_name_is_snake_case() {
        let e = parse("struct UserSession { #[cqlx(primary_key)] id: uuid::Uuid }").unwrap();
        assert_eq!(e.meta.table, "user_session");
        assert_eq!(e.meta.columns[0].cql_type, "uuid");
    }

    #[test]
    fn rejects_missing_partition_key() {
        assert!(parse("struct A { x: i32 }").is_err());
    }

    #[test]
    fn rejects_unknown_type_without_override() {
        assert!(parse("struct A { #[cqlx(partition_key)] x: Foo }").is_err());
        assert!(
            parse(r#"struct A { #[cqlx(partition_key, cql_type = "frozen<foo>")] x: Foo }"#)
                .is_ok()
        );
    }
}
