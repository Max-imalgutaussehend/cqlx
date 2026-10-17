//! `#[derive(CqlEntity)]`: table metadata, insert plumbing and a type-state query builder.

use cqlx_core::meta::{KeyRole, Order};
use cqlx_core::parse::{ParsedEntity, parse_entity};
use proc_macro::TokenStream;
use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::{format_ident, quote};
use syn::{DeriveInput, Ident, LitStr, parse_macro_input};

/// Derives `cqlx::CqlModel` and a `<Name>Query` type-state builder.
///
/// See the `cqlx` crate docs for the attribute reference.
#[proc_macro_derive(CqlEntity, attributes(cqlx))]
pub fn derive_cql_entity(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match parse_entity(&input) {
        Ok(e) => expand(&e).into(),
        Err(err) => err.to_compile_error().into(),
    }
}

fn lit(s: &str) -> LitStr {
    LitStr::new(s, Span::call_site())
}

fn expand(e: &ParsedEntity) -> TokenStream2 {
    let name = &e.ident;
    let meta = &e.meta;
    let qname = meta.qualified_name();
    let builder = format_ident!("{}Query", name);

    // Compile-time rendered statements.
    let select_list = meta
        .columns
        .iter()
        .map(|c| {
            if c.name == c.field {
                c.name.clone()
            } else {
                format!("{} AS {}", c.name, c.field)
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    let col_list = meta
        .columns
        .iter()
        .map(|c| c.name.as_str())
        .collect::<Vec<_>>()
        .join(", ");
    let marks = vec!["?"; meta.columns.len()].join(", ");
    let insert_cql = format!("INSERT INTO {qname} ({col_list}) VALUES ({marks})");
    let create_table = meta.create_table_cql();
    let (q_name, q_select, q_insert, q_create) = (
        lit(&qname),
        lit(&select_list),
        lit(&insert_cql),
        lit(&create_table),
    );
    let create_ks = match meta.create_keyspace_cql() {
        Some(s) => {
            let l = lit(&s);
            quote!(::core::option::Option::Some(#l))
        }
        None => quote!(::core::option::Option::None),
    };

    let part: Vec<_> = e
        .fields
        .iter()
        .filter(|f| f.column.role == KeyRole::Partition)
        .collect();
    let clus: Vec<_> = e
        .fields
        .iter()
        .filter(|f| matches!(f.column.role, KeyRole::Clustering(_)))
        .collect();
    let part_names: Vec<LitStr> = part.iter().map(|f| lit(&f.column.name)).collect();
    let clus_names: Vec<LitStr> = clus.iter().map(|f| lit(&f.column.name)).collect();
    let n = part.len();

    let params: Vec<Ident> = (0..n).map(|i| format_ident!("P{}", i)).collect();
    let missing = quote!(::cqlx::Missing);
    let provided = quote!(::cqlx::Provided);

    let all_missing: Vec<_> = (0..n).map(|_| missing.clone()).collect();
    let all_provided: Vec<_> = (0..n).map(|_| provided.clone()).collect();

    // One impl block per partition key: only callable while that key is `Missing`.
    let setters = part.iter().enumerate().map(|(i, f)| {
        let method = &f.ident;
        let ty = &f.ty;
        let col = lit(&f.column.name);
        let generics: Vec<_> = params
            .iter()
            .enumerate()
            .filter(|(j, _)| *j != i)
            .map(|(_, p)| p)
            .collect();
        let from: Vec<_> = (0..n)
            .map(|j| {
                if j == i {
                    missing.clone()
                } else {
                    let p = &params[j];
                    quote!(#p)
                }
            })
            .collect();
        let to: Vec<_> = (0..n)
            .map(|j| {
                if j == i {
                    provided.clone()
                } else {
                    let p = &params[j];
                    quote!(#p)
                }
            })
            .collect();
        let doc = format!(
            "Sets the partition key column `{}` (`=`). Required before the query can run.",
            f.column.name
        );
        quote! {
            impl<#(#generics),*> #builder<#(#from),*> {
                #[doc = #doc]
                pub fn #method(self, value: #ty) -> #builder<#(#to),*> {
                    #builder {
                        state: self.state.restrict(#col, ::cqlx::Op::Eq, value),
                        _states: ::core::marker::PhantomData,
                    }
                }
            }
        }
    });

    let clustering_methods = clus.iter().map(|f| {
        let ty = &f.ty;
        let col = lit(&f.column.name);
        let base = f.ident.to_string();
        let mk = |suffix: &str, op: TokenStream2, doc: &str| {
            let m = format_ident!("{}_{}", base, suffix);
            let doc = format!("Restricts clustering column `{}` ({}).", f.column.name, doc);
            quote! {
                #[doc = #doc]
                pub fn #m(self, value: #ty) -> Self {
                    Self { state: self.state.restrict(#col, #op, value), _states: ::core::marker::PhantomData }
                }
            }
        };
        let eq = mk("eq", quote!(::cqlx::Op::Eq), "`=`");
        let gt = mk("gt", quote!(::cqlx::Op::Gt), "`>`");
        let ge = mk("ge", quote!(::cqlx::Op::Ge), "`>=`");
        let lt = mk("lt", quote!(::cqlx::Op::Lt), "`<`");
        let le = mk("le", quote!(::cqlx::Op::Le), "`<=`");
        quote!(#eq #gt #ge #lt #le)
    });

    let pushes = e.fields.iter().map(|f| {
        let id = &f.ident;
        quote!(v.push(&self.#id);)
    });
    let col_lits = meta.columns.iter().map(|c| {
        let (n, f, t) = (lit(&c.name), lit(&c.field), lit(&c.cql_type));
        let role = match c.role {
            KeyRole::Partition => quote!(::cqlx::KeyRole::Partition),
            KeyRole::Regular => quote!(::cqlx::KeyRole::Regular),
            KeyRole::Clustering(Order::Asc) => quote!(::cqlx::KeyRole::Clustering(::cqlx::Order::Asc)),
            KeyRole::Clustering(Order::Desc) => quote!(::cqlx::KeyRole::Clustering(::cqlx::Order::Desc)),
        };
        quote!(::cqlx::ColumnMeta { name: #n.into(), field: #f.into(), cql_type: #t.into(), role: #role })
    });
    let keyspace = match &meta.keyspace {
        Some(k) => {
            let k = lit(k);
            quote!(::core::option::Option::Some(#k.into()))
        }
        None => quote!(::core::option::Option::None),
    };
    let table = lit(&meta.table);
    let builder_doc = format!(
        "Type-state query builder for [`{name}`]. `fetch`/`build`/`delete` only exist once every partition key is provided."
    );

    quote! {
        #[doc = #builder_doc]
        pub struct #builder<#(#params),*> {
            state: ::cqlx::QueryState,
            _states: ::core::marker::PhantomData<fn() -> (#(#params,)*)>,
        }

        impl #builder<#(#all_missing),*> {
            #[doc(hidden)]
            pub fn __new() -> Self {
                Self {
                    state: ::cqlx::QueryState::new(::cqlx::Shape {
                        table: #q_name,
                        select_list: #q_select,
                        partition: &[#(#part_names),*],
                        clustering: &[#(#clus_names),*],
                    }),
                    _states: ::core::marker::PhantomData,
                }
            }
        }

        #(#setters)*

        impl #builder<#(#all_provided),*> {
            #(#clustering_methods)*

            /// Limits the number of returned rows.
            pub fn limit(self, n: i32) -> Self {
                Self { state: self.state.limit(n), _states: ::core::marker::PhantomData }
            }

            /// Renders the `SELECT` statement and its bind values.
            pub fn build(self) -> ::cqlx::Result<::cqlx::Built> {
                self.state.build_select()
            }

            /// Renders a `DELETE` for the restricted partition/rows.
            pub fn build_delete(self) -> ::cqlx::Result<::cqlx::Built> {
                self.state.build_delete()
            }

            /// Runs the query and decodes all rows.
            pub async fn fetch(self, session: &::cqlx::Session) -> ::cqlx::Result<::std::vec::Vec<#name>> {
                session.fetch::<#name>(self.state.build_select()?).await
            }

            /// Runs the query with `LIMIT 1` and returns the first row, if any.
            pub async fn first(self, session: &::cqlx::Session) -> ::cqlx::Result<::core::option::Option<#name>> {
                Ok(session.fetch::<#name>(self.state.limit(1).build_select()?).await?.into_iter().next())
            }

            /// Deletes the restricted partition/rows.
            pub async fn delete(self, session: &::cqlx::Session) -> ::cqlx::Result<()> {
                session.run(self.state.build_delete()?).await
            }
        }

        impl ::cqlx::CqlModel for #name {
            type Query = #builder<#(#all_missing),*>;

            const TABLE: &'static str = #q_name;
            const INSERT_CQL: &'static str = #q_insert;
            const CREATE_TABLE_CQL: &'static str = #q_create;
            const CREATE_KEYSPACE_CQL: ::core::option::Option<&'static str> = #create_ks;

            fn query() -> Self::Query {
                #builder::__new()
            }

            fn insert_values(&self) -> ::cqlx::BoundValues<'_> {
                let mut v = ::cqlx::BoundValues::new();
                #(#pushes)*
                v
            }

            fn meta() -> &'static ::cqlx::EntityMeta {
                static META: ::std::sync::OnceLock<::cqlx::EntityMeta> = ::std::sync::OnceLock::new();
                META.get_or_init(|| ::cqlx::EntityMeta {
                    keyspace: #keyspace,
                    table: #table.into(),
                    columns: ::std::vec![#(#col_lits),*],
                })
            }
        }
    }
}
