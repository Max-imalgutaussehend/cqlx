//! `#[derive(CqlEntity)]`: table metadata, insert/delete plumbing, a type-state query builder and a type-state
//! `UPDATE` builder.

use cqlx_core::meta::{KeyRole, Order};
use cqlx_core::parse::{ParsedEntity, ParsedField, parse_entity};
use proc_macro::TokenStream;
use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::{format_ident, quote};
use syn::{DeriveInput, Ident, LitStr, Type, parse_macro_input};

/// Derives `cqlx::CqlModel` and the `<Name>Query` builder, plus an `update()` builder on the struct.
///
/// See the `cqlx` crate docs for the attribute reference.
#[proc_macro_derive(CqlEntity, attributes(cqlx))]
pub fn derive_cql_entity(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match parse_entity(&input).and_then(|e| expand(&e)) {
        Ok(ts) => ts.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

fn lit(s: &str) -> LitStr {
    LitStr::new(s, Span::call_site())
}

/// `Option<T>` -> `T`, anything else unchanged.
fn inner_type(ty: &Type) -> (&Type, bool) {
    if let Type::Path(p) = ty
        && let Some(seg) = p.path.segments.last()
        && seg.ident == "Option"
        && let syn::PathArguments::AngleBracketed(a) = &seg.arguments
        && let Some(syn::GenericArgument::Type(t)) = a.args.first()
    {
        return (t, true);
    }
    (ty, false)
}

fn is_collection(cql: &str) -> bool {
    cql.starts_with("list<") || cql.starts_with("set<") || cql.starts_with("map<")
}

/// Type-parameter lists: `params[j]` everywhere except position `at`, which becomes `replacement`.
fn with_state(
    params: &[Ident],
    at: Option<usize>,
    replacement: &TokenStream2,
) -> Vec<TokenStream2> {
    params
        .iter()
        .enumerate()
        .map(|(j, p)| {
            if Some(j) == at {
                replacement.clone()
            } else {
                quote!(#p)
            }
        })
        .collect()
}

fn expand(e: &ParsedEntity) -> syn::Result<TokenStream2> {
    let name = &e.ident;
    let meta = &e.meta;
    let qname = meta.qualified_name();
    let query = format_ident!("{}Query", name);
    let update = format_ident!("{}Update", name);

    let counters: Vec<&ParsedField> = e
        .fields
        .iter()
        .filter(|f| f.column.cql_type == "counter")
        .collect();
    let has_counter = !counters.is_empty();
    if has_counter
        && let Some(f) = e
            .fields
            .iter()
            .find(|f| f.column.role == KeyRole::Regular && f.column.cql_type != "counter")
    {
        return Err(syn::Error::new_spanned(
            &f.ident,
            "a table with a counter column can only have counter columns besides its primary key",
        ));
    }

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

    let part: Vec<&ParsedField> = e
        .fields
        .iter()
        .filter(|f| f.column.role == KeyRole::Partition)
        .collect();
    let clus: Vec<&ParsedField> = e
        .fields
        .iter()
        .filter(|f| matches!(f.column.role, KeyRole::Clustering(_)))
        .collect();
    let regular: Vec<&ParsedField> = e
        .fields
        .iter()
        .filter(|f| f.column.role == KeyRole::Regular)
        .collect();
    let key_fields: Vec<&ParsedField> = part.iter().chain(clus.iter()).copied().collect();
    let delete_cql = format!(
        "DELETE FROM {qname} WHERE {}",
        key_fields
            .iter()
            .map(|f| format!("{} = ?", f.column.name))
            .collect::<Vec<_>>()
            .join(" AND ")
    );
    let (q_name, q_select, q_insert, q_delete) = (
        lit(&qname),
        lit(&select_list),
        lit(&insert_cql),
        lit(&delete_cql),
    );
    let q_create = lit(&meta.create_table_cql());
    let create_ks = match meta.create_keyspace_cql() {
        Some(s) => {
            let l = lit(&s);
            quote!(::core::option::Option::Some(#l))
        }
        None => quote!(::core::option::Option::None),
    };

    let part_names: Vec<LitStr> = part.iter().map(|f| lit(&f.column.name)).collect();
    let clus_names: Vec<LitStr> = clus.iter().map(|f| lit(&f.column.name)).collect();
    let clus_desc: Vec<bool> = clus
        .iter()
        .map(|f| matches!(f.column.role, KeyRole::Clustering(Order::Desc)))
        .collect();

    let missing = quote!(::cqlx::Missing);
    let provided = quote!(::cqlx::Provided);
    let nofilter = quote!(::cqlx::NoFilter);
    let filtering = quote!(::cqlx::Filtering);

    // ---------------------------------------------------------------- query builder
    let n = part.len();
    let qp: Vec<Ident> = (0..n).map(|i| format_ident!("P{}", i)).collect();
    let all_missing: Vec<TokenStream2> = (0..n).map(|_| missing.clone()).collect();
    let all_provided: Vec<TokenStream2> = (0..n).map(|_| provided.clone()).collect();

    let ctor = quote! { #query { state: ::cqlx::QueryState::new(::cqlx::Shape {
        table: #q_name, select_list: #q_select,
        partition: &[#(#part_names),*], clustering: &[#(#clus_names),*], clustering_desc: &[#(#clus_desc),*],
    }), _states: ::core::marker::PhantomData } };

    let part_setters = part.iter().enumerate().map(|(i, f)| {
        let method = &f.ident;
        let method_in = format_ident!("{}_in", f.ident);
        let ty = &f.ty;
        let col = lit(&f.column.name);
        let generics: Vec<_> = qp.iter().enumerate().filter(|(j, _)| *j != i).map(|(_, p)| p).collect();
        let from = with_state(&qp, Some(i), &missing);
        let to = with_state(&qp, Some(i), &provided);
        let doc = format!("Sets the partition key column `{}` (`=`). Required before the query can run.", f.column.name);
        let doc_in = format!("Restricts the partition key column `{}` with `IN` (a multi-partition read).", f.column.name);
        quote! {
            impl<#(#generics,)* F> #query<#(#from,)* F> {
                #[doc = #doc]
                pub fn #method(self, value: #ty) -> #query<#(#to,)* F> {
                    #query { state: self.state.restrict(#col, ::cqlx::Op::Eq, value), _states: ::core::marker::PhantomData }
                }
                #[doc = #doc_in]
                pub fn #method_in(self, values: ::std::vec::Vec<#ty>) -> #query<#(#to,)* F> {
                    #query { state: self.state.restrict(#col, ::cqlx::Op::In, values), _states: ::core::marker::PhantomData }
                }
            }
        }
    });

    let clustering_methods = clus.iter().map(|f| {
        let ty = &f.ty;
        let col = lit(&f.column.name);
        let base = f.ident.to_string();
        let mk = |suffix: &str, op: TokenStream2, doc: &str, param: TokenStream2, ty: TokenStream2| {
            let m = format_ident!("{}_{}", base, suffix);
            let doc = format!("Restricts clustering column `{}` ({}).", f.column.name, doc);
            let _ = &param;
            quote! {
                #[doc = #doc]
                pub fn #m(self, value: #ty) -> Self {
                    Self { state: self.state.restrict(#col, #op, value), _states: ::core::marker::PhantomData }
                }
            }
        };
        let t = quote!(#ty);
        let eq = mk("eq", quote!(::cqlx::Op::Eq), "`=`", quote!(), t.clone());
        let gt = mk("gt", quote!(::cqlx::Op::Gt), "`>`", quote!(), t.clone());
        let ge = mk("ge", quote!(::cqlx::Op::Ge), "`>=`", quote!(), t.clone());
        let lt = mk("lt", quote!(::cqlx::Op::Lt), "`<`", quote!(), t.clone());
        let le = mk("le", quote!(::cqlx::Op::Le), "`<=`", quote!(), t.clone());
        let inn = mk("in", quote!(::cqlx::Op::In), "`IN`", quote!(), quote!(::std::vec::Vec<#ty>));
        quote!(#eq #gt #ge #lt #le #inn)
    });

    let filter_methods = regular.iter().filter(|f| !is_collection(&f.column.cql_type) && f.column.cql_type != "counter").map(|f| {
        let (ty, _) = inner_type(&f.ty);
        let col = lit(&f.column.name);
        let base = f.ident.to_string();
        let mk = |suffix: &str, op: TokenStream2, doc: &str| {
            let m = format_ident!("where_{}_{}", base, suffix);
            let doc = format!("Filters on the non-key column `{}` ({}). Needs `allow_filtering()`: Cassandra scans and discards rows.", f.column.name, doc);
            quote! {
                #[doc = #doc]
                pub fn #m(self, value: #ty) -> Self {
                    Self { state: self.state.filter(#col, #op, value), _states: ::core::marker::PhantomData }
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

    // ---------------------------------------------------------------- update builder
    let k = key_fields.len();
    let up: Vec<Ident> = (0..k).map(|i| format_ident!("K{}", i)).collect();
    let up_missing: Vec<TokenStream2> = (0..k).map(|_| missing.clone()).collect();
    let up_provided: Vec<TokenStream2> = (0..k).map(|_| provided.clone()).collect();
    let no_assign = quote!(::cqlx::NoAssignment);
    let has_assign = quote!(::cqlx::HasAssignment);

    let key_setters = key_fields.iter().enumerate().map(|(i, f)| {
        let method = &f.ident;
        let ty = &f.ty;
        let col = lit(&f.column.name);
        let generics: Vec<_> = up.iter().enumerate().filter(|(j, _)| *j != i).map(|(_, p)| p).collect();
        let from = with_state(&up, Some(i), &missing);
        let to = with_state(&up, Some(i), &provided);
        let doc = format!("Sets the primary key column `{}` of the row to update.", f.column.name);
        quote! {
            impl<#(#generics,)* S> #update<#(#from,)* S> {
                #[doc = #doc]
                pub fn #method(self, value: #ty) -> #update<#(#to,)* S> {
                    #update { state: self.state.key(#col, value), _states: ::core::marker::PhantomData }
                }
            }
        }
    });

    let assignment_methods = regular.iter().map(|f| {
        let (ty, optional) = inner_type(&f.ty);
        let col = lit(&f.column.name);
        let base = f.ident.to_string();
        let cql = f.column.cql_type.as_str();
        let to_has = |body: TokenStream2| body;
        let wrap = |op: TokenStream2, value: TokenStream2| to_has(quote! {
            #update { state: self.state.set(#col, #op, #value), _states: ::core::marker::PhantomData }
        });
        if cql == "counter" {
            let inc = format_ident!("increment_{}", base);
            let dec = format_ident!("decrement_{}", base);
            let a = wrap(quote!(::cqlx::SetOp::Add), quote!(::cqlx::scylla::value::Counter(by)));
            let b = wrap(quote!(::cqlx::SetOp::Add), quote!(::cqlx::scylla::value::Counter(-by)));
            return quote! {
                /// Increments the counter by `by`.
                pub fn #inc(self, by: i64) -> #update<#(#up,)* #has_assign> { #a }
                /// Decrements the counter by `by`.
                pub fn #dec(self, by: i64) -> #update<#(#up,)* #has_assign> { #b }
            };
        }
        let set = format_ident!("set_{}", base);
        let doc = format!("Sets `{}`.", f.column.name);
        let w = wrap(quote!(::cqlx::SetOp::Set), quote!(value));
        let mut out = quote! {
            #[doc = #doc]
            pub fn #set(self, value: #ty) -> #update<#(#up,)* #has_assign> { #w }
        };
        if optional {
            let set_null = format_ident!("set_{}_null", base);
            let wn = wrap(quote!(::cqlx::SetOp::Set), quote!(::core::option::Option::<#ty>::None));
            out.extend(quote! {
                /// Writes NULL (a tombstone) into the column.
                pub fn #set_null(self) -> #update<#(#up,)* #has_assign> { #wn }
            });
        }
        if is_collection(cql) {
            let add = format_ident!("add_{}", base);
            let remove = format_ident!("remove_{}", base);
            let wa = wrap(quote!(::cqlx::SetOp::Add), quote!(value));
            let wr = wrap(quote!(::cqlx::SetOp::Remove), quote!(value));
            out.extend(quote! {
                /// Appends to the collection (`col = col + ?`) without rewriting it.
                pub fn #add(self, value: #ty) -> #update<#(#up,)* #has_assign> { #wa }
                /// Removes elements (`col = col - ?`) without rewriting the collection.
                pub fn #remove(self, value: #ty) -> #update<#(#up,)* #has_assign> { #wr }
            });
        }
        out
    });

    let condition_methods = regular.iter().filter(|f| !is_collection(&f.column.cql_type) && f.column.cql_type != "counter").map(|f| {
        let (ty, _) = inner_type(&f.ty);
        let col = lit(&f.column.name);
        let m = format_ident!("if_{}_eq", f.ident);
        let doc = format!("Lightweight transaction: only applies if `{}` currently equals `value`. Use `execute_lwt`.", f.column.name);
        quote! {
            #[doc = #doc]
            pub fn #m(self, value: #ty) -> Self {
                Self { state: self.state.condition(#col, value), _states: ::core::marker::PhantomData }
            }
        }
    });

    let update_entry = if regular.is_empty() {
        quote!()
    } else {
        quote! {
            impl #name {
                /// Starts a type-state `UPDATE`: every primary key column and at least one assignment are required
                /// before `execute` exists.
                pub fn update() -> #update<#(#up_missing,)* #no_assign> {
                    #update {
                        state: ::cqlx::UpdateState::new(::cqlx::Shape {
                            table: #q_name, select_list: #q_select,
                            partition: &[#(#part_names),*], clustering: &[#(#clus_names),*], clustering_desc: &[#(#clus_desc),*],
                        }),
                        _states: ::core::marker::PhantomData,
                    }
                }
            }

            #[doc = "Type-state `UPDATE` builder; created by `update()`."]
            pub struct #update<#(#up,)* S> {
                state: ::cqlx::UpdateState,
                _states: ::core::marker::PhantomData<fn() -> (#(#up,)* S,)>,
            }

            #(#key_setters)*

            impl<#(#up,)* S> #update<#(#up,)* S> {
                #(#assignment_methods)*
                #(#condition_methods)*

                /// `USING TTL seconds`.
                pub fn ttl(self, seconds: i32) -> Self {
                    Self { state: self.state.ttl(seconds), _states: ::core::marker::PhantomData }
                }
                /// `USING TIMESTAMP micros`.
                pub fn timestamp(self, micros: i64) -> Self {
                    Self { state: self.state.timestamp(micros), _states: ::core::marker::PhantomData }
                }
                /// Consistency level of this statement.
                pub fn consistency(self, c: ::cqlx::Consistency) -> Self {
                    Self { state: self.state.consistency(c), _states: ::core::marker::PhantomData }
                }
                /// Serial consistency level of a conditional update.
                pub fn serial_consistency(self, c: ::cqlx::SerialConsistency) -> Self {
                    Self { state: self.state.serial_consistency(c), _states: ::core::marker::PhantomData }
                }
                /// `IF EXISTS`: only update an existing row. Use `execute_lwt`.
                pub fn if_exists(self) -> Self {
                    Self { state: self.state.if_exists(), _states: ::core::marker::PhantomData }
                }
            }

            impl #update<#(#up_provided,)* #has_assign> {
                /// Renders the statement and its bind values.
                pub fn build(self) -> ::cqlx::Result<::cqlx::Built> {
                    self.state.build()
                }
                /// Runs the update.
                pub async fn execute(self, session: &::cqlx::Session) -> ::cqlx::Result<()> {
                    session.run(self.state.build()?).await
                }
                /// Runs a conditional update (`if_exists()` / `if_<field>_eq`) and returns whether it was applied.
                pub async fn execute_lwt(self, session: &::cqlx::Session) -> ::cqlx::Result<bool> {
                    session.run_lwt(self.state.build()?).await
                }
            }
        }
    };

    // ---------------------------------------------------------------- metadata and trait impl
    let pushes = e.fields.iter().map(|f| {
        let id = &f.ident;
        quote!(v.push(&self.#id);)
    });
    let key_pushes = key_fields.iter().map(|f| {
        let id = &f.ident;
        quote!(v.push(&self.#id);)
    });
    let col_lits = meta.columns.iter().map(|c| {
        let (nm, fl, ty) = (lit(&c.name), lit(&c.field), lit(&c.cql_type));
        let role = match c.role {
            KeyRole::Partition => quote!(::cqlx::KeyRole::Partition),
            KeyRole::Regular => quote!(::cqlx::KeyRole::Regular),
            KeyRole::Clustering(Order::Asc) => quote!(::cqlx::KeyRole::Clustering(::cqlx::Order::Asc)),
            KeyRole::Clustering(Order::Desc) => quote!(::cqlx::KeyRole::Clustering(::cqlx::Order::Desc)),
        };
        quote!(::cqlx::ColumnMeta { name: #nm.into(), field: #fl.into(), cql_type: #ty.into(), role: #role })
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
        "Type-state query builder for [`{name}`]. `fetch`/`first`/`stream`/`count`/`build`/`delete` only exist once every partition key is provided."
    );
    let insertable = if has_counter {
        quote!()
    } else {
        quote!(impl ::cqlx::Insertable for #name {})
    };

    Ok(quote! {
        #[doc = #builder_doc]
        pub struct #query<#(#qp,)* F = #nofilter> {
            state: ::cqlx::QueryState,
            _states: ::core::marker::PhantomData<fn() -> (#(#qp,)* F,)>,
        }

        impl #query<#(#all_missing,)* #nofilter> {
            #[doc(hidden)]
            pub fn __new() -> Self {
                #ctor
            }
        }

        #(#part_setters)*

        impl<F> #query<#(#all_provided,)* F> {
            #(#clustering_methods)*

            /// Limits the number of returned rows.
            pub fn limit(self, n: i32) -> Self {
                Self { state: self.state.limit(n), _states: ::core::marker::PhantomData }
            }

            /// Page size used when streaming or reading large results.
            pub fn page_size(self, n: i32) -> Self {
                Self { state: self.state.page_size(n), _states: ::core::marker::PhantomData }
            }

            /// Consistency level of this query.
            pub fn consistency(self, c: ::cqlx::Consistency) -> Self {
                Self { state: self.state.consistency(c), _states: ::core::marker::PhantomData }
            }

            /// Reads in the opposite of the declared clustering order (`ORDER BY ... ASC/DESC` flipped).
            pub fn reverse(self) -> Self {
                Self { state: self.state.reverse(), _states: ::core::marker::PhantomData }
            }

            /// Renders the `SELECT` statement and its bind values.
            pub fn build(self) -> ::cqlx::Result<::cqlx::Built> {
                self.state.build_select()
            }

            /// Renders a `SELECT COUNT(*)`.
            pub fn build_count(self) -> ::cqlx::Result<::cqlx::Built> {
                self.state.build_count()
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

            /// Streams the rows page by page.
            pub async fn stream(
                self,
                session: &::cqlx::Session,
            ) -> ::cqlx::Result<impl ::cqlx::futures::Stream<Item = ::cqlx::Result<#name>>> {
                session.stream::<#name>(self.state.build_select()?).await
            }

            /// `SELECT COUNT(*)` over the restricted rows.
            pub async fn count(self, session: &::cqlx::Session) -> ::cqlx::Result<i64> {
                session.count(self.state.build_count()?).await
            }

            /// Deletes the restricted partition/rows.
            pub async fn delete(self, session: &::cqlx::Session) -> ::cqlx::Result<()> {
                session.run(self.state.build_delete()?).await
            }
        }

        impl #query<#(#all_provided,)* #nofilter> {
            /// Opts in to `ALLOW FILTERING`, which unlocks `where_<column>_*` on non-key columns. Cassandra reads
            /// and discards rows for these, so use it on small partitions only.
            pub fn allow_filtering(self) -> #query<#(#all_provided,)* #filtering> {
                #query { state: self.state.allow_filtering(), _states: ::core::marker::PhantomData }
            }
        }

        impl #query<#(#all_provided,)* #filtering> {
            #(#filter_methods)*
        }

        #update_entry

        #insertable

        impl ::cqlx::CqlModel for #name {
            type Query = #query<#(#all_missing,)* #nofilter>;

            const TABLE: &'static str = #q_name;
            const INSERT_CQL: &'static str = #q_insert;
            const CREATE_TABLE_CQL: &'static str = #q_create;
            const CREATE_KEYSPACE_CQL: ::core::option::Option<&'static str> = #create_ks;
            const DELETE_CQL: &'static str = #q_delete;

            fn query() -> Self::Query {
                #query::__new()
            }

            fn insert_values(&self) -> ::cqlx::BoundValues<'_> {
                let mut v = ::cqlx::BoundValues::new();
                #(#pushes)*
                v
            }

            fn key_values(&self) -> ::cqlx::BoundValues<'_> {
                let mut v = ::cqlx::BoundValues::new();
                #(#key_pushes)*
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
    })
}
