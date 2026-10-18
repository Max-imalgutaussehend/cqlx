use cqlx::scylla::DeserializeRow;
use cqlx::CqlEntity;

#[derive(CqlEntity, DeserializeRow)]
struct Item {
    #[cqlx(partition_key)]
    id: i32,
    #[cqlx(clustering_key)]
    seq: i32,
    note: String,
}

fn main() {
    // `seq` is part of the primary key and was never provided: `build` does not exist.
    let _ = Item::update().id(1).set_note("x".into()).build();
}
