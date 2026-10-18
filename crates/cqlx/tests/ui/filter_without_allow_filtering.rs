use cqlx::scylla::DeserializeRow;
use cqlx::{CqlEntity, CqlModel};

#[derive(CqlEntity, DeserializeRow)]
struct Item {
    #[cqlx(partition_key)]
    id: i32,
    note: String,
}

fn main() {
    // non-key filters only exist after the explicit `allow_filtering()` opt-in
    let _ = Item::query().id(1).where_note_eq("x".into());
}
