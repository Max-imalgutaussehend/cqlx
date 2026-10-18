use cqlx::scylla::DeserializeRow;
use cqlx::CqlEntity;

#[derive(CqlEntity, DeserializeRow)]
struct Item {
    #[cqlx(partition_key)]
    id: i32,
    note: String,
}

fn main() {
    // an UPDATE without any assignment cannot be built
    let _ = Item::update().id(1).build();
}
