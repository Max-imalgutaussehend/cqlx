use cqlx::scylla::DeserializeRow;
use cqlx::CqlEntity;

#[derive(CqlEntity, DeserializeRow)]
struct Event {
    id: i32,
}

fn main() {}
