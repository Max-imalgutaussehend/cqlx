use cqlx::scylla::DeserializeRow;
use cqlx::CqlEntity;
use cqlx::scylla::value::Counter;

#[derive(CqlEntity, DeserializeRow)]
struct Bad {
    #[cqlx(partition_key)]
    page: String,
    views: Counter,
    title: String,
}

fn main() {}
