use cqlx::scylla::DeserializeRow;
use cqlx::{CqlEntity, CqlModel};

#[derive(CqlEntity, DeserializeRow)]
struct Event {
    #[cqlx(partition_key)]
    tenant: String,
    #[cqlx(partition_key)]
    day: i32,
    body: String,
}

fn main() {
    // `day` was never provided: `build` does not exist on `EventQuery<Provided, Missing>`.
    let _ = Event::query().tenant("a".into()).build();
}
