use cqlx::scylla::DeserializeRow;
use cqlx::{CqlEntity, CqlModel};

#[derive(CqlEntity, DeserializeRow)]
struct Event {
    #[cqlx(partition_key)]
    tenant: String,
}

fn main() {
    let _ = Event::query().tenant("a".into()).tenant("b".into());
}
