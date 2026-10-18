use cqlx::scylla::DeserializeRow;
use cqlx::{CqlEntity, Session};
use cqlx::scylla::value::Counter;

#[derive(CqlEntity, DeserializeRow)]
struct Hits {
    #[cqlx(partition_key)]
    page: String,
    views: Counter,
}

async fn go(session: &Session, h: &Hits) {
    // counter tables only support UPDATE, so `Insertable` is not implemented
    let _ = session.insert(h).await;
}

fn main() {}
