use std::path::Path;

use crate::server::DBRead;
use serde_json::Value;
use shared::KVPair;
use uuid::Uuid;

pub mod client;
pub mod server;
pub mod shared;
fn main() {
    server::run(Path::new("./data"), &[("get_random", get_random)]);
}

fn get_random(db: DBRead, _: Value) -> Vec<KVPair> {
    db.get_prefix("")
        .into_iter()
        .filter(|_| Uuid::new_v4() > Uuid::new_v4())
        .collect()
}
