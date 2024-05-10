use std::path::Path;

pub mod client;
mod server;
mod shared;
fn main() {
    server::run(Path::new("./data"));
}
