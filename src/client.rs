use std::{
    collections::HashMap,
    net::TcpStream,
    str::FromStr,
    sync::{
        mpsc::{channel, Receiver, Sender},
        Arc, Mutex,
    },
    thread,
};

use serde::Serialize;
use serde_json::Value;
use uuid::Uuid;
use websocket::{
    sync::{client, Reader, Writer},
    OwnedMessage,
};

use crate::shared::{KVPair, Query, QueryType, Response};

pub struct LVBClient {
    sender: Writer<TcpStream>,
    callbacks: CBMap,
}

type CBMap = Arc<Mutex<HashMap<String, (bool, Sender<Vec<KVPair>>)>>>;

impl LVBClient {
    pub fn new(addr: &str) -> Self {
        let addr = format!("ws://{addr}:3990");

        let client = client::ClientBuilder::new(&addr)
            .unwrap()
            .connect_insecure()
            .unwrap();

        let (reader, sender) = client.split().unwrap();

        let callbacks = Arc::new(Mutex::new(HashMap::new()));
        let callbacks2 = callbacks.clone();
        thread::spawn(move || run_socket(reader, callbacks2));

        LVBClient { sender, callbacks }
    }

    pub fn insert<T: Serialize>(&mut self, key: &str, value: T) {
        let query_id = Uuid::new_v4();

        let json_str = serde_json::to_string(&value).unwrap();
        let value = Value::from_str(&json_str).unwrap();

        let query = Query {
            query_type: QueryType::INSERT(key.into(), value),
            query_id: query_id.to_string(),
        };

        let query_str = serde_json::to_string(&query).unwrap();

        self.sender
            .send_message(&OwnedMessage::Text(query_str))
            .unwrap();
    }

    pub fn get(&mut self, search: &str) -> Receiver<Vec<KVPair>> {
        let (sx, rx) = channel();

        let query_id = Uuid::new_v4();

        self.callbacks
            .lock()
            .unwrap()
            .insert(query_id.to_string(), (false, sx));

        let query = Query {
            query_type: QueryType::GET(search.into()),
            query_id: query_id.to_string(),
        };

        let query_str = serde_json::to_string(&query).unwrap();

        self.sender
            .send_message(&OwnedMessage::Text(query_str))
            .unwrap();

        rx
    }

    pub fn watch(&mut self, search: &str) -> Receiver<Vec<KVPair>> {
        let (sx, rx) = channel();

        let query_id = Uuid::new_v4();

        self.callbacks
            .lock()
            .unwrap()
            .insert(query_id.to_string(), (false, sx));

        let query = Query {
            query_type: QueryType::WATCH(search.into()),
            query_id: query_id.to_string(),
        };

        let query_str = serde_json::to_string(&query).unwrap();

        self.sender
            .send_message(&OwnedMessage::Text(query_str))
            .unwrap();

        rx
    }
}

fn run_socket(mut reader: Reader<TcpStream>, callbacks: CBMap) {
    while let Result::Ok(msg) = reader.recv_message() {
        match msg {
            websocket::OwnedMessage::Binary(_) => todo!(),
            websocket::OwnedMessage::Close(_) => todo!(),
            websocket::OwnedMessage::Ping(_) => todo!(),
            websocket::OwnedMessage::Pong(_) => todo!(),
            websocket::OwnedMessage::Text(json_str) => {
                let Result::Ok(response) = serde_json::from_str::<Response>(&json_str) else {
                    eprintln!("Failed to parse json {json_str}");
                    continue;
                };

                let mut cb_lock = callbacks.lock().unwrap();

                if let Some((persist, sx)) = cb_lock.get_mut(&response.query_id) {
                    let mut persist = *persist;

                    if let Err(err) = sx.send(response.query_res) {
                        eprintln!("Failed to send response {} err: {err:?}", response.query_id);
                        persist = false;
                    }

                    if !persist {
                        cb_lock.remove(&response.query_id);
                    }
                }
            }
        }
    }
}

#[test]
fn insert_test() {
    let mut client = LVBClient::new("0.0.0.0");

    client.insert(
        "user-123",
        serde_json::json!({"name": "jens", "age": "karsten"}),
    );
}

#[test]
fn get_test() {
    let mut client = LVBClient::new("0.0.0.0");

    let rx = client.get("");

    println!("{:#?}", rx.recv().unwrap());
}

#[test]
fn watch_test() {
    let mut client = LVBClient::new("0.0.0.0");

    let random_key = Uuid::new_v4();

    let rx = client.watch(random_key.to_string().as_str());

    client.insert(random_key.to_string().as_str(), "123");

    for v in rx.iter() {
        println!("{v:#?}");
    }
}
