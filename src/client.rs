use std::{
    collections::HashMap,
    net::TcpStream,
    ops::{Deref, DerefMut},
    str::FromStr,
    sync::{Arc, Mutex},
    thread,
};

use crossbeam::channel::{unbounded, Receiver, Sender};
use serde::Serialize;
use serde_json::Value;
use uuid::Uuid;
use websocket::{
    sync::{client, Reader, Writer},
    OwnedMessage,
};

use crate::shared::{KVPair, Query, QueryType, Response};

pub struct LVBClient {
    sender: Arc<Mutex<Writer<TcpStream>>>,
    callbacks: CBMap,
}

pub struct RespWaiter {
    pub rx: Receiver<Vec<KVPair>>,
    pub query_id: String,
    pub callbacks: CBMap,
    pub sender: Arc<Mutex<Writer<TcpStream>>>,
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

        LVBClient {
            sender: Arc::new(Mutex::new(sender)),
            callbacks,
        }
    }

    pub fn insert<T: Serialize>(&self, key: &str, value: T) {
        let query_id = Uuid::new_v4();

        let json_str = serde_json::to_string(&value).unwrap();
        let value = Value::from_str(&json_str).unwrap();

        let query = Query {
            query_type: QueryType::INSERT(key.into(), value),
            query_id: query_id.to_string(),
        };

        let query_str = serde_json::to_string(&query).unwrap();

        self.sender
            .lock()
            .unwrap()
            .send_message(&OwnedMessage::Text(query_str))
            .unwrap();
    }

    pub fn get(&self, search: &str) -> RespWaiter {
        let (sx, rx) = unbounded();

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
            .lock()
            .unwrap()
            .send_message(&OwnedMessage::Text(query_str))
            .unwrap();
        RespWaiter {
            rx,
            query_id: query_id.to_string(),
            callbacks: self.callbacks.clone(),
            sender: self.sender.clone(),
        }
    }

    pub fn watch(&self, search: &str) -> RespWaiter {
        let (sx, rx) = unbounded();

        let query_id = Uuid::new_v4();

        self.callbacks
            .lock()
            .unwrap()
            .insert(query_id.to_string(), (true, sx));

        let query = Query {
            query_type: QueryType::WATCH(search.into()),
            query_id: query_id.to_string(),
        };

        let query_str = serde_json::to_string(&query).unwrap();

        self.sender
            .lock()
            .unwrap()
            .send_message(&OwnedMessage::Text(query_str))
            .unwrap();

        RespWaiter {
            rx,
            query_id: query_id.to_string(),
            callbacks: self.callbacks.clone(),
            sender: self.sender.clone(),
        }
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

    let _ = callbacks.lock().unwrap().drain().collect::<Vec<_>>();
}

#[test]
fn insert_test() {
    let client = LVBClient::new("jensogkarsten.site");

    client.insert(
        "user-1234",
        serde_json::json!({"name": "jens", "age": "karsten"}),
    );
}

#[test]
fn get_test() {
    let client = LVBClient::new("jensogkarsten.site");

    let rx = client.get("");

    println!("{:#?}", rx.recv().unwrap());
}

impl Deref for RespWaiter {
    type Target = Receiver<Vec<KVPair>>;

    fn deref(&self) -> &Self::Target {
        &self.rx
    }
}

impl DerefMut for RespWaiter {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.rx
    }
}

impl Drop for RespWaiter {
    fn drop(&mut self) {
        self.callbacks.lock().unwrap().remove(&self.query_id);

        let drop_msg = Query {
            query_type: QueryType::UNWATCH,
            query_id: self.query_id.clone(),
        };
        let str: String = serde_json::to_string(&drop_msg).unwrap();
        self.sender
            .lock()
            .unwrap()
            .send_message(&OwnedMessage::Text(str))
            .unwrap();
    }
}
