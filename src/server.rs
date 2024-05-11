use std::{
    collections::HashMap,
    net::TcpStream,
    path::Path,
    sync::mpsc::{channel, Receiver, Sender},
    thread,
};

use serde::de::DeserializeOwned;
use serde_json::Value;
use sled::Db;
use uuid::Uuid;
use websocket::{
    sync::{Client, Writer},
    OwnedMessage,
};

use crate::shared::{GetFn, KVPair, Query, QueryType, Response};

pub fn run(path: &Path, functions: &'static [(&'static str, fn(DBRead, Value) -> Vec<KVPair>)]) {
    let mut server = websocket::server::sync::Server::bind("0.0.0.0:3990").unwrap();

    let db = sled::open(path).unwrap();

    let (sx, rx) = channel();
    let sx_c = sx.clone();
    thread::spawn(move || server_event_handler(db, rx, sx_c, functions));

    while let Some(conn_res) = server.next() {
        let Result::Ok(conn_up) = conn_res else {
            continue;
        };
        let Result::Ok(conn) = conn_up.accept() else {
            continue;
        };
        let sx = sx.clone();
        thread::spawn(move || run_client(conn, sx));
    }
}

fn server_event_handler(
    db: Db,
    rx: Receiver<ServerEvent>,
    event_sx: Sender<ServerEvent>,
    functions: &'static [(&'static str, fn(DBRead, Value) -> Vec<KVPair>)],
) {
    let mut clients = HashMap::new();
    let mut watches = vec![];

    while let Result::Ok(event) = rx.recv() {
        match event {
            ServerEvent::ClientConnected(client_id, sx) => {
                clients.insert(client_id, sx);
            }
            ServerEvent::ClientDisconnected(client_id) => {
                clients.remove(&client_id);
                watches.retain(|(c, _, _)| *c != client_id);
            }
            ServerEvent::Query(client_id, query) => match query.query_type {
                QueryType::GET(search) => {
                    let query_res = match search {
                        GetFn::Procedure(fn_name, arg) => {
                            let Some(fn_) = functions.iter().find(|(f, _)| f == &fn_name) else {
                                eprintln!("TODO: Handle invalid function name");
                                continue;
                            };

                            fn_.1(DBRead::new(db.clone()), arg)
                        }
                        GetFn::Prefix(search) => get_query(&search, &db),
                    };

                    let Some(sx) = clients.get_mut(&client_id) else {
                        eprintln!("Failed getting sx of {client_id}");
                        continue;
                    };
                    let resp = Response {
                        query_id: query.query_id,
                        query_res,
                    };

                    let Result::Ok(resp_text) = serde_json::to_string(&resp) else {
                        eprintln!("Failed to serialize response {resp:#?}");
                        continue;
                    };
                    if let Err(_) = sx.send_message(&OwnedMessage::Text(resp_text)) {
                        clients.remove(&client_id);
                    }
                }
                QueryType::WATCH(search) => {
                    watches.push((client_id, query.query_id.clone(), search.clone()));

                    if let Err(err) = event_sx.send(ServerEvent::Query(
                        client_id,
                        Query {
                            query_type: QueryType::GET(search.clone()),
                            query_id: query.query_id,
                        },
                    )) {
                        eprintln!("Failed to self-send watch update {search:?} with: {err:?}");
                        continue;
                    }
                }
                QueryType::INSERT(key, value) => {
                    let Result::Ok(ser_json) = serde_json::to_string(&value) else {
                        eprintln!("Failed to serialize {value:#?}");
                        continue;
                    };
                    if let Err(insert_err) = db.insert(&key, ser_json.as_str()) {
                        eprintln!("Failed to insert {key}:{ser_json} into db: {insert_err:?}");
                        continue;
                    }
                    for (client_id, id, search) in &watches {
                        if let GetFn::Procedure(search, _) = search {
                            if !search.starts_with(&key) {
                                continue;
                            }
                        }

                        if let Err(err) = event_sx.send(ServerEvent::Query(
                            *client_id,
                            Query {
                                query_type: QueryType::GET(search.to_owned()),
                                query_id: id.to_owned(),
                            },
                        )) {
                            eprintln!("Failed to self-send watch update {search:?} with: {err:?}");
                            continue;
                        }
                    }
                }
                QueryType::UNWATCH => watches.retain(|(_, q, _)| q != &query.query_id),
            },
        }
    }
}

fn get_query(search: &str, db: &Db) -> Vec<KVPair> {
    let mut res = vec![];
    for entry in db.scan_prefix(search) {
        let Result::Ok((key, value)) = entry else {
            eprintln!("Failed fetching {search} prefixed item from db");
            continue;
        };
        let Result::Ok(key) = String::from_utf8(key.to_vec()) else {
            eprintln!("Failed converting db key {key:?} to string");
            continue;
        };
        let Result::Ok(json_str) = String::from_utf8(value.to_vec()) else {
            eprintln!("Failed converting db value {value:?} to string");
            continue;
        };
        let Result::Ok(value) = serde_json::from_str(&json_str) else {
            eprintln!("Failed to parse {json_str} to json value");
            continue;
        };

        res.push(KVPair { key, value });
    }

    res
}

type ClientID = Uuid;
enum ServerEvent {
    ClientConnected(ClientID, Writer<TcpStream>),
    ClientDisconnected(ClientID),
    Query(ClientID, Query),
}

fn run_client(client: Client<TcpStream>, event_sx: Sender<ServerEvent>) {
    let Result::Ok((mut rx, sx)) = client.split() else {
        eprintln!("Failed to split client..");
        return;
    };

    let client_id = Uuid::new_v4();

    event_sx
        .send(ServerEvent::ClientConnected(client_id, sx))
        .unwrap();

    while let Result::Ok(msg) = rx.recv_message() {
        match msg {
            websocket::OwnedMessage::Text(json_text) => {
                let Result::Ok(query) = serde_json::from_str::<Query>(&json_text) else {
                    eprintln!("Failed to parse query: {json_text}");
                    continue;
                };
                if let Err(send_error) = event_sx.send(ServerEvent::Query(client_id, query)) {
                    eprintln!("{client_id} failed to post query event with err: {send_error}");
                }
            }
            websocket::OwnedMessage::Binary(_) => todo!(),
            websocket::OwnedMessage::Close(_) => {
                if let Err(send_error) = event_sx.send(ServerEvent::ClientDisconnected(client_id)) {
                    eprintln!("{client_id} failed to post disconnect event with err: {send_error}");
                }
                return;
            }
            websocket::OwnedMessage::Ping(_) => todo!(),
            websocket::OwnedMessage::Pong(_) => todo!(),
        };
    }
    if let Err(err) = event_sx.send(ServerEvent::ClientDisconnected(client_id)) {
        eprintln!("Failed to post disconnect event: {err:#?}");
    }
}

pub struct DBRead {
    db: Db,
}

impl DBRead {
    fn new(db: Db) -> Self {
        Self { db }
    }

    pub fn get<T: DeserializeOwned>(&self, key: &str) -> Option<T> {
        let data = self.db.get(key).ok()??;
        let t = serde_json::from_slice(&data).ok()?;
        Some(t)
    }
    pub fn get_prefix_parsed<T: DeserializeOwned>(&self, prefix: &str) -> Vec<(String, T)> {
        self.db
            .scan_prefix(prefix)
            .filter_map(|d| d.ok())
            .filter_map(|(key, value)| {
                Some((
                    String::from_utf8(key.to_vec()).ok()?,
                    serde_json::from_slice(&value).ok()?,
                ))
            })
            .collect()
    }
    pub fn get_prefix(&self, prefix: &str) -> Vec<KVPair> {
        self.get_prefix_parsed::<Value>(prefix)
            .into_iter()
            .map(|(key, value)| KVPair { key, value })
            .collect()
    }
}

#[test]
fn insert_test() {
    use serde_json::json;
    let mut client = websocket::ClientBuilder::from_url(&"ws://0.0.0.0:3990".parse().unwrap())
        .connect(None)
        .unwrap();

    client
        .send_message(&OwnedMessage::Text(
            serde_json::to_string(&Query {
                query_type: QueryType::INSERT(
                    "user-1".into(),
                    json!({"name" : "thor", "jens": "karsten"}),
                ),
                query_id: Uuid::new_v4().to_string(),
            })
            .unwrap(),
        ))
        .unwrap();
}
#[test]
fn read_all_test() {
    let mut client = websocket::ClientBuilder::from_url(&"ws://0.0.0.0:3990".parse().unwrap())
        .connect(None)
        .unwrap();

    client
        .send_message(&OwnedMessage::Text(
            serde_json::to_string(&Query {
                query_type: QueryType::GET(GetFn::Prefix("".into())),
                query_id: Uuid::new_v4().to_string(),
            })
            .unwrap(),
        ))
        .unwrap();
}
