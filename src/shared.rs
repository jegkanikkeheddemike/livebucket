use serde_json::Value;

#[derive(Debug,Clone, serde::Deserialize, serde::Serialize)]
pub enum GetFn {
    Procedure(String, Value),
    Prefix(String),
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub enum QueryType {
    GET(GetFn),
    WATCH(GetFn),
    UNWATCH,
    INSERT(String, Value),
}
#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct Query {
    pub query_type: QueryType,
    pub query_id: String,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct Response {
    pub query_id: String,
    pub query_res: Vec<KVPair>,
}
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct KVPair {
    pub key: String,
    pub value: Value,
}
