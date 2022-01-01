use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::io;
use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

struct Node {
    id: Id,
    next_msg_id: MessageCounter,
    senders: MessageHash,
}
type MessageCounter = Arc<Mutex<i64>>;
type MessageHash = Arc<Mutex<HashMap<i64, Sender<Value>>>>;
type Id = Arc<RwLock<String>>;
const KV: &str = "lin-kv";
const ROOT: &str = "root";
const CAS_CONFLICT: i64 = 30;

async fn transact(
    next_msg_id: MessageCounter,
    txns: Value,
    sender_hash: MessageHash,
    node_id: Id,
    incoming_id: i64,
    dest: String,
) {
    let (tx, rx): (Sender<Value>, Receiver<Value>) = mpsc::channel();
    let message = send_read(node_id.clone(), next_msg_id.clone(), &sender_hash, &tx, &rx);
    match message {
        Ok(val) => {
            let mut txs_json: Vec<TxnType> = Vec::new();
            eprintln!("Inside channel {}", serde_json::to_string(&val).unwrap());
            let hash = run_transactions(&val, txns, &mut txs_json);

            let message = send_cas(
                &next_msg_id,
                node_id.clone(),
                val,
                hash,
                sender_hash,
                tx,
                rx,
            );
            reply_to_transaction(node_id, message, next_msg_id, dest, incoming_id, txs_json);
        }
        _ => todo!(),
    }
}

fn reply_to_transaction(
    node_id: Arc<RwLock<String>>,
    message: Result<Value, mpsc::RecvTimeoutError>,
    next_msg_id: Arc<Mutex<i64>>,
    dest: String,
    incoming_id: i64,
    txs_json: Vec<TxnType>,
) {
    let src = node_id.read().unwrap();
    match message {
        Ok(val) => {
            let completed = val.as_bool().unwrap();
            let mut msg_id = next_msg_id.lock().unwrap();
            *msg_id += 1;
            let wait_key = *msg_id;
            drop(msg_id);

            let reply: Reply = match completed {
                true => Reply {
                    dest: &dest,
                    src: &src,
                    body: ResponseBody::Txn {
                        msg_id: wait_key,
                        r#type: "txn_ok",
                        in_reply_to: incoming_id,
                        txn: txs_json,
                    },
                },
                _ => Reply {
                    dest: &dest,
                    src: &src,
                    body: ResponseBody::Error {
                        msg_id: wait_key,
                        r#type: "error",
                        in_reply_to: incoming_id,
                        text: "Cas Conflict",
                        code: CAS_CONFLICT,
                    },
                },
            };
            eprintln!("Sending {}", serde_json::to_string(&reply).unwrap());
            println!("{}", serde_json::to_string(&reply).unwrap());
        }

        _ => todo!(),
    }
}

fn send_cas(
    next_msg_id: &Arc<Mutex<i64>>,
    node_id: Arc<RwLock<String>>,
    val: Value,
    hash: Store,
    sender_hash: Arc<Mutex<HashMap<i64, Sender<Value>>>>,
    tx: Sender<Value>,
    rx: Receiver<Value>,
) -> Result<Value, mpsc::RecvTimeoutError> {
    let mut msg_id = next_msg_id.lock().unwrap();
    *msg_id += 1;
    let wait_key = *msg_id;
    drop(msg_id);
    let src = node_id.read().unwrap();
    let reply = Reply {
        dest: KV,
        src: &src,
        body: ResponseBody::Cas {
            r#type: "cas",
            key: ROOT,
            from: serde_json::from_value(val).unwrap(),
            to: hash,
            msg_id: wait_key,
            create_if_not_exists: false,
        },
    };
    let mut lookup = sender_hash.lock().unwrap();
    lookup.insert(wait_key, tx);
    drop(lookup);
    eprintln!("Sending {}", serde_json::to_string(&reply).unwrap());
    println!("{}", serde_json::to_string(&reply).unwrap());
    let message = rx.recv_timeout(Duration::from_secs(5));
    let mut lookup = sender_hash.lock().unwrap();
    lookup.remove(&wait_key).unwrap();
    drop(lookup);
    message
}

fn run_transactions(val: &Value, txns: Value, txs_json: &mut Vec<TxnType>) -> Store {
    let mut hash: Store = serde_json::from_value(val.to_owned()).unwrap();
    for t in txns.as_array().unwrap() {
        let txn = t.as_array().unwrap();

        match txn[0].as_str().unwrap() {
            "append" => {
                let key = txn[1].as_i64().unwrap();
                let value = txn[2].as_i64().unwrap();
                hash.0.entry(key).or_insert_with(Vec::new).push(value);
                txs_json.push(TxnType(
                    "append".to_string(),
                    key,
                    TxnAnswer::Integer(value),
                ));
            }
            "r" => {
                let key = txn[1].as_i64().unwrap();
                txs_json.push(TxnType(
                    "r".to_string(),
                    key,
                    match hash.0.contains_key(&key) {
                        true => TxnAnswer::Array(hash.0[&key].clone()),
                        _ => TxnAnswer::None,
                    },
                ));
            }
            _ => todo!(),
        }
    }
    hash
}

fn send_read(
    node_id: Arc<RwLock<String>>,
    next_msg_id: Arc<Mutex<i64>>,
    sender_hash: &Arc<Mutex<HashMap<i64, Sender<Value>>>>,
    tx: &Sender<Value>,
    rx: &Receiver<Value>,
) -> Result<Value, mpsc::RecvTimeoutError> {
    let src = node_id.read().unwrap();
    let mut msg_id = next_msg_id.lock().unwrap();
    *msg_id += 1;
    let wait_key = *msg_id;
    drop(msg_id);
    let reply = Reply {
        dest: KV,
        src: &src,
        body: ResponseBody::Read {
            msg_id: wait_key,
            r#type: "read",
            key: ROOT,
        },
    };
    let mut lookup = sender_hash.lock().unwrap();
    lookup.insert(wait_key, tx.clone());
    drop(lookup);
    eprintln!("Sending {}", serde_json::to_string(&reply).unwrap());
    println!("{}", serde_json::to_string(&reply).unwrap());

    let message = rx.recv_timeout(Duration::from_secs(5));
    let mut lookup = sender_hash.lock().unwrap();
    lookup.remove(&wait_key).unwrap();
    message
}

impl Node {
    fn new(id: String) -> Node {
        Node {
            id: Arc::new(RwLock::new(id)),
            next_msg_id: Arc::new(Mutex::new(0)),
            senders: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

#[derive(Serialize)]
enum ResponseBody<'a> {
    #[serde(rename = "body")]
    Init {
        msg_id: i64,
        r#type: &'a str,
        in_reply_to: i64,
    },
    #[serde(rename = "body")]
    Txn {
        r#type: &'a str,
        in_reply_to: i64,
        msg_id: i64,
        txn: Vec<TxnType>,
    },
    #[serde(rename = "body")]
    Read {
        r#type: &'a str,
        msg_id: i64,
        key: &'a str,
    },
    #[serde(rename = "body")]
    Cas {
        r#type: &'a str,
        key: &'a str,
        from: Store,
        to: Store,
        msg_id: i64,
        create_if_not_exists: bool,
    },
    #[serde(rename = "body")]
    Error {
        r#type: &'a str,
        in_reply_to: i64,
        code: i64,
        text: &'a str,
        msg_id: i64,
    },
}

#[derive(Serialize)]
struct TxnType(String, i64, TxnAnswer);

#[derive(Serialize)]
#[serde(untagged)]
enum TxnAnswer {
    None,
    Integer(i64),
    Array(Vec<i64>),
}

#[derive(Serialize)]
struct Reply<'a> {
    src: &'a str,
    dest: &'a str,
    #[serde(flatten)]
    body: ResponseBody<'a>,
}

#[derive(Serialize, Deserialize)]
struct Store(BTreeMap<i64, Vec<i64>>);

#[tokio::main]
async fn main() {
    let mut node: Option<Node> = None;
    loop {
        let mut input = String::new();
        match io::stdin().read_line(&mut input) {
            Err(error) => println!("error: {}", error),
            Ok(_) => {
                eprintln!("Received {}", input);
                let parsed: Value = serde_json::from_str(&input).unwrap();
                let body = &parsed["body"];
                match body["type"].as_str().unwrap() {
                    "init" => {
                        node = Some(Node::new(body["node_id"].as_str().unwrap().to_string()));

                        eprintln!("Initialized node {:?}", node.as_ref().map(|s| &s.id));
                        if let Some(s) = node.as_mut() {
                            let mut msg_id = s.next_msg_id.lock().unwrap();
                            *msg_id += 1;
                            let reply = Reply {
                                dest: parsed["src"].as_str().unwrap(),
                                src: &s.id.read().unwrap(),
                                body: ResponseBody::Init {
                                    msg_id: *msg_id,
                                    r#type: "init_ok",
                                    in_reply_to: body["msg_id"].as_i64().unwrap(),
                                },
                            };
                            eprintln!("Sending {}", serde_json::to_string(&reply).unwrap());
                            println!("{}", serde_json::to_string(&reply).unwrap());
                            *msg_id += 1;
                            let reply = Reply {
                                dest: KV,
                                src: &s.id.read().unwrap(),
                                body: ResponseBody::Cas {
                                    msg_id: *msg_id,
                                    r#type: "cas",
                                    key: ROOT,
                                    from: Store(BTreeMap::new()),
                                    to: Store(BTreeMap::new()),
                                    create_if_not_exists: true,
                                },
                            };
                            eprintln!("Sending {}", serde_json::to_string(&reply).unwrap());
                            println!("{}", serde_json::to_string(&reply).unwrap());
                        }
                    }
                    "txn" => {
                        if let Some(s) = node.as_mut() {
                            tokio::spawn(transact(
                                s.next_msg_id.clone(),
                                body["txn"].to_owned(),
                                s.senders.clone(),
                                s.id.clone(),
                                body["msg_id"].as_i64().unwrap(),
                                parsed["src"].as_str().unwrap().to_string(),
                            ));
                        }
                    }
                    "cas_ok" => {
                        let msg_id = body["in_reply_to"].as_i64().unwrap();
                        if let Some(s) = node.as_ref() {
                            let lookup = s.senders.lock().unwrap();
                            if lookup.contains_key(&msg_id) {
                                lookup[&msg_id].send(Value::Bool(true)).unwrap();
                            }
                        }
                    }
                    "read_ok" => {
                        let msg_id = body["in_reply_to"].as_i64().unwrap();
                        if let Some(s) = node.as_ref() {
                            let lookup = s.senders.lock().unwrap();
                            if lookup.contains_key(&msg_id) {
                                lookup[&msg_id].send(body["value"].to_owned()).unwrap();
                            }
                        }
                    }
                    "error" => {
                        let msg_id = body["in_reply_to"].as_i64().unwrap();
                        if let Some(s) = node.as_ref() {
                            let lookup = s.senders.lock().unwrap();
                            if lookup.contains_key(&msg_id) {
                                lookup[&msg_id].send(Value::Bool(false)).unwrap();
                            }
                        }
                    }

                    _ => continue,
                }
            }
        }
    }
}
