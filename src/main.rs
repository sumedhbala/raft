use serde::Serialize;
use serde_json::Value;
use std::collections::HashMap;
use std::io;
use std::sync::{Arc, Mutex};

struct Node {
    id: String,
    next_msg_id: i64,
    txn: ThreadMap,
}
type ThreadMap = Arc<Mutex<HashMap<i64, Vec<i64>>>>;

impl Node {
    fn new(id: String) -> Node {
        Node {
            id,
            next_msg_id: 0,
            txn: Arc::new(Mutex::new(HashMap::new())),
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
    Echo {
        msg_id: i64,
        r#type: &'a str,
        in_reply_to: i64,
        echo: &'a str,
    },
    #[serde(rename = "body")]
    Txn {
        r#type: &'a str,
        in_reply_to: i64,
        msg_id: i64,
        txn: Vec<TxnType>,
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
                            s.next_msg_id += 1;
                        }
                        let reply = Reply {
                            dest: parsed["src"].as_str().unwrap(),
                            src: node.as_ref().map(|s| &s.id).unwrap(),
                            body: ResponseBody::Init {
                                msg_id: node.as_ref().map(|s| s.next_msg_id).unwrap(),
                                r#type: "init_ok",
                                in_reply_to: body["msg_id"].as_i64().unwrap(),
                            },
                        };
                        eprintln!("Sending {}", serde_json::to_string(&reply).unwrap());
                        println!("{}", serde_json::to_string(&reply).unwrap());
                    }
                    "echo" => {
                        eprintln!("Echoing {}", body["echo"].as_str().unwrap());
                        if let Some(s) = node.as_mut() {
                            s.next_msg_id += 1;
                        }
                        let reply = Reply {
                            dest: parsed["src"].as_str().unwrap(),
                            src: node.as_ref().map(|s| &s.id).unwrap(),
                            body: ResponseBody::Echo {
                                msg_id: node.as_ref().map(|s| s.next_msg_id).unwrap(),
                                r#type: "echo_ok",
                                in_reply_to: body["msg_id"].as_i64().unwrap(),
                                echo: body["echo"].as_str().unwrap(),
                            },
                        };
                        eprintln!("Sending {}", serde_json::to_string(&reply).unwrap());
                        println!("{}", serde_json::to_string(&reply).unwrap());
                    }
                    "txn" => {
                        let mut txs_json: Vec<TxnType> = Vec::new();
                        for t in body["txn"].as_array().unwrap() {
                            let txn = t.as_array().unwrap();

                            match txn[0].as_str().unwrap() {
                                "append" => {
                                    if let Some(s) = node.as_mut() {
                                        let mut hash = s.txn.lock().unwrap();
                                        let key = txn[1].as_i64().unwrap();
                                        let value = txn[2].as_i64().unwrap();
                                        hash.entry(key).or_insert_with(Vec::new).push(value);
                                        txs_json.push(TxnType(
                                            "append".to_string(),
                                            key,
                                            TxnAnswer::Integer(value),
                                        ));
                                    }
                                }
                                "r" => {
                                    if let Some(s) = node.as_mut() {
                                        let hash = s.txn.lock().unwrap();
                                        let key = txn[1].as_i64().unwrap();
                                        txs_json.push(TxnType(
                                            "r".to_string(),
                                            key,
                                            match hash.contains_key(&key) {
                                                true => TxnAnswer::Array(hash[&key].clone()),
                                                _ => TxnAnswer::None,
                                            },
                                        ));
                                    }
                                }
                                _ => todo!(),
                            }
                        }
                        if let Some(s) = node.as_mut() {
                            s.next_msg_id += 1;
                            let reply = Reply {
                                dest: parsed["src"].as_str().unwrap(),
                                src: &s.id,
                                body: ResponseBody::Txn {
                                    msg_id: s.next_msg_id,
                                    r#type: "txn_ok",
                                    in_reply_to: body["msg_id"].as_i64().unwrap(),
                                    txn: txs_json,
                                },
                            };
                            eprintln!("Sending {}", serde_json::to_string(&reply).unwrap());
                            println!("{}", serde_json::to_string(&reply).unwrap());
                        }
                    }
                    _ => continue,
                }
            }
        }
    }
}
