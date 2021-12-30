use serde::Serialize;
use serde_json::Value;
use std::cmp::{max, min};
use std::collections::HashMap;
use std::io;
use std::sync::{Arc, Mutex};
use tokio::time::{sleep, Duration};

struct Node {
    id: String,
    neighbours: Vec<String>,
    next_msg_id: i64,
    counter: ThreadMap,
}
const ADD: usize = 0;
const SUBTRACT: usize = 1;
type ThreadMap = Arc<Mutex<[HashMap<String, i64>; 2]>>;

async fn replicate(dest: String, src: String, messges: ThreadMap) {
    loop {
        {
            let hash = messges.lock().unwrap();
            let message = Reply {
                dest: &dest,
                src: &src,
                body: ResponseBody::Replicate {
                    r#type: "replicate",
                    msg: &hash,
                },
            };
            eprintln!("Sending {}", serde_json::to_string(&message).unwrap());
            println!("{}", serde_json::to_string(&message).unwrap());
        }
        sleep(Duration::from_millis(5000)).await;
    }
}

impl Node {
    fn new(id: String, neighbours: Vec<String>) -> Node {
        let mut node = Node {
            id: id.clone(),
            neighbours,
            next_msg_id: 0,
            counter: Arc::new(Mutex::new([
                HashMap::from([(id.clone(), 0)]),
                HashMap::from([(id, 0)]),
            ])),
        };
        node.replicate_neighbours();
        node
    }
    fn replicate_neighbours(&mut self) {
        for n in &self.neighbours {
            {
                let mut hash = self.counter.lock().unwrap();
                hash[ADD].insert(n.to_string(), 0);
                hash[SUBTRACT].insert(n.to_string(), 0);
            }
            tokio::spawn(replicate(n.clone(), self.id.clone(), self.counter.clone()));
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
    Replicate {
        r#type: &'a str,
        msg: &'a [HashMap<String, i64>; 2],
    },
    #[serde(rename = "body")]
    Add {
        r#type: &'a str,
        in_reply_to: i64,
        msg_id: i64,
    },
    #[serde(rename = "body")]
    Read {
        r#type: &'a str,
        value: i64,
        in_reply_to: i64,
    },
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
                        node = Some(Node::new(
                            body["node_id"].as_str().unwrap().to_string(),
                            serde_json::from_value(body["node_ids"].clone()).unwrap(),
                        ));

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
                        // node.as_mut().map(|s| s.next_msg_id += 1);
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
                    "add" => {
                        if let Some(s) = node.as_mut() {
                            let current;
                            let mut hash = s.counter.lock().unwrap();
                            let value = body["delta"].as_i64().unwrap();
                            match value {
                                0.. => {
                                    {
                                        current = *hash[ADD].get(&s.id).unwrap();
                                    }
                                    hash[ADD].insert(s.id.clone(), current + value);
                                }
                                _ => {
                                    {
                                        current = *hash[SUBTRACT].get(&s.id).unwrap();
                                    }
                                    hash[SUBTRACT].insert(s.id.clone(), current + value);
                                }
                            }
                            s.next_msg_id += 1;

                            let reply = Reply {
                                dest: parsed["src"].as_str().unwrap(),
                                src: &s.id,
                                body: ResponseBody::Add {
                                    msg_id: s.next_msg_id,
                                    r#type: "add_ok",
                                    in_reply_to: body["msg_id"].as_i64().unwrap(),
                                },
                            };
                            eprintln!("Sending {}", serde_json::to_string(&reply).unwrap());
                            println!("{}", serde_json::to_string(&reply).unwrap());
                        }
                    }
                    "replicate" => {
                        if let Some(s) = node.as_mut() {
                            let r: [HashMap<String, i64>; 2] =
                                serde_json::from_value(body["msg"].clone()).unwrap();
                            let mut hash = s.counter.lock().unwrap();
                            let funcs = [max, min];
                            for i in [ADD, SUBTRACT] {
                                for k in r[i].keys() {
                                    let current: i64 = match hash[i].get(k) {
                                        Some(val) => *val,
                                        None => 0,
                                    };
                                    let value = *r[i].get(k).unwrap();
                                    hash[i].insert(k.to_string(), funcs[i](current, value));
                                }
                            }
                        }
                    }
                    "read" => {
                        if let Some(s) = node.as_mut() {
                            s.next_msg_id += 1;
                            let hash = s.counter.lock().unwrap();
                            let reply = Reply {
                                dest: parsed["src"].as_str().unwrap(),
                                src: &s.id,
                                body: ResponseBody::Read {
                                    value: hash[ADD].values().sum::<i64>()
                                        + hash[SUBTRACT].values().sum::<i64>(),
                                    r#type: "read_ok",
                                    in_reply_to: body["msg_id"].as_i64().unwrap(),
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
