use serde::Serialize;
use serde_json::Value;
use std::collections::HashSet;
use std::io;
use std::sync::{Arc, Mutex};
use tokio::time::{sleep, Duration};

struct Node {
    id: String,
    neighbours: Vec<String>,
    next_msg_id: i64,
    messages: Vec<i64>,
    seen_messages: HashSet<i64>,
    ack_messages: Arc<Mutex<HashSet<i64>>>,
}

type ThreadSet = Arc<Mutex<HashSet<i64>>>;

async fn broadcast(dest: String, src: String, msg: i64, msg_id: i64, seen: ThreadSet) {
    loop {
        {
            let set = seen.lock().unwrap();
            if !set.contains(&msg_id) {
                break;
            }
            let message = Reply {
                dest: &dest,
                src: &src,
                body: ResponseBody::Broadcast {
                    r#type: "broadcast",
                    message: msg,
                    msg_id,
                },
            };
            eprintln!("Sending {}", serde_json::to_string(&message).unwrap());
            println!("{}", serde_json::to_string(&message).unwrap());
        }
        sleep(Duration::from_millis(2000)).await;
    }
}

impl Node {
    fn broadcast_neighbours(&mut self, msg: i64, src: &str) {
        for n in &self.neighbours {
            if n != src {
                self.next_msg_id += 1;
                let mut db = self.ack_messages.lock().unwrap();
                db.insert(self.next_msg_id);
                tokio::spawn(broadcast(
                    n.clone(),
                    self.id.clone(),
                    msg,
                    self.next_msg_id,
                    self.ack_messages.clone(),
                ));
            }
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
    Topology {
        msg_id: i64,
        r#type: &'a str,
        in_reply_to: i64,
    },
    #[serde(rename = "body")]
    Broadcast {
        r#type: &'a str,
        message: i64,
        msg_id: i64,
    },
    #[serde(rename = "body")]
    BroadcastOk {
        r#type: &'a str,
        in_reply_to: i64,
        msg_id: i64,
    },
    #[serde(rename = "body")]
    Read {
        r#type: &'a str,
        messages: &'a [i64],
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
                        node = Some(Node {
                            id: body["node_id"].as_str().unwrap().to_string(),
                            next_msg_id: 0,
                            neighbours: serde_json::from_value(body["node_ids"].clone()).unwrap(),
                            messages: Vec::new(),
                            seen_messages: HashSet::new(),
                            ack_messages: Arc::new(Mutex::new(HashSet::new())),
                        });
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
                    "topology" => {
                        if let Some(s) = node.as_mut() {
                            s.neighbours =
                                serde_json::from_value(body["topology"][&s.id].clone()).unwrap();
                            eprintln!("My neighbours are {:?}", s.neighbours);
                        }
                        if let Some(s) = node.as_mut() {
                            s.next_msg_id += 1;
                        }
                        let reply = Reply {
                            dest: parsed["src"].as_str().unwrap(),
                            src: node.as_ref().map(|s| &s.id).unwrap(),
                            body: ResponseBody::Topology {
                                msg_id: node.as_ref().map(|s| s.next_msg_id).unwrap(),
                                r#type: "topology_ok",
                                in_reply_to: body["msg_id"].as_i64().unwrap(),
                            },
                        };
                        eprintln!("Sending {}", serde_json::to_string(&reply).unwrap());
                        println!("{}", serde_json::to_string(&reply).unwrap());
                    }
                    "broadcast" => {
                        if let Some(s) = node.as_mut() {
                            if !s.seen_messages.contains(&body["message"].as_i64().unwrap()) {
                                s.broadcast_neighbours(
                                    body["message"].as_i64().unwrap(),
                                    parsed["src"].as_str().unwrap(),
                                );
                                s.messages.push(body["message"].as_i64().unwrap());
                                s.seen_messages.insert(body["message"].as_i64().unwrap());
                            }
                        }

                        if let Some(msg_id) = body["msg_id"].as_i64() {
                            if let Some(s) = node.as_mut() {
                                s.next_msg_id += 1;
                            }

                            let reply = Reply {
                                dest: parsed["src"].as_str().unwrap(),
                                src: node.as_ref().map(|s| &s.id).unwrap(),
                                body: ResponseBody::BroadcastOk {
                                    msg_id: node.as_ref().map(|s| s.next_msg_id).unwrap(),
                                    r#type: "broadcast_ok",
                                    in_reply_to: msg_id,
                                },
                            };
                            eprintln!("Sending {}", serde_json::to_string(&reply).unwrap());
                            println!("{}", serde_json::to_string(&reply).unwrap());
                        }
                    }
                    "broadcast_ok" => {
                        if let Some(s) = node.as_mut() {
                            let mut set = s.ack_messages.lock().unwrap();
                            if set.contains(&body["in_reply_to"].as_i64().unwrap()) {
                                set.remove(&body["in_reply_to"].as_i64().unwrap());
                            }
                        }
                    }
                    "read" => {
                        if let Some(s) = node.as_mut() {
                            s.next_msg_id += 1;
                        }
                        let reply = Reply {
                            dest: parsed["src"].as_str().unwrap(),
                            src: node.as_ref().map(|s| &s.id).unwrap(),
                            body: ResponseBody::Read {
                                messages: node.as_ref().map(|s| &s.messages[..]).unwrap(),
                                r#type: "read_ok",
                                in_reply_to: body["msg_id"].as_i64().unwrap(),
                            },
                        };
                        eprintln!("Sending {}", serde_json::to_string(&reply).unwrap());
                        println!("{}", serde_json::to_string(&reply).unwrap());
                    }
                    _ => continue,
                }
            }
        }
    }
}
