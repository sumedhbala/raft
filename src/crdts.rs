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
    messages: Arc<Mutex<HashSet<i64>>>,
}

type ThreadSet = Arc<Mutex<HashSet<i64>>>;

async fn replicate(dest: String, src: String, messges: ThreadSet) {
    loop {
        {
            let set = messges.lock().unwrap();
            let message = Reply {
                dest: &dest,
                src: &src,
                body: ResponseBody::Replicate {
                    r#type: "replicate",
                    message: &set,
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
            id,
            neighbours,
            next_msg_id: 0,
            messages: Arc::new(Mutex::new(HashSet::new())),
        };
        node.replicate_neighbours();
        node
    }
    fn replicate_neighbours(&mut self) {
        for n in &self.neighbours {
            tokio::spawn(replicate(n.clone(), self.id.clone(), self.messages.clone()));
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
    Replicate {
        r#type: &'a str,
        message: &'a HashSet<i64>,
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
        value: &'a HashSet<i64>,
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
                    "add" => {
                        if let Some(s) = node.as_mut() {
                            let mut set = s.messages.lock().unwrap();
                            set.insert(body["element"].as_i64().unwrap());
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
                            let mut set = s.messages.lock().unwrap();
                            let r: HashSet<i64> =
                                serde_json::from_value(body["message"].clone()).unwrap();
                            set.extend(&r);
                        }
                    }
                    "read" => {
                        if let Some(s) = node.as_mut() {
                            s.next_msg_id += 1;
                            let set = s.messages.lock().unwrap();
                            let reply = Reply {
                                dest: parsed["src"].as_str().unwrap(),
                                src: &s.id,
                                body: ResponseBody::Read {
                                    value: &set,
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
