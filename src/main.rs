use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io;

struct Node {
    id: String,
    next_msg_id: i32,
}

#[derive(Serialize, Deserialize)]
enum ResponseBody<'a> {
    #[serde(rename = "body")]
    InitOkBody {
        msg_id: i32,
        r#type: &'a str,
        in_reply_to: i64,
    },
    #[serde(rename = "body")]
    EchoOkBody {
        msg_id: i32,
        r#type: &'a str,
        in_reply_to: i64,
        echo: &'a str,
    },
}

#[derive(Serialize, Deserialize)]
struct InitOk<'a> {
    src: &'a str,
    dest: &'a str,
    #[serde(flatten)]
    body: ResponseBody<'a>,
}

fn main() {
    let mut node: Option<Node> = None;
    loop {
        let mut input = String::new();

        match io::stdin().read_line(&mut input) {
            Err(error) => println!("error: {}", error),
            Ok(_) => {
                eprintln!("Received {}", input);
                let parsed: Value = serde_json::from_str(&input).unwrap();
                // let obj: Map<String, Value> = parsed.as_object().unwrap().to_owned();
                let body = &parsed["body"];
                // eprintln!("type is {:?}", &body["type"].as_str().unwrap());
                match body["type"].as_str().unwrap() {
                    "init" => {
                        node = Some(Node {
                            id: body["node_id"].as_str().unwrap().to_string(),
                            next_msg_id: 0,
                        });
                        eprintln!("Initialized node {:?}", node.as_ref().map(|s| &s.id));
                        if let Some(s) = node.as_mut() {
                            s.next_msg_id += 1;
                        }
                        let reply = InitOk {
                            dest: parsed["src"].as_str().unwrap(),
                            src: node.as_ref().map(|s| &s.id).unwrap(),
                            body: ResponseBody::InitOkBody {
                                msg_id: node.as_ref().map(|s| s.next_msg_id).unwrap(),
                                r#type: "init_ok",
                                in_reply_to: body["msg_id"].as_i64().unwrap(),
                            },
                        };
                        println!("{}", serde_json::to_string(&reply).unwrap());
                    }
                    "echo" => {
                        eprintln!("Echoing {}", body["echo"].as_str().unwrap());
                        // node.as_mut().map(|s| s.next_msg_id += 1);
                        if let Some(s) = node.as_mut() {
                            s.next_msg_id += 1;
                        }
                        let reply = InitOk {
                            dest: parsed["src"].as_str().unwrap(),
                            src: node.as_ref().map(|s| &s.id).unwrap(),
                            body: ResponseBody::EchoOkBody {
                                msg_id: node.as_ref().map(|s| s.next_msg_id).unwrap(),
                                r#type: "echo_ok",
                                in_reply_to: body["msg_id"].as_i64().unwrap(),
                                echo: body["echo"].as_str().unwrap(),
                            },
                        };
                        println!("{}", serde_json::to_string(&reply).unwrap());
                    }
                    _ => continue,
                }
            }
        }
    }
}
