use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;

use mini_redis::protocol::{RespParser, RespValue};

fn main() {
    let mut host = "127.0.0.1".to_string();
    let mut port = 6391u16;
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut index = 0;

    while index < args.len() {
        match args[index].as_str() {
            "--host" => {
                host = match args.get(index + 1) {
                    Some(value) => value.clone(),
                    None => usage(),
                };
                index += 2;
            }
            "--port" => {
                port = match args.get(index + 1).and_then(|value| value.parse().ok()) {
                    Some(value) => value,
                    None => usage(),
                };
                index += 2;
            }
            "--help" | "-h" => {
                println!("Usage: baseline [--host HOST] [--port PORT]");
                return;
            }
            other => {
                eprintln!("unknown option: {other}");
                std::process::exit(2);
            }
        }
    }

    let listener = TcpListener::bind((host.as_str(), port)).unwrap_or_else(|error| {
        eprintln!("failed to bind {host}:{port}: {error}");
        std::process::exit(1);
    });

    println!("baseline map server listening on {host}:{port}");
    let values: Arc<Mutex<HashMap<Vec<u8>, Vec<u8>>>> = Arc::new(Mutex::new(HashMap::new()));

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let values = Arc::clone(&values);
                thread::spawn(move || handle_client(stream, values));
            }
            Err(error) => eprintln!("accept failed: {error}"),
        }
    }
}

fn usage() -> ! {
    eprintln!("Usage: baseline [--host HOST] [--port PORT]");
    std::process::exit(2);
}

fn handle_client(mut stream: TcpStream, values: Arc<Mutex<HashMap<Vec<u8>, Vec<u8>>>>) {
    let _ = stream.set_nodelay(true);
    let mut parser = RespParser::new(1024 * 1024, 1024);
    let mut buffer = [0u8; 16 * 1024];

    loop {
        let read = match stream.read(&mut buffer) {
            Ok(0) => return,
            Ok(value) => value,
            Err(_) => return,
        };

        let parsed = match parser.feed(&buffer[..read]) {
            Ok(commands) => commands,
            Err(error) => {
                let reply = RespValue::Error(format!("ERR Protocol error: {}", error.0));
                let _ = stream.write_all(&reply.to_bytes());
                return;
            }
        };

        for args in parsed {
            if args.is_empty() {
                continue;
            }

            let command = String::from_utf8_lossy(&args[0]).to_ascii_uppercase();

            let reply = match command.as_str() {
                "PING" => RespValue::Simple("PONG".to_string()),
                "GET" => {
                    let value = args.get(1).and_then(|key| {
                        values
                            .lock()
                            .unwrap_or_else(|error| error.into_inner())
                            .get(key)
                            .cloned()
                    });

                    match value {
                        Some(value) => RespValue::Bulk(value),
                        None => RespValue::Null,
                    }
                }
                "SET" => {
                    if args.len() < 3 {
                        RespValue::Error(
                            "ERR wrong number of arguments for 'set' command".to_string(),
                        )
                    } else {
                        values
                            .lock()
                            .unwrap_or_else(|error| error.into_inner())
                            .insert(args[1].clone(), args[2].clone());
                        RespValue::Simple("OK".to_string())
                    }
                }
                "DEL" | "DELETE" => {
                    let mut store = values.lock().unwrap_or_else(|error| error.into_inner());
                    let mut deleted = 0i64;

                    for key in &args[1..] {
                        if store.remove(key).is_some() {
                            deleted += 1;
                        }
                    }

                    RespValue::Integer(deleted)
                }
                "QUIT" => {
                    let _ = stream.write_all(&RespValue::Simple("OK".to_string()).to_bytes());
                    return;
                }
                _ => RespValue::Error(format!(
                    "ERR unknown command '{}'",
                    command.replace('\r', " ").replace('\n', " ")
                )),
            };

            if stream.write_all(&reply.to_bytes()).is_err() {
                return;
            }
        }
    }
}
