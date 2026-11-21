use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use mini_redis::config::ServerConfig;
use mini_redis::protocol::{encode_command, ReplyReader, RespValue};
use mini_redis::server::{serve, SharedState};
use mini_redis::store::{estimate_entry_bytes, system_clock, EvictionPolicy, Store};

struct TestServer {
    address: SocketAddr,
    shutdown: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<()>>,
}

impl TestServer {
    fn start(config: ServerConfig) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let store = Store::new(config.max_memory, config.eviction_policy, system_clock());
        let state = Arc::new(SharedState::new(store));
        let shutdown = Arc::new(AtomicBool::new(false));
        let server_shutdown = Arc::clone(&shutdown);
        let handle = thread::spawn(move || {
            let _ = serve(listener, state, config, server_shutdown);
        });

        Self {
            address,
            shutdown,
            handle: Some(handle),
        }
    }

    fn stop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);

        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.stop();
    }
}

struct Client {
    stream: TcpStream,
    reader: ReplyReader,
}

impl Client {
    fn connect(address: SocketAddr) -> Self {
        let stream = TcpStream::connect(address).unwrap();
        let _ = stream.set_nodelay(true);
        Self {
            stream,
            reader: ReplyReader::new(),
        }
    }

    fn send(&mut self, args: &[&[u8]]) -> RespValue {
        self.stream.write_all(&encode_command(args)).unwrap();
        self.read_reply()
    }

    fn read_reply(&mut self) -> RespValue {
        let mut buffer = [0u8; 4096];

        loop {
            if let Some(reply) = self.reader.read().unwrap() {
                return reply;
            }

            let read = self.stream.read(&mut buffer).unwrap();
            assert!(read > 0, "connection closed before a reply");
            self.reader.push(&buffer[..read]);
        }
    }
}

fn test_config() -> ServerConfig {
    ServerConfig {
        sweep_interval_ms: 0,
        ..ServerConfig::default()
    }
}

fn parts<'a>(values: &[&'a str]) -> Vec<&'a [u8]> {
    values.iter().map(|value| value.as_bytes()).collect()
}

#[test]
fn handles_core_commands_over_tcp() {
    let mut server = TestServer::start(test_config());
    let mut client = Client::connect(server.address);

    assert_eq!(
        client.send(&parts(&["PING"])),
        RespValue::Simple("PONG".to_string())
    );
    assert_eq!(
        client.send(&parts(&["SET", "greeting", "hello"])),
        RespValue::Simple("OK".to_string())
    );
    assert_eq!(
        client.send(&parts(&["GET", "greeting"])),
        RespValue::Bulk(b"hello".to_vec())
    );
    assert_eq!(
        client.send(&parts(&["EXISTS", "greeting", "missing"])),
        RespValue::Integer(1)
    );
    assert_eq!(
        client.send(&parts(&["DELETE", "greeting"])),
        RespValue::Integer(1)
    );
    assert_eq!(client.send(&parts(&["GET", "greeting"])), RespValue::Null);

    server.stop();
}

#[test]
fn round_trips_binary_values() {
    let mut server = TestServer::start(test_config());
    let mut client = Client::connect(server.address);
    let value: &[u8] = &[0, 1, 13, 10, 255];

    assert_eq!(
        client.send(&[b"SET".as_slice(), b"binary".as_slice(), value]),
        RespValue::Simple("OK".to_string())
    );
    assert_eq!(
        client.send(&parts(&["GET", "binary"])),
        RespValue::Bulk(value.to_vec())
    );

    server.stop();
}

#[test]
fn handles_pipelined_commands() {
    let mut server = TestServer::start(test_config());
    let mut client = Client::connect(server.address);
    let mut payload = Vec::new();
    payload.extend_from_slice(&encode_command(&parts(&["PING"])));
    payload.extend_from_slice(&encode_command(&parts(&["SET", "key", "value"])));
    payload.extend_from_slice(&encode_command(&parts(&["GET", "key"])));
    client.stream.write_all(&payload).unwrap();

    assert_eq!(client.read_reply(), RespValue::Simple("PONG".to_string()));
    assert_eq!(client.read_reply(), RespValue::Simple("OK".to_string()));
    assert_eq!(client.read_reply(), RespValue::Bulk(b"value".to_vec()));

    server.stop();
}

#[test]
fn serves_concurrent_clients() {
    let mut server = TestServer::start(test_config());
    let address = server.address;
    let mut handles = Vec::new();

    for client_index in 0..12 {
        handles.push(thread::spawn(move || {
            let mut client = Client::connect(address);
            let key = format!("client:{client_index}");
            let value = format!("value:{client_index}");

            assert_eq!(
                client.send(&parts(&["SET", key.as_str(), value.as_str()])),
                RespValue::Simple("OK".to_string())
            );
            assert_eq!(
                client.send(&parts(&["GET", key.as_str()])),
                RespValue::Bulk(value.into_bytes())
            );
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    server.stop();
}

#[test]
fn rejects_writes_over_the_memory_limit() {
    let item_size = estimate_entry_bytes(b"a", b"x");
    let mut server = TestServer::start(ServerConfig {
        max_memory: item_size,
        eviction_policy: EvictionPolicy::NoEviction,
        sweep_interval_ms: 0,
        ..ServerConfig::default()
    });
    let mut client = Client::connect(server.address);

    assert_eq!(
        client.send(&parts(&["SET", "a", "x"])),
        RespValue::Simple("OK".to_string())
    );

    match client.send(&parts(&["SET", "b", "x"])) {
        RespValue::Error(message) => assert!(message.starts_with("OOM ")),
        other => panic!("expected an OOM reply, received {other:?}"),
    }

    assert_eq!(
        client.send(&parts(&["GET", "a"])),
        RespValue::Bulk(b"x".to_vec())
    );

    server.stop();
}

#[test]
fn closes_only_the_malformed_connection() {
    let mut server = TestServer::start(test_config());
    let mut stream = TcpStream::connect(server.address).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_millis(1000)))
        .unwrap();
    stream.write_all(b"*x\r\n").unwrap();

    let mut response = Vec::new();
    let mut buffer = [0u8; 256];

    loop {
        match stream.read(&mut buffer) {
            Ok(0) => break,
            Ok(read) => response.extend_from_slice(&buffer[..read]),
            Err(error)
                if error.kind() == std::io::ErrorKind::WouldBlock
                    || error.kind() == std::io::ErrorKind::TimedOut =>
            {
                break;
            }
            Err(_) => break,
        }
    }

    let text = String::from_utf8_lossy(&response);
    assert!(text.contains("Protocol error"), "response was {text}");

    let mut client = Client::connect(server.address);
    assert_eq!(
        client.send(&parts(&["PING"])),
        RespValue::Simple("PONG".to_string())
    );

    server.stop();
}
