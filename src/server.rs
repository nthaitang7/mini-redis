use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread;
use std::time::Duration;

use crate::commands;
use crate::config::ServerConfig;
use crate::protocol::{RespParser, RespValue};
use crate::store::{system_clock, Store};

/// Shared state used by every connection thread.
pub struct SharedState {
    pub store: Mutex<Store>,
}

impl SharedState {
    pub fn new(store: Store) -> Self {
        Self {
            store: Mutex::new(store),
        }
    }
}

/// Bind the configured address and serve until the process exits.
pub fn run(config: ServerConfig) -> io::Result<()> {
    let listener = TcpListener::bind((config.host.as_str(), config.port))?;
    let address = listener.local_addr()?;
    let memory = if config.max_memory == 0 {
        "unlimited".to_string()
    } else {
        format!("{} bytes", config.max_memory)
    };
    println!(
        "mini-redis listening on {}:{} (maxmemory={}, policy={})",
        address.ip(),
        address.port(),
        memory,
        config.eviction_policy.as_str()
    );

    let store = Store::new(config.max_memory, config.eviction_policy, system_clock());
    let state = Arc::new(SharedState::new(store));
    let shutdown = Arc::new(AtomicBool::new(false));
    serve(listener, state, config, shutdown)
}

/// Serve connections from an existing listener until the shutdown flag is set.
pub fn serve(
    listener: TcpListener,
    state: Arc<SharedState>,
    config: ServerConfig,
    shutdown: Arc<AtomicBool>,
) -> io::Result<()> {
    listener.set_nonblocking(true)?;

    if config.sweep_interval_ms > 0 {
        let sweep_state = Arc::clone(&state);
        let sweep_shutdown = Arc::clone(&shutdown);
        let interval = Duration::from_millis(config.sweep_interval_ms);

        thread::spawn(move || {
            while !sweep_shutdown.load(Ordering::Relaxed) {
                thread::sleep(interval);

                if sweep_shutdown.load(Ordering::Relaxed) {
                    break;
                }

                let mut store = lock_store(sweep_state.as_ref());
                store.sweep();
            }
        });
    }

    while !shutdown.load(Ordering::Relaxed) {
        match listener.accept() {
            Ok((stream, _)) => {
                let client_state = Arc::clone(&state);
                let client_config = config.clone();

                thread::spawn(move || {
                    handle_client(stream, client_state, client_config);
                });
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(5));
            }
            Err(error) => return Err(error),
        }
    }

    Ok(())
}

fn handle_client(mut stream: TcpStream, state: Arc<SharedState>, config: ServerConfig) {
    let _ = stream.set_nonblocking(false);
    let _ = stream.set_nodelay(true);

    if config.idle_timeout_ms > 0 {
        let _ = stream.set_read_timeout(Some(Duration::from_millis(config.idle_timeout_ms)));
    }

    let mut parser = RespParser::new(config.max_request_bytes, 1024);
    let mut buffer = [0u8; 16 * 1024];

    loop {
        let read = match stream.read(&mut buffer) {
            Ok(0) => return,
            Ok(value) => value,
            Err(error)
                if error.kind() == io::ErrorKind::WouldBlock
                    || error.kind() == io::ErrorKind::TimedOut =>
            {
                return;
            }
            Err(_) => return,
        };

        let parsed_commands = match parser.feed(&buffer[..read]) {
            Ok(commands) => commands,
            Err(error) => {
                let message = format!("ERR Protocol error: {}", safe_text(&error.0));
                let reply = RespValue::Error(message);
                let _ = stream.write_all(&reply.to_bytes());
                return;
            }
        };

        for args in parsed_commands {
            let result = {
                let mut store = lock_store(state.as_ref());
                commands::dispatch(&args, &mut store)
            };

            match result {
                Ok(outcome) => {
                    if stream.write_all(&outcome.reply.to_bytes()).is_err() {
                        return;
                    }

                    if outcome.close {
                        return;
                    }
                }
                Err(message) => {
                    if stream
                        .write_all(&RespValue::Error(message).to_bytes())
                        .is_err()
                    {
                        return;
                    }
                }
            }
        }
    }
}

fn lock_store(state: &SharedState) -> MutexGuard<'_, Store> {
    state
        .store
        .lock()
        .unwrap_or_else(|error| error.into_inner())
}

fn safe_text(value: &str) -> String {
    value.replace('\r', " ").replace('\n', " ")
}
