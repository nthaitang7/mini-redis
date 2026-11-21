use crate::protocol::RespValue;
use crate::store::Store;

/// The reply for one command and whether the connection should close.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandOutcome {
    pub reply: RespValue,
    pub close: bool,
}

impl CommandOutcome {
    fn reply(reply: RespValue) -> Self {
        Self {
            reply,
            close: false,
        }
    }
}

/// Execute one parsed command. Returns a full RESP error message on failure.
pub fn dispatch(args: &[Vec<u8>], store: &mut Store) -> Result<CommandOutcome, String> {
    store.record_command();

    if args.is_empty() {
        return Err("ERR empty command".to_string());
    }

    let command = token(&args[0]);
    let command_name = safe_text(&command);

    match command.as_str() {
        "PING" => {
            require_arity("ping", args, 1, 2)?;

            if args.len() == 2 {
                Ok(CommandOutcome::reply(RespValue::Bulk(args[1].clone())))
            } else {
                Ok(CommandOutcome::reply(RespValue::Simple("PONG".to_string())))
            }
        }
        "ECHO" => {
            require_arity("echo", args, 2, 2)?;
            Ok(CommandOutcome::reply(RespValue::Bulk(args[1].clone())))
        }
        "GET" => {
            require_arity("get", args, 2, 2)?;
            Ok(CommandOutcome::reply(match store.get(&args[1]) {
                Some(value) => RespValue::Bulk(value),
                None => RespValue::Null,
            }))
        }
        "SET" => {
            require_arity("set", args, 3, usize::MAX)?;
            let mut ttl_ms: Option<u64> = None;
            let mut index = 3;

            while index < args.len() {
                let option = token(&args[index]);

                if option != "EX" && option != "PX" {
                    return Err("ERR syntax error".to_string());
                }

                if ttl_ms.is_some() {
                    return Err("ERR syntax error".to_string());
                }

                let amount_buffer = args
                    .get(index + 1)
                    .ok_or_else(|| "ERR syntax error".to_string())?;
                let amount = parse_integer(amount_buffer)
                    .ok_or_else(|| "ERR invalid expire time in 'set' command".to_string())?;

                if amount <= 0 {
                    return Err("ERR invalid expire time in 'set' command".to_string());
                }

                let multiplier = if option == "EX" { 1000 } else { 1 };
                let milliseconds = amount
                    .checked_mul(multiplier)
                    .ok_or_else(|| "ERR invalid expire time in 'set' command".to_string())?;

                ttl_ms = Some(milliseconds as u64);
                index += 2;
            }

            store
                .set(&args[1], &args[2], ttl_ms)
                .map_err(|error| error.to_string())?;
            Ok(CommandOutcome::reply(RespValue::Simple("OK".to_string())))
        }
        "DEL" | "DELETE" => {
            require_arity(&command_name, args, 2, usize::MAX)?;
            Ok(CommandOutcome::reply(RespValue::Integer(
                store.delete(&args[1..]),
            )))
        }
        "EXISTS" => {
            require_arity("exists", args, 2, usize::MAX)?;
            let mut count = 0i64;

            for key in &args[1..] {
                if store.exists(key) {
                    count += 1;
                }
            }

            Ok(CommandOutcome::reply(RespValue::Integer(count)))
        }
        "EXPIRE" | "PEXPIRE" => {
            require_arity(&command_name, args, 3, 3)?;
            let amount = parse_integer(&args[2])
                .ok_or_else(|| "ERR value is not an integer or out of range".to_string())?;
            let multiplier: i64 = if command == "EXPIRE" { 1000 } else { 1 };
            let milliseconds = if amount > 0 {
                amount
                    .checked_mul(multiplier)
                    .ok_or_else(|| "ERR value is not an integer or out of range".to_string())?
            } else {
                amount.saturating_mul(multiplier)
            };
            let changed = store.expire(&args[1], milliseconds);
            Ok(CommandOutcome::reply(RespValue::Integer(if changed {
                1
            } else {
                0
            })))
        }
        "TTL" | "PTTL" => {
            require_arity(&command_name, args, 2, 2)?;
            let value = if command == "TTL" {
                store.ttl_seconds(&args[1])
            } else {
                store.ttl_milliseconds(&args[1])
            };
            Ok(CommandOutcome::reply(RespValue::Integer(value)))
        }
        "PERSIST" => {
            require_arity("persist", args, 2, 2)?;
            Ok(CommandOutcome::reply(RespValue::Integer(
                if store.persist(&args[1]) { 1 } else { 0 },
            )))
        }
        "DBSIZE" => {
            require_arity("dbsize", args, 1, 1)?;
            Ok(CommandOutcome::reply(RespValue::Integer(
                store.key_count() as i64
            )))
        }
        "FLUSHDB" | "FLUSHALL" => {
            require_arity(&command_name, args, 1, 1)?;
            store.flush();
            Ok(CommandOutcome::reply(RespValue::Simple("OK".to_string())))
        }
        "SELECT" => {
            require_arity("select", args, 2, 2)?;

            match parse_integer(&args[1]) {
                Some(0) => Ok(CommandOutcome::reply(RespValue::Simple("OK".to_string()))),
                _ => Err("ERR DB index is out of range".to_string()),
            }
        }
        "INFO" => {
            require_arity("info", args, 1, 2)?;
            Ok(CommandOutcome::reply(info_reply(args, store)))
        }
        "TYPE" => {
            require_arity("type", args, 2, 2)?;
            let value = if store.exists(&args[1]) {
                "string"
            } else {
                "none"
            };
            Ok(CommandOutcome::reply(RespValue::Simple(value.to_string())))
        }
        "CLIENT" => {
            require_arity("client", args, 2, usize::MAX)?;
            let subcommand = token(&args[1]);

            match subcommand.as_str() {
                "SETNAME" => {
                    require_arity("client", args, 3, 3)?;
                    Ok(CommandOutcome::reply(RespValue::Simple("OK".to_string())))
                }
                "SETINFO" => {
                    require_arity("client", args, 3, 4)?;
                    Ok(CommandOutcome::reply(RespValue::Simple("OK".to_string())))
                }
                "GETNAME" => {
                    require_arity("client", args, 2, 2)?;
                    Ok(CommandOutcome::reply(RespValue::Null))
                }
                _ => Err(format!(
                    "ERR unknown subcommand '{}'",
                    safe_text(&subcommand)
                )),
            }
        }
        "COMMAND" => Ok(CommandOutcome::reply(RespValue::Array(Vec::new()))),
        "QUIT" => {
            require_arity("quit", args, 1, 1)?;
            Ok(CommandOutcome {
                reply: RespValue::Simple("OK".to_string()),
                close: true,
            })
        }
        _ => Err(format!("ERR unknown command '{command_name}'")),
    }
}

fn info_reply(args: &[Vec<u8>], store: &mut Store) -> RespValue {
    let requested = if args.len() == 2 {
        token(&args[1])
    } else {
        "DEFAULT".to_string()
    };

    if requested == "DEFAULT" || requested == "ALL" {
        return RespValue::Bulk(info_all(store).into_bytes());
    }

    let section = requested.to_ascii_lowercase();

    match info_section(&section, store) {
        Some(content) => RespValue::Bulk(content.into_bytes()),
        None => RespValue::Bulk(Vec::new()),
    }
}

fn info_all(store: &mut Store) -> String {
    let sections = ["server", "memory", "stats", "keyspace"];
    let mut parts = Vec::new();

    for section in sections {
        if let Some(content) = info_section(section, store) {
            parts.push(content);
        }
    }

    parts.join("\r\n\r\n")
}

fn info_section(name: &str, store: &mut Store) -> Option<String> {
    match name {
        "server" => Some(
            [
                "# Server".to_string(),
                "redis_version:mini-rs-0.1.0".to_string(),
                "redis_mode:standalone".to_string(),
                format!("uptime_in_seconds:{}", store.uptime_seconds()),
            ]
            .join("\r\n"),
        ),
        "memory" => Some(
            [
                "# Memory".to_string(),
                format!("used_memory:{}", store.used_memory()),
                format!("used_memory_human:{}", format_bytes(store.used_memory())),
                format!("maxmemory:{}", store.memory_limit()),
                format!("maxmemory_policy:{}", store.policy().as_str()),
            ]
            .join("\r\n"),
        ),
        "stats" => {
            let stats = store.stats();
            Some(
                [
                    "# Stats".to_string(),
                    format!("total_commands_processed:{}", stats.total_commands),
                    format!("keyspace_hits:{}", stats.hits),
                    format!("keyspace_misses:{}", stats.misses),
                    format!("evicted_keys:{}", stats.evictions),
                    format!("expired_keys:{}", stats.expired_keys),
                ]
                .join("\r\n"),
            )
        }
        "keyspace" => {
            let keys = store.key_count();

            if keys == 0 {
                return Some("# Keyspace".to_string());
            }

            let expires = store.expiring_key_count();
            let average_ttl = store.average_ttl_milliseconds();
            Some(
                [
                    "# Keyspace".to_string(),
                    format!("db0:keys={keys},expires={expires},avg_ttl={average_ttl}"),
                ]
                .join("\r\n"),
            )
        }
        _ => None,
    }
}

fn format_bytes(bytes: usize) -> String {
    if bytes < 1024 {
        format!("{bytes}B")
    } else if bytes < 1024 * 1024 {
        format!("{:.1}K", bytes as f64 / 1024.0)
    } else if bytes < 1024 * 1024 * 1024 {
        format!("{:.1}M", bytes as f64 / (1024.0 * 1024.0))
    } else {
        format!("{:.1}G", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    }
}

fn token(value: &[u8]) -> String {
    String::from_utf8_lossy(value).to_ascii_uppercase()
}

fn safe_text(value: &str) -> String {
    value.replace('\r', " ").replace('\n', " ")
}

fn parse_integer(value: &[u8]) -> Option<i64> {
    std::str::from_utf8(value).ok()?.parse::<i64>().ok()
}

fn require_arity(name: &str, args: &[Vec<u8>], min: usize, max: usize) -> Result<(), String> {
    if args.len() < min || args.len() > max {
        Err(format!(
            "ERR wrong number of arguments for '{}' command",
            name.to_ascii_lowercase()
        ))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::{EvictionPolicy, Store};

    fn store() -> Store {
        Store::new(0, EvictionPolicy::AllKeysLru, Box::new(|| 1_000))
    }

    fn args(values: &[&str]) -> Vec<Vec<u8>> {
        values
            .iter()
            .map(|value| value.as_bytes().to_vec())
            .collect()
    }

    #[test]
    fn handles_core_commands() {
        let mut store = store();
        assert_eq!(
            dispatch(&args(&["SET", "greeting", "hello"]), &mut store)
                .unwrap()
                .reply,
            RespValue::Simple("OK".to_string())
        );
        assert_eq!(
            dispatch(&args(&["GET", "greeting"]), &mut store)
                .unwrap()
                .reply,
            RespValue::Bulk(b"hello".to_vec())
        );
        assert_eq!(
            dispatch(&args(&["DELETE", "greeting"]), &mut store)
                .unwrap()
                .reply,
            RespValue::Integer(1)
        );
        assert_eq!(
            dispatch(&args(&["GET", "greeting"]), &mut store)
                .unwrap()
                .reply,
            RespValue::Null
        );
    }

    #[test]
    fn handles_expiration_options() {
        let mut store = store();
        dispatch(&args(&["SET", "key", "value", "EX", "1"]), &mut store).unwrap();
        assert_eq!(
            dispatch(&args(&["TTL", "key"]), &mut store).unwrap().reply,
            RespValue::Integer(1)
        );
    }

    #[test]
    fn reports_info() {
        let mut store = store();
        let reply = dispatch(&args(&["INFO", "memory"]), &mut store)
            .unwrap()
            .reply;

        match reply {
            RespValue::Bulk(value) => {
                let text = String::from_utf8_lossy(&value);
                assert!(text.contains("# Memory"));
                assert!(text.contains("maxmemory_policy:allkeys-lru"));
            }
            other => panic!("expected bulk reply, received {other:?}"),
        }
    }

    #[test]
    fn rejects_unknown_commands() {
        let mut store = store();
        assert_eq!(
            dispatch(&args(&["NOPE"]), &mut store),
            Err("ERR unknown command 'NOPE'".to_string())
        );
    }
}
