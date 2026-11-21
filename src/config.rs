use std::collections::HashMap;
use std::fmt;

use crate::store::EvictionPolicy;

/// Fully validated server settings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    pub max_memory: usize,
    pub eviction_policy: EvictionPolicy,
    pub sweep_interval_ms: u64,
    pub max_request_bytes: usize,
    pub idle_timeout_ms: u64,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 6379,
            max_memory: 0,
            eviction_policy: EvictionPolicy::AllKeysLru,
            sweep_interval_ms: 1000,
            max_request_bytes: 1024 * 1024,
            idle_timeout_ms: 0,
        }
    }
}

/// Result of parsing command-line and environment configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedConfig {
    pub help: bool,
    pub config: ServerConfig,
}

/// Invalid environment or command-line configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigError(pub String);

impl fmt::Display for ConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for ConfigError {}

pub const HELP_TEXT: &str = "Usage: mini-redis [options]

Options:
  --host <host>                 Bind address (default: 127.0.0.1)
  --port <port>                 TCP port (default: 6379)
  --maxmemory <size>            Memory limit, for example 64mb, 512kb or 0 for unlimited
  --eviction <policy>           noeviction, allkeys-lru or volatile-lru
  --sweep-interval <ms>         Expired key sweep interval (default: 1000)
  --max-request-bytes <bytes>   Maximum request size (default: 1048576)
  --idle-timeout <ms>           Close idle clients after this long, 0 disables it
  --help                        Show this help

Environment variables use the same names: HOST, PORT, MAXMEMORY, EVICTION_POLICY,
SWEEP_INTERVAL_MS, MAX_REQUEST_BYTES and IDLE_TIMEOUT_MS.
";

/// Parse a byte count such as 512, 64kb, or 1.5mb.
pub fn parse_memory_size(value: &str) -> Result<usize, ConfigError> {
    let trimmed = value.trim();
    let split = trimmed
        .find(|character: char| !character.is_ascii_digit() && character != '.')
        .unwrap_or(trimmed.len());
    let (amount_text, unit_text) = trimmed.split_at(split);
    let amount: f64 = amount_text
        .parse()
        .map_err(|_| ConfigError(format!("invalid memory size: {value}")))?;
    let unit = if unit_text.is_empty() {
        "b"
    } else {
        unit_text.trim()
    };

    let multiplier = match unit.to_ascii_lowercase().as_str() {
        "b" => 1.0,
        "kb" => 1024.0,
        "mb" => 1024.0 * 1024.0,
        "gb" => 1024.0 * 1024.0 * 1024.0,
        _ => return Err(ConfigError(format!("invalid memory size: {value}"))),
    };

    let bytes = amount * multiplier;

    if !bytes.is_finite() || bytes < 0.0 || bytes > usize::MAX as f64 {
        return Err(ConfigError(format!("invalid memory size: {value}")));
    }

    Ok(bytes.floor() as usize)
}

/// Parse a non-negative integer within an inclusive range.
pub fn parse_usize(name: &str, value: &str, min: usize, max: usize) -> Result<usize, ConfigError> {
    let parsed = value
        .trim()
        .parse::<usize>()
        .map_err(|_| ConfigError(format!("{name} must be an integer")))?;

    if parsed < min || parsed > max {
        return Err(ConfigError(format!(
            "{name} must be between {min} and {max}"
        )));
    }

    Ok(parsed)
}

fn parse_u64(name: &str, value: &str, min: u64, max: u64) -> Result<u64, ConfigError> {
    let parsed = value
        .trim()
        .parse::<u64>()
        .map_err(|_| ConfigError(format!("{name} must be an integer")))?;

    if parsed < min || parsed > max {
        return Err(ConfigError(format!(
            "{name} must be between {min} and {max}"
        )));
    }

    Ok(parsed)
}

fn parse_policy(value: &str) -> Result<EvictionPolicy, ConfigError> {
    EvictionPolicy::parse(value)
        .ok_or_else(|| ConfigError(format!("invalid eviction policy: {value}")))
}

fn required_value<'a>(
    args: &'a [String],
    index: usize,
    option: &str,
) -> Result<&'a String, ConfigError> {
    args.get(index + 1)
        .ok_or_else(|| ConfigError(format!("{option} requires a value")))
}

/// Load configuration from the process environment and arguments.
pub fn load() -> Result<ParsedConfig, ConfigError> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let env: HashMap<String, String> = std::env::vars().collect();
    parse_args(&args, &env)
}

/// Parse configuration from explicit arguments and environment values.
pub fn parse_args(
    args: &[String],
    env: &HashMap<String, String>,
) -> Result<ParsedConfig, ConfigError> {
    let mut config = ServerConfig {
        host: env
            .get("HOST")
            .cloned()
            .unwrap_or_else(|| "127.0.0.1".to_string()),
        port: parse_u64(
            "PORT",
            env.get("PORT").map(String::as_str).unwrap_or("6379"),
            0,
            65535,
        )? as u16,
        max_memory: match env.get("MAXMEMORY") {
            Some(value) => parse_memory_size(value)?,
            None => 0,
        },
        eviction_policy: parse_policy(
            env.get("EVICTION_POLICY")
                .or_else(|| env.get("MAXMEMORY_POLICY"))
                .map(String::as_str)
                .unwrap_or("allkeys-lru"),
        )?,
        sweep_interval_ms: parse_u64(
            "SWEEP_INTERVAL_MS",
            env.get("SWEEP_INTERVAL_MS")
                .map(String::as_str)
                .unwrap_or("1000"),
            0,
            86_400_000,
        )?,
        max_request_bytes: parse_usize(
            "MAX_REQUEST_BYTES",
            env.get("MAX_REQUEST_BYTES")
                .map(String::as_str)
                .unwrap_or("1048576"),
            16,
            1024 * 1024 * 1024,
        )?,
        idle_timeout_ms: parse_u64(
            "IDLE_TIMEOUT_MS",
            env.get("IDLE_TIMEOUT_MS")
                .map(String::as_str)
                .unwrap_or("0"),
            0,
            86_400_000,
        )?,
    };

    let mut index = 0;

    while index < args.len() {
        let option = args[index].as_str();

        match option {
            "--help" | "-h" => {
                return Ok(ParsedConfig { help: true, config });
            }
            "--host" => {
                config.host = required_value(args, index, option)?.clone();
                index += 1;
            }
            "--port" => {
                config.port =
                    parse_u64("port", required_value(args, index, option)?, 0, 65535)? as u16;
                index += 1;
            }
            "--maxmemory" => {
                config.max_memory = parse_memory_size(required_value(args, index, option)?)?;
                index += 1;
            }
            "--eviction" => {
                config.eviction_policy = parse_policy(required_value(args, index, option)?)?;
                index += 1;
            }
            "--sweep-interval" => {
                config.sweep_interval_ms = parse_u64(
                    "sweep-interval",
                    required_value(args, index, option)?,
                    0,
                    86_400_000,
                )?;
                index += 1;
            }
            "--max-request-bytes" => {
                config.max_request_bytes = parse_usize(
                    "max-request-bytes",
                    required_value(args, index, option)?,
                    16,
                    1024 * 1024 * 1024,
                )?;
                index += 1;
            }
            "--idle-timeout" => {
                config.idle_timeout_ms = parse_u64(
                    "idle-timeout",
                    required_value(args, index, option)?,
                    0,
                    86_400_000,
                )?;
                index += 1;
            }
            _ => return Err(ConfigError(format!("unknown option: {option}"))),
        }

        index += 1;
    }

    Ok(ParsedConfig {
        help: false,
        config,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env(values: &[(&str, &str)]) -> HashMap<String, String> {
        values
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect()
    }

    #[test]
    fn parses_memory_sizes() {
        assert_eq!(parse_memory_size("0").unwrap(), 0);
        assert_eq!(parse_memory_size("512").unwrap(), 512);
        assert_eq!(parse_memory_size("1kb").unwrap(), 1024);
        assert_eq!(parse_memory_size("64MB").unwrap(), 64 * 1024 * 1024);
        assert_eq!(parse_memory_size("1.5gb").unwrap(), 1_610_612_736);
        assert!(parse_memory_size("lots").is_err());
        assert!(parse_memory_size("-1mb").is_err());
        assert!(parse_memory_size("1tb").is_err());
    }

    #[test]
    fn applies_environment_and_arguments() {
        let parsed = parse_args(
            &[
                "--port".to_string(),
                "6380".to_string(),
                "--maxmemory".to_string(),
                "64mb".to_string(),
                "--eviction".to_string(),
                "volatile-lru".to_string(),
            ],
            &env(&[
                ("HOST", "0.0.0.0"),
                ("PORT", "6379"),
                ("MAXMEMORY", "1mb"),
                ("EVICTION_POLICY", "noeviction"),
                ("SWEEP_INTERVAL_MS", "250"),
                ("MAX_REQUEST_BYTES", "2048"),
                ("IDLE_TIMEOUT_MS", "1000"),
            ]),
        )
        .unwrap();

        assert!(!parsed.help);
        assert_eq!(parsed.config.host, "0.0.0.0");
        assert_eq!(parsed.config.port, 6380);
        assert_eq!(parsed.config.max_memory, 64 * 1024 * 1024);
        assert_eq!(parsed.config.eviction_policy, EvictionPolicy::VolatileLru);
        assert_eq!(parsed.config.sweep_interval_ms, 250);
        assert_eq!(parsed.config.max_request_bytes, 2048);
        assert_eq!(parsed.config.idle_timeout_ms, 1000);
    }

    #[test]
    fn uses_defaults() {
        let parsed = parse_args(&[], &HashMap::new()).unwrap();
        assert_eq!(parsed.config, ServerConfig::default());
    }

    #[test]
    fn rejects_invalid_options() {
        assert!(parse_args(&["--unknown".to_string()], &HashMap::new()).is_err());
        assert!(parse_args(&["--port".to_string()], &HashMap::new()).is_err());
        assert!(parse_args(
            &["--eviction".to_string(), "lfu".to_string()],
            &HashMap::new()
        )
        .is_err());
        assert!(parse_args(&[], &env(&[("PORT", "70000")])).is_err());
    }

    #[test]
    fn reports_help() {
        assert!(
            parse_args(&["--help".to_string()], &HashMap::new())
                .unwrap()
                .help
        );
    }
}
