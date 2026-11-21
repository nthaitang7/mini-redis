use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use mini_redis::protocol::{encode_command, ReplyReader, RespValue};

#[derive(Debug, Clone)]
struct LoadOptions {
    host: String,
    port: u16,
    label: String,
    clients: usize,
    pipeline: usize,
    duration_ms: u64,
    warmup_ms: u64,
    keyspace: usize,
    value_size: usize,
    set_ratio: f64,
}

const USAGE: &str = "Usage: load [options]

Options:
  --host <host>         Target host (default: 127.0.0.1)
  --port <port>         Target port (default: 6379)
  --label <name>        Result label
  --clients <count>     Concurrent clients (default: 32)
  --pipeline <count>    Commands per round trip (default: 16)
  --duration <seconds>  Measurement duration (default: 5)
  --warmup <seconds>    Warmup duration (default: 1)
  --keyspace <count>    Number of preloaded keys (default: 10000)
  --value-size <bytes>  Value size for SET commands (default: 64)
  --set-ratio <ratio>   Fraction of SET commands, 0 to 1 (default: 0.2)
";

struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(if seed == 0 { 1 } else { seed })
    }

    fn next(&mut self) -> u64 {
        let mut value = self.0;
        value ^= value << 13;
        value ^= value >> 7;
        value ^= value << 17;
        self.0 = value;
        value
    }

    fn fraction(&mut self) -> f64 {
        self.next() as f64 / u64::MAX as f64
    }
}

struct Connection {
    stream: TcpStream,
    reader: ReplyReader,
    errors: u64,
}

impl Connection {
    fn connect(host: &str, port: u16) -> Result<Self, String> {
        let stream = TcpStream::connect((host, port))
            .map_err(|error| format!("connect to {host}:{port}: {error}"))?;
        let _ = stream.set_nodelay(true);
        Ok(Self {
            stream,
            reader: ReplyReader::new(),
            errors: 0,
        })
    }

    fn send_batch(&mut self, payload: &[u8], replies: usize) -> Result<(), String> {
        self.stream
            .write_all(payload)
            .map_err(|error| format!("write failed: {error}"))?;
        let mut received = 0;
        let mut buffer = [0u8; 16 * 1024];

        while received < replies {
            if let Some(reply) = self.reader.read().map_err(|error| error.to_string())? {
                if matches!(reply, RespValue::Error(_)) {
                    self.errors += 1;
                }

                received += 1;
                continue;
            }

            let read = self
                .stream
                .read(&mut buffer)
                .map_err(|error| format!("read failed: {error}"))?;

            if read == 0 {
                return Err("connection closed".to_string());
            }

            self.reader.push(&buffer[..read]);
        }

        Ok(())
    }
}

struct WorkerStats {
    commands: u64,
    reads: u64,
    writes: u64,
    durations: Vec<u64>,
    errors: u64,
}

fn main() {
    let options = match parse_options() {
        Ok(options) => options,
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(2);
        }
    };

    if let Err(error) = run(options) {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

fn run(options: LoadOptions) -> Result<(), String> {
    let value = vec![b'x'; options.value_size];
    let mut preload = Connection::connect(&options.host, options.port)?;
    let preload_batch = 512;

    for start in (0..options.keyspace).step_by(preload_batch) {
        let count = preload_batch.min(options.keyspace - start);
        let mut payload = Vec::new();

        for index in start..start + count {
            let key = format!("bench:{index}");
            payload.extend_from_slice(&encode_command(&[
                b"SET".as_slice(),
                key.as_bytes(),
                value.as_slice(),
            ]));
        }

        preload.send_batch(&payload, count)?;
    }

    if preload.errors > 0 {
        return Err(format!("preload received {} error replies", preload.errors));
    }

    drop(preload);

    let barrier = Arc::new(Barrier::new(options.clients + 1));
    let mut handles = Vec::with_capacity(options.clients);

    for worker_id in 0..options.clients {
        let worker_options = options.clone();
        let worker_value = value.clone();
        let worker_barrier = Arc::clone(&barrier);

        handles.push(thread::spawn(move || {
            run_worker(worker_id, worker_options, worker_value, worker_barrier)
        }));
    }

    barrier.wait();
    let started = Instant::now();
    let mut results = Vec::with_capacity(options.clients);

    for handle in handles {
        match handle.join() {
            Ok(Ok(result)) => results.push(result),
            Ok(Err(error)) => return Err(error),
            Err(_) => return Err("a benchmark worker panicked".to_string()),
        }
    }

    let elapsed = started.elapsed();
    let commands: u64 = results.iter().map(|result| result.commands).sum();
    let reads: u64 = results.iter().map(|result| result.reads).sum();
    let writes: u64 = results.iter().map(|result| result.writes).sum();
    let errors: u64 = results.iter().map(|result| result.errors).sum();
    let mut durations: Vec<u64> = results
        .into_iter()
        .flat_map(|result| result.durations)
        .collect();
    durations.sort_unstable();

    let throughput = if elapsed.as_secs_f64() == 0.0 {
        0.0
    } else {
        commands as f64 / elapsed.as_secs_f64()
    };

    println!("label: {}", options.label);
    println!("target: {}:{}", options.host, options.port);
    println!(
        "workload: {} clients, pipeline {}, {:.0}% SET, {} keys",
        options.clients,
        options.pipeline,
        options.set_ratio * 100.0,
        options.keyspace
    );
    println!("commands: {commands} ({reads} GET, {writes} SET)");
    println!("elapsed: {:.2}s", elapsed.as_secs_f64());
    println!("throughput: {:.0} ops/sec", throughput);
    println!(
        "batch latency: p50 {:.2} ms, p95 {:.2} ms, p99 {:.2} ms",
        percentile(&durations, 0.50) as f64 / 1000.0,
        percentile(&durations, 0.95) as f64 / 1000.0,
        percentile(&durations, 0.99) as f64 / 1000.0
    );
    println!("errors: {errors}");

    if errors > 0 {
        Err(format!("benchmark received {errors} error replies"))
    } else {
        Ok(())
    }
}

fn run_worker(
    worker_id: usize,
    options: LoadOptions,
    value: Vec<u8>,
    barrier: Arc<Barrier>,
) -> Result<WorkerStats, String> {
    let mut connection = match Connection::connect(&options.host, options.port) {
        Ok(connection) => connection,
        Err(error) => {
            barrier.wait();
            return Err(error);
        }
    };
    let mut random =
        Rng::new(0x9e37_79b9_7f4a_7c15 ^ (worker_id as u64 + 1).wrapping_mul(2_654_435_761));

    let warmup_end = Instant::now() + Duration::from_millis(options.warmup_ms);

    while Instant::now() < warmup_end {
        if let Err(error) = run_batch(&mut connection, &options, &value, &mut random) {
            barrier.wait();
            return Err(error);
        }
    }

    barrier.wait();
    let mut reads = 0u64;
    let mut writes = 0u64;

    let mut commands = 0u64;
    let mut durations = Vec::new();
    let end = Instant::now() + Duration::from_millis(options.duration_ms);

    while Instant::now() < end {
        let started = Instant::now();
        let (batch_reads, batch_writes) =
            run_batch(&mut connection, &options, &value, &mut random)?;
        durations.push(started.elapsed().as_micros() as u64);
        reads += batch_reads;
        writes += batch_writes;
        commands += options.pipeline as u64;
    }

    Ok(WorkerStats {
        commands,
        reads,
        writes,
        durations,
        errors: connection.errors,
    })
}

fn run_batch(
    connection: &mut Connection,
    options: &LoadOptions,
    value: &[u8],
    random: &mut Rng,
) -> Result<(u64, u64), String> {
    let mut payload = Vec::new();
    let mut reads = 0u64;
    let mut writes = 0u64;

    for _ in 0..options.pipeline {
        let key = format!("bench:{}", random.next() as usize % options.keyspace);

        if random.fraction() < options.set_ratio {
            payload.extend_from_slice(&encode_command(&[b"SET".as_slice(), key.as_bytes(), value]));
            writes += 1;
        } else {
            payload.extend_from_slice(&encode_command(&[b"GET".as_slice(), key.as_bytes()]));
            reads += 1;
        }
    }

    connection.send_batch(&payload, options.pipeline)?;
    Ok((reads, writes))
}

fn percentile(sorted: &[u64], fraction: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }

    let index = ((fraction * sorted.len() as f64).ceil() as usize)
        .saturating_sub(1)
        .min(sorted.len() - 1);
    sorted[index]
}

fn parse_options() -> Result<LoadOptions, String> {
    let mut options = LoadOptions {
        host: "127.0.0.1".to_string(),
        port: 6379,
        label: "mini_redis".to_string(),
        clients: 32,
        pipeline: 16,
        duration_ms: 5000,
        warmup_ms: 1000,
        keyspace: 10_000,
        value_size: 64,
        set_ratio: 0.2,
    };

    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut index = 0;

    while index < args.len() {
        let option = args[index].as_str();

        match option {
            "--help" | "-h" => {
                print!("{USAGE}");
                std::process::exit(0);
            }
            "--host" => {
                options.host = required_value(&args, index, option)?.clone();
                index += 2;
            }
            "--port" => {
                options.port = required_value(&args, index, option)?
                    .parse()
                    .map_err(|_| "--port must be a number".to_string())?;
                index += 2;
            }
            "--label" => {
                options.label = required_value(&args, index, option)?.clone();
                index += 2;
            }
            "--clients" => {
                options.clients = parse_usize(required_value(&args, index, option)?, "--clients")?;
                index += 2;
            }
            "--pipeline" => {
                options.pipeline =
                    parse_usize(required_value(&args, index, option)?, "--pipeline")?;
                index += 2;
            }
            "--duration" => {
                options.duration_ms =
                    (parse_f64(required_value(&args, index, option)?, "--duration")? * 1000.0)
                        as u64;
                index += 2;
            }
            "--warmup" => {
                options.warmup_ms =
                    (parse_f64(required_value(&args, index, option)?, "--warmup")? * 1000.0) as u64;
                index += 2;
            }
            "--keyspace" => {
                options.keyspace =
                    parse_usize(required_value(&args, index, option)?, "--keyspace")?;
                index += 2;
            }
            "--value-size" => {
                options.value_size =
                    parse_usize(required_value(&args, index, option)?, "--value-size")?;
                index += 2;
            }
            "--set-ratio" => {
                options.set_ratio =
                    parse_f64(required_value(&args, index, option)?, "--set-ratio")?;
                index += 2;
            }
            other => return Err(format!("unknown option: {other}")),
        }
    }

    if options.clients < 1 {
        return Err("--clients must be at least 1".to_string());
    }

    if options.pipeline < 1 {
        return Err("--pipeline must be at least 1".to_string());
    }

    if options.duration_ms < 100 {
        return Err("--duration must be at least 0.1 seconds".to_string());
    }

    if options.keyspace < 1 {
        return Err("--keyspace must be at least 1".to_string());
    }

    if options.value_size < 1 {
        return Err("--value-size must be at least 1".to_string());
    }

    if !(0.0..=1.0).contains(&options.set_ratio) {
        return Err("--set-ratio must be between 0 and 1".to_string());
    }

    Ok(options)
}

fn parse_usize(value: &str, name: &str) -> Result<usize, String> {
    value
        .parse()
        .map_err(|_| format!("{name} requires a positive integer"))
}

fn parse_f64(value: &str, name: &str) -> Result<f64, String> {
    value
        .parse()
        .map_err(|_| format!("{name} requires a number"))
}

fn required_value<'a>(
    args: &'a [String],
    index: usize,
    option: &str,
) -> Result<&'a String, String> {
    args.get(index + 1)
        .ok_or_else(|| format!("{option} requires a value"))
}
