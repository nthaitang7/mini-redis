# mini redis (Rust)

An in-memory key-value cache server that speaks RESP2 over TCP. It supports binary-safe string keys, TTLs, concurrent connections, a configurable memory limit, and LRU eviction.

This is a focused cache server, not a full Redis replacement. There is no persistence, replication, cluster support, pub/sub, transactions, scripting, or authentication.

The crate uses only the Rust standard library and builds without external dependencies.

## Requirements

- A Rust toolchain with Cargo
- A Unix-like environment for the shell benchmark script

## Quick start

```sh
cargo run --release --bin mini-redis
```

The server listens on `127.0.0.1:6379` by default. Send requests with a RESP2 Redis client, or with a raw TCP tool.

```sh
printf '*1\r\n$4\r\nPING\r\n' | nc 127.0.0.1 6379
printf 'SET greeting hello EX 60\r\nGET greeting\r\n' | nc 127.0.0.1 6379
```

A command-line smoke test:

```sh
printf 'SET key value\r\nGET key\r\nDEL key\r\nGET key\r\n' | nc 127.0.0.1 6379
```

## Commands

| Command | Description |
| --- | --- |
| `PING [message]` | Health check. |
| `ECHO message` | Return the message. |
| `GET key` | Get a value, or nil when the key is missing or expired. |
| `SET key value [EX seconds \| PX milliseconds]` | Set a value and optionally an expiry. A plain `SET` clears any previous expiry. |
| `DEL key [key ...]` | Delete keys and return how many existed. |
| `DELETE key [key ...]` | Alias for `DEL`. |
| `EXISTS key [key ...]` | Count existing keys. Repeated keys count repeatedly. |
| `EXPIRE key seconds` | Set a TTL in seconds. A value at or below zero deletes the key. |
| `PEXPIRE key milliseconds` | Set a TTL in milliseconds. |
| `TTL key` | Remaining TTL in seconds, `-1` for no expiry, `-2` for a missing key. |
| `PTTL key` | Remaining TTL in milliseconds. |
| `PERSIST key` | Remove a TTL. |
| `TYPE key` | Return `string` or `none`. |
| `DBSIZE` | Number of live keys. |
| `FLUSHDB` / `FLUSHALL` | Remove all keys. |
| `SELECT 0` | Select the only database. |
| `INFO [server\|memory\|stats\|keyspace]` | Server counters and memory information. |
| `CLIENT SETINFO`, `CLIENT SETNAME`, `CLIENT GETNAME` | Minimal client handshake support. |
| `COMMAND` | Returns an empty array. |
| `QUIT` | Close the connection. |

Keys and values are byte strings. Inline commands are accepted for convenience, but RESP2 is the normal protocol.

## Configuration

Environment variables:

| Variable | Default | Description |
| --- | --- | --- |
| `HOST` | `127.0.0.1` | Bind address. |
| `PORT` | `6379` | TCP port. |
| `MAXMEMORY` | `0` | Memory limit such as `64mb`, `512kb`, or `0` for unlimited. |
| `EVICTION_POLICY` | `allkeys-lru` | `noeviction`, `allkeys-lru`, or `volatile-lru`. |
| `SWEEP_INTERVAL_MS` | `1000` | Background expired-key sweep interval. `0` disables active sweeping. |
| `MAX_REQUEST_BYTES` | `1048576` | Maximum size of one request. |
| `IDLE_TIMEOUT_MS` | `0` | Close idle clients after this long. `0` disables it. |

The same options are available as command-line flags:

```sh
cargo run --release --bin mini-redis -- --host 127.0.0.1 --port 6380 --maxmemory 64mb --eviction allkeys-lru
```

Run with `--help` for the complete list.

### Memory accounting

Memory use is an estimate: the key's byte length, the value's byte length, and a fixed 96-byte overhead per entry. This keeps accounting fast and predictable, but it is not an exact measure of allocator or process memory.

When a write would exceed `MAXMEMORY`:

- `noeviction` returns an OOM error and leaves the old value untouched.
- `allkeys-lru` evicts the least recently used keys until the write fits.
- `volatile-lru` evicts only keys with a TTL. If that is not enough, the write fails with an OOM error.

Eviction candidates are selected and checked before anything is removed, so a failed write does not partially evict the cache. Candidate selection scans the key set and sorts by access stamp, so eviction is O(n log n). Normal reads and writes are O(1) plus TTL maintenance.

### Expiration

Expired keys are removed lazily when accessed and actively by a background sweep. Expiry records live in a binary min-heap, and stale records from overwritten keys are ignored by a version check. The heap is compacted when stale records grow beyond a fixed multiple of the key count.

## Concurrency

The server uses one OS thread per client connection. Cache state is shared through `Arc<Mutex<Store>>`, so each command executes atomically with respect to other clients. Network writes happen outside the lock.

The listener is nonblocking so the accept loop can observe the shutdown flag used by integration tests. Accepted streams are switched back to blocking mode.

## Tests

```sh
cargo test
```

The suite covers RESP parsing across chunk boundaries, pipelining, binary values, malformed requests, TTL behavior, stale expiry records, memory accounting, all eviction policies, real TCP connections, concurrent clients, and protocol error isolation.

## Benchmark

```sh
./bench/run.sh
```

The script builds the release binaries, starts the cache server and a bare-map RESP baseline, preloads data, warms up, and runs a mixed GET/SET workload against both. The baseline uses the same parser and TCP stack but no TTLs, memory accounting, or eviction.

Defaults are 32 clients, pipeline depth 16, a 20 percent SET ratio, 10,000 keys, 64-byte values, 1 second of warmup, and a 5 second measurement. Override them with environment variables:

```sh
CLIENTS=64 PIPELINE=32 DURATION=10 SET_RATIO=0.1 ./bench/run.sh
```

To include Redis 7 when Docker is running:

```sh
WITH_REDIS=1 ./bench/run.sh
```

The benchmark reports aggregate throughput and round-trip latency for a pipelined batch. It is a relative comparison on one machine, not a universal performance claim. The client and server can share CPU resources, and a Docker Redis instance adds container networking overhead.

### Sample result

From a local run on macOS with Rust 1.99.1, 32 clients, pipeline 16, 20 percent SET, 10,000 keys, and a 3 second measurement:

| Target | Throughput | p50 batch latency | p95 batch latency | Errors |
| --- | ---: | ---: | ---: | ---: |
| mini_redis_rust | 74,284 ops/sec | 6.90 ms | 7.22 ms | 0 |
| baseline_map | 74,886 ops/sec | 6.83 ms | 7.17 ms | 0 |

The cache features add a small amount of overhead over the bare map server in this workload. Absolute numbers depend heavily on the client, CPU, and run length.

## Behavior and limits

- Commands for a connection are processed in order. Pipelining is supported.
- The server binds to loopback by default and has no authentication. Do not expose it to untrusted networks.
- The wire protocol is RESP2. RESP3 negotiation and its map, set, and push types are not implemented.
- Keys are stored in a single logical database. `SELECT` accepts only database `0`.
- TTL values are wall-clock based. Nothing is persisted across restarts.
- SIGINT and SIGTERM use the operating system default process termination; there is no custom graceful-drain handler.
