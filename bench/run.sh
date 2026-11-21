#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

HOST="${HOST:-127.0.0.1}"
MINI_PORT="${MINI_PORT:-6390}"
BASELINE_PORT="${BASELINE_PORT:-6391}"
REDIS_PORT="${REDIS_PORT:-6389}"
CLIENTS="${CLIENTS:-32}"
PIPELINE="${PIPELINE:-16}"
DURATION="${DURATION:-5}"
WARMUP="${WARMUP:-1}"
KEYSPACE="${KEYSPACE:-10000}"
VALUE_SIZE="${VALUE_SIZE:-64}"
SET_RATIO="${SET_RATIO:-0.2}"
WITH_REDIS="${WITH_REDIS:-0}"

cargo build --release

TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/mini-redis-rs-bench.XXXXXX")"
PIDS=()
REDIS_CONTAINER="mini-redis-rs-bench-$$"
REDIS_STARTED=0

cleanup() {
  if (( ${#PIDS[@]} > 0 )); then
    for pid in "${PIDS[@]}"; do
      kill "$pid" 2>/dev/null || true
    done
  fi

  if [[ "$REDIS_STARTED" == "1" ]]; then
    docker rm -f "$REDIS_CONTAINER" >/dev/null 2>&1 || true
  fi

  rm -rf "$TMP_DIR"
}

trap cleanup EXIT

wait_for_port() {
  local port="$1"

  for _ in $(seq 1 100); do
    if nc -z "$HOST" "$port" >/dev/null 2>&1; then
      return 0
    fi

    sleep 0.1
  done

  return 1
}

run_load() {
  local label="$1"
  local port="$2"

  target/release/load \
    --label "$label" \
    --host "$HOST" \
    --port "$port" \
    --clients "$CLIENTS" \
    --pipeline "$PIPELINE" \
    --duration "$DURATION" \
    --warmup "$WARMUP" \
    --keyspace "$KEYSPACE" \
    --value-size "$VALUE_SIZE" \
    --set-ratio "$SET_RATIO"
}

echo "mini-redis Rust benchmark"
echo "workload: $CLIENTS clients, pipeline $PIPELINE, ${DURATION}s measured, ${WARMUP}s warmup, ${SET_RATIO} SET ratio"
echo ""

HOST="$HOST" PORT="$MINI_PORT" MAXMEMORY=0 EVICTION_POLICY=allkeys-lru target/release/mini-redis >"$TMP_DIR/mini.log" 2>&1 &
PIDS+=("$!")

if ! wait_for_port "$MINI_PORT"; then
  cat "$TMP_DIR/mini.log"
  echo "mini-redis failed to start"
  exit 1
fi

target/release/baseline --host "$HOST" --port "$BASELINE_PORT" >"$TMP_DIR/baseline.log" 2>&1 &
PIDS+=("$!")

if ! wait_for_port "$BASELINE_PORT"; then
  cat "$TMP_DIR/baseline.log"
  echo "baseline server failed to start"
  exit 1
fi

if [[ "$WITH_REDIS" == "1" ]]; then
  if docker info >/dev/null 2>&1; then
    docker run --rm -d --name "$REDIS_CONTAINER" -p "${REDIS_PORT}:6379" redis:7-alpine >/dev/null
    REDIS_STARTED=1

    if ! wait_for_port "$REDIS_PORT"; then
      echo "redis baseline failed to start"
      exit 1
    fi
  else
    echo "Docker is not running, skipping the Redis baseline"
    echo ""
  fi
fi

run_load "mini_redis_rust" "$MINI_PORT"
echo ""
run_load "baseline_map" "$BASELINE_PORT"

if [[ "$WITH_REDIS" == "1" && "$REDIS_STARTED" == "1" ]]; then
  echo ""
  run_load "redis_7" "$REDIS_PORT"
fi

echo ""
echo "Latency values are round-trip times for one pipelined batch, not per-command latency."
echo "Run with WITH_REDIS=1 to include a Redis 7 container in the comparison."
