#!/usr/bin/env bash
# Manages the local Redis Sentinel demo topology (docker compose): start it,
# inspect it, trigger a failover, and run the demo app or test suite against it.
#
# Usage: ./sentinel.sh <command>
#
#   up             Start Redis + sentinels and wait for quorum
#   down           Stop and remove all containers and volumes
#   status         Show current master, replicas, and known sentinels
#   failover       Trigger a forced failover via sentinel-1
#   demo           Run the demo Crystal app inside the Docker network
#   test           Run sentinel integration tests (unit + integration)
#   test-failover  Run the failover integration test (spec/sentinel_failover_spec.cr)
#   logs           Stream logs from all services

set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

sentinel1() {
  docker compose exec -T sentinel-1 redis-cli -p 26379 "$@"
}

# Prints "key: value" for the given comma-separated field names out of a
# redis-cli flat key/value listing (one item per line, keys on odd lines,
# values on even lines) — e.g. the output of `SENTINEL master mymaster`.
filter_fields() {
  awk -v fields="$1" '
    BEGIN { n = split(fields, wanted, ","); for (i = 1; i <= n; i++) want[wanted[i]] = 1 }
    NR % 2 == 1 { key = $0; next }
    key in want { print key ": " $0 }
  '
}

cmd_up() {
  docker compose up -d redis-master redis-replica-1 redis-replica-2 sentinel-1 sentinel-2 sentinel-3
  echo "Waiting for sentinel quorum..."
  docker compose wait sentinel-1 sentinel-2 sentinel-3 2>/dev/null || {
    echo "  (docker compose wait not available, sleeping 15 s)"
    sleep 15
  }
  cmd_status
}

cmd_down() {
  docker compose down
}

cmd_status() {
  echo "=== Current master ==="
  sentinel1 sentinel master mymaster 2>/dev/null | filter_fields "ip,port" || echo "  (sentinel not ready yet)"
  echo
  echo "=== Replicas ==="
  sentinel1 sentinel replicas mymaster 2>/dev/null | filter_fields "ip,port,flags" || echo "  (none)"
  echo
  echo "=== Known sentinels (from sentinel-1) ==="
  sentinel1 sentinel sentinels mymaster 2>/dev/null | filter_fields "ip,port,flags" || echo "  (none)"
}

cmd_failover() {
  echo "Triggering forced failover on mymaster..."
  sentinel1 sentinel failover mymaster
  echo "Waiting for promotion to complete (5 s)..."
  sleep 5
  cmd_status
}

# Works on both Linux and macOS (runs Crystal inside the network, not on the
# host). In a second terminal, run `./sentinel.sh failover` to watch the
# client follow the new master in real time.
cmd_demo() {
  docker compose --profile demo run --rm demo
}

# The slow failover test is in a separate file — use `test-failover` for
# that. Works on both Linux and macOS — Crystal runs inside the Docker
# network. Requires: ./sentinel.sh up
cmd_test() {
  docker compose --profile test run --rm test
}

# Triggers a real sentinel failover and polls for up to 30 s for the client
# to follow the new master.
# Requires: ./sentinel.sh up — do not run immediately after `failover` (need
# a stable topology first).
cmd_test_failover() {
  docker compose --profile test-failover run --rm test-failover
}

cmd_logs() {
  docker compose logs -f
}

usage() {
  sed -n '2,15p' "${BASH_SOURCE[0]}" | sed 's/^#//; s/^ //'
}

case "${1:-}" in
  up) cmd_up ;;
  down) cmd_down ;;
  status) cmd_status ;;
  failover) cmd_failover ;;
  demo) cmd_demo ;;
  test) cmd_test ;;
  test-failover) cmd_test_failover ;;
  logs) cmd_logs ;;
  *)
    usage
    exit 1
    ;;
esac
