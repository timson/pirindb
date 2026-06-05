#!/usr/bin/env bash

set -euo pipefail

HOST="127.0.0.1"
PORT="6379"
DB="0"
TOTAL_KEYS="1000000"
WORKERS_PER_SHARD="2"
KEY_PREFIX="cluster-demo:key"
VALUE_PREFIX="value"
SLOT_SPLIT="8191"

usage() {
  cat <<'EOF'
Usage:
  bash playground/load_cluster_keys_redis_cli.sh [options]

Options:
  --host HOST               Cluster entrypoint host (default: 127.0.0.1)
  --port PORT               Cluster entrypoint redis port (default: 6379)
  --db DB                   Redis logical DB (default: 0)
  --total-keys N            Total keys to insert across both shards (default: 1000000)
  --workers-per-shard N     Parallel redis-cli writers per shard (default: 2)
  --key-prefix PREFIX       Key prefix before the hash tag (default: cluster-demo:key)
  --value-prefix PREFIX     Value prefix (default: value)
  --slot-split SLOT         Last slot owned by shard A (default: 8191)
  -h, --help                Show this help

Notes:
  - The script assumes a 2-shard cluster:
      shard A owns slots 0..SLOT_SPLIT
      shard B owns slots SLOT_SPLIT+1..16383
  - Keys are inserted evenly across both shards by choosing hash tags that map
    into each shard's slot range.
  - This uses plain `redis-cli -c`, not `--pipe`, because cluster redirects
    are the point of the test.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --host)
      HOST="$2"
      shift 2
      ;;
    --port)
      PORT="$2"
      shift 2
      ;;
    --db)
      DB="$2"
      shift 2
      ;;
    --total-keys)
      TOTAL_KEYS="$2"
      shift 2
      ;;
    --workers-per-shard)
      WORKERS_PER_SHARD="$2"
      shift 2
      ;;
    --key-prefix)
      KEY_PREFIX="$2"
      shift 2
      ;;
    --value-prefix)
      VALUE_PREFIX="$2"
      shift 2
      ;;
    --slot-split)
      SLOT_SPLIT="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if ! [[ "$TOTAL_KEYS" =~ ^[0-9]+$ ]] || (( TOTAL_KEYS <= 0 )); then
  echo "--total-keys must be a positive integer" >&2
  exit 1
fi
if ! [[ "$WORKERS_PER_SHARD" =~ ^[0-9]+$ ]] || (( WORKERS_PER_SHARD <= 0 )); then
  echo "--workers-per-shard must be a positive integer" >&2
  exit 1
fi
if ! [[ "$SLOT_SPLIT" =~ ^[0-9]+$ ]] || (( SLOT_SPLIT < 0 || SLOT_SPLIT >= 16383 )); then
  echo "--slot-split must be between 0 and 16382" >&2
  exit 1
fi

redis_cli() {
  redis-cli -c -h "$HOST" -p "$PORT" -n "$DB" --raw "$@"
}

keyslot_for_tag() {
  local tag="$1"
  redis_cli CLUSTER KEYSLOT "probe:{${tag}}" | tr -d '\r'
}

find_tag_for_range() {
  local start_slot="$1"
  local end_slot="$2"
  local seed="$3"
  local candidate slot

  for ((i = 0; i < 200000; i++)); do
    candidate="${seed}-${i}"
    slot="$(keyslot_for_tag "$candidate")"
    if [[ "$slot" =~ ^[0-9]+$ ]] && (( slot >= start_slot && slot <= end_slot )); then
      printf '%s %s\n' "$candidate" "$slot"
      return 0
    fi
  done

  echo "failed to find a tag for slot range ${start_slot}-${end_slot}" >&2
  return 1
}

worker_key_count() {
  local total="$1"
  local workers="$2"
  local index="$3"
  local base=$(( total / workers ))
  local extra=$(( total % workers ))
  if (( index < extra )); then
    echo $(( base + 1 ))
  else
    echo "$base"
  fi
}

run_loader_worker() {
  local label="$1"
  local tag="$2"
  local start_index="$3"
  local count="$4"
  local key_prefix="$5"
  local value_prefix="$6"

  local full_prefix="${key_prefix}:{${tag}}"
  echo "[$label] loading ${count} keys with tag=${tag} start=${start_index}" >&2
  awk \
    -v start="${start_index}" \
    -v count="${count}" \
    -v key_prefix="${full_prefix}" \
    -v value_prefix="${value_prefix}" '
      BEGIN {
        end = start + count
        for (i = start; i < end; i++) {
          printf "SET %s:%d %s-%d\n", key_prefix, i, value_prefix, i
        }
      }
    ' | redis_cli > /dev/null
  echo "[$label] done" >&2
}

left_total=$(( TOTAL_KEYS / 2 ))
right_total=$(( TOTAL_KEYS - left_total ))
right_start=$left_total

echo "Discovering shard-specific hash tags via CLUSTER KEYSLOT..." >&2

declare -a left_tags=()
declare -a right_tags=()

for ((worker = 0; worker < WORKERS_PER_SHARD; worker++)); do
  read -r left_tag left_slot < <(find_tag_for_range 0 "$SLOT_SPLIT" "shard-a-worker-${worker}")
  left_tags+=("${left_tag}:${left_slot}")
  read -r right_tag right_slot < <(find_tag_for_range $(( SLOT_SPLIT + 1 )) 16383 "shard-b-worker-${worker}")
  right_tags+=("${right_tag}:${right_slot}")
done

echo "Shard A tags:" >&2
printf '  %s\n' "${left_tags[@]}" >&2
echo "Shard B tags:" >&2
printf '  %s\n' "${right_tags[@]}" >&2

declare -a pids=()

left_offset=0
for ((worker = 0; worker < WORKERS_PER_SHARD; worker++)); do
  count="$(worker_key_count "$left_total" "$WORKERS_PER_SHARD" "$worker")"
  tag="${left_tags[$worker]%:*}"
  run_loader_worker "shard-a/${worker}" "$tag" "$left_offset" "$count" "$KEY_PREFIX" "$VALUE_PREFIX" &
  pids+=("$!")
  left_offset=$(( left_offset + count ))
done

right_offset="$right_start"
for ((worker = 0; worker < WORKERS_PER_SHARD; worker++)); do
  count="$(worker_key_count "$right_total" "$WORKERS_PER_SHARD" "$worker")"
  tag="${right_tags[$worker]%:*}"
  run_loader_worker "shard-b/${worker}" "$tag" "$right_offset" "$count" "$KEY_PREFIX" "$VALUE_PREFIX" &
  pids+=("$!")
  right_offset=$(( right_offset + count ))
done

for pid in "${pids[@]}"; do
  wait "$pid"
done

echo "Loaded ${TOTAL_KEYS} keys across both shards." >&2
echo "Suggested verification:" >&2
echo "  redis-cli -p 6379 DBSIZE" >&2
echo "  redis-cli -p 6380 DBSIZE" >&2
