#!/usr/bin/env bash
# End-to-end check that -storage.metaindexChunkRows is a RAM-only setting.
#
# One vmstorage data volume is reused by the stock image and the fork image, restarted in this order:
#   stock -> fork N=0 -> fork N=64 -> fork N=256 (ingest more + force merge) -> fork N=0 -> stock -> fork N=64
# At every step the script compares query results with the reference and the sha256 of the indexdb files
# (they must not change between restarts without merges). It prints the indexdb metaindex RAM size per step.
#
# Requires docker (compose v2), curl and python3.
# Env: STOCK_IMAGE (default victoriametrics/vmstorage:v1.155.0-cluster), FORK_IMAGE (required, built from this branch),
#      VM_TAG (tag of vminsert and vmselect, default v1.155.0-cluster).
set -euo pipefail

cd "$(dirname "$0")"
export MSYS_NO_PATHCONV=1

STOCK_IMAGE=${STOCK_IMAGE:-victoriametrics/vmstorage:v1.155.0-cluster}
FORK_IMAGE=${FORK_IMAGE:?set FORK_IMAGE to the image built from the metaindex-chunked branch}
export VM_TAG=${VM_TAG:-v1.155.0-cluster}
export COMPOSE_PROJECT_NAME=metaindex-e2e
DC="docker compose -f docker-compose.yml"

WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
SERIES_PER_BATCH=${SERIES_PER_BATCH:-4000}
LABEL_LEN=${LABEL_LEN:-30000}
T0=$(( $(date +%s) / 3600 * 3600 - 7200 ))

fail() { echo "FAIL: $*" >&2; exit 1; }

# gen BATCH FIRST COUNT: prometheus text format with long label values sharing a long prefix.
gen() {
  python3 - "$1" "$2" "$3" "$T0" "$LABEL_LEN" <<'EOF' > "$WORK/batch$1.txt"
import sys
batch, first, count, t0, label_len = map(int, sys.argv[1:])
prefix = ("/usr/bin/java -cp /opt/docker/lib/some.jar:" * 1000)[:label_len]
for i in range(first, first + count):
    cmd = "%s%06d" % (prefix, i)
    for j in range(2):
        print('e2e_metric{host="h%06d",cmd="%s"} %d %d' % (i, cmd, i + j, (t0 + j * 60) * 1000))
EOF
}

ingest() {
  # Relative path, because curl may not understand the shell's temp dir path on Windows.
  (cd "$WORK" && curl -fsS --data-binary "@batch$1.txt" "http://localhost:18480/insert/0/prometheus/api/v1/import/prometheus" >/dev/null)
  curl -fsS "http://localhost:18482/internal/force_flush" >/dev/null || true
  sleep 3
}

wait_ready() {
  for _ in $(seq 1 120); do
    if curl -fsS "http://localhost:18482/health" >/dev/null 2>&1 && curl -fsS "http://localhost:18481/select/0/prometheus/api/v1/labels" >/dev/null 2>&1 && curl -fsS "http://localhost:18480/health" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  $DC logs --tail 50 vmstorage >&2
  fail "vmstorage is not ready"
}

start_storage() { # IMAGE N
  # Stop and remove first, so the published port is released before the new container binds it.
  $DC rm -sf vmstorage >/dev/null
  sleep 2
  VMSTORAGE_IMAGE=$1 CHUNK_ROWS=$2 $DC up -d --no-deps vmstorage >/dev/null
  wait_ready
  sleep 5
}

norm() { python3 -c '
import json, sys
d = json.load(sys.stdin)
assert d["status"] == "success", d
r = d["data"]
if isinstance(r, dict):
    r = r["result"]
r = sorted(json.dumps(x, sort_keys=True) for x in r) if r and isinstance(r[0], dict) else sorted(r)
print("\n".join(r))
' | sha256sum | cut -d' ' -f1; }

# queries prints one sha256 per query.
queries() {
  local sel="http://localhost:18481/select/0/prometheus/api/v1"
  local end=$((T0 + 7200)) cmd6
  cmd6=$(python3 -c "print(('/usr/bin/java -cp /opt/docker/lib/some.jar:' * 1000)[:$LABEL_LEN] + '000123')")
  curl -fsS -G "$sel/series" --data-urlencode 'match[]=e2e_metric' -d "start=$((T0 - 3600))" -d "end=$end" | norm || return 1
  curl -fsS -G "$sel/series" --data-urlencode "match[]=e2e_metric{cmd=\"$cmd6\"}" -d "start=$((T0 - 3600))" -d "end=$end" | norm || return 1
  curl -fsS -G "$sel/series" --data-urlencode 'match[]=e2e_metric{host=~"h0001.*"}' -d "start=$((T0 - 3600))" -d "end=$end" | norm || return 1
  curl -fsS -G "$sel/labels" -d "start=$((T0 - 3600))" -d "end=$end" | norm || return 1
  curl -fsS -G "$sel/label/host/values" -d "start=$((T0 - 3600))" -d "end=$end" | norm || return 1
  curl -fsS -G "$sel/query_range" --data-urlencode 'query=sum(e2e_metric) by (host)' -d "start=$T0" -d "end=$((T0 + 240))" -d step=60 | norm || return 1
  curl -fsS -G "$sel/query_range" --data-urlencode "query=e2e_metric{cmd=~\"${cmd6:0:100}.*\", host=~\"h00[0-9]{4}\"}" -d "start=$T0" -d "end=$((T0 + 240))" -d step=60 | norm || return 1
}

# indexdb file hashes (the data dir layout differs between versions, so take everything under indexdb).
file_hashes() {
  docker run --rm -v "${COMPOSE_PROJECT_NAME}_strgdata:/storage:ro" alpine:3.24.3 sh -c \
    'cd /storage && find . -path "*indexdb*" -type f | sort | xargs sha256sum' | sha256sum | cut -d' ' -f1
}

part_files_ok() { # every part dir in indexdb must contain only stock file names
  docker run --rm -v "${COMPOSE_PROJECT_NAME}_strgdata:/storage:ro" alpine:3.24.3 sh -c '
    cd /storage && for d in $(find . -path "*indexdb*" -name metaindex.bin -exec dirname {} \;); do
      for f in $(ls "$d"); do
        case "$f" in metaindex.bin|index.bin|items.bin|lens.bin|metadata.json) ;; *) echo "unexpected $d/$f"; exit 1;; esac
      done
    done'
}

metaindex_bytes() {
  curl -fsS http://localhost:18482/metrics | grep '^vm_data_size_bytes{type="indexdb/metaindex"}' | awk '{print $2}'
}

REF=""
HASHES=""
declare -A MI_BYTES

check_step() { # NAME N
  local name=$1 n=$2 q h
  q=$(queries) || fail "$name: a query failed"
  [ "$(echo "$q" | wc -l)" -eq 7 ] || fail "$name: expected 7 query hashes"
  if [ -z "$REF" ]; then
    REF=$q
  elif [ "$q" != "$REF" ]; then
    diff <(echo "$REF") <(echo "$q") || true
    fail "$name: query results differ from the reference"
  fi
  part_files_ok || fail "$name: unexpected files in indexdb parts"
  h=$(file_hashes)
  if [ -n "$HASHES" ] && [ "$h" != "$HASHES" ]; then
    fail "$name: indexdb files changed after a plain restart"
  fi
  HASHES=$h
  MI_BYTES[$name]=$(metaindex_bytes)
  echo "OK   $name (chunkRows=$n) metaindex_ram_bytes=${MI_BYTES[$name]} indexdb_sha=${h:0:12}"
  if $DC logs vmstorage 2>&1 | grep -qE 'panic:|FATAL'; then fail "$name: errors in vmstorage log"; fi
}

echo "pulling stock images"
docker pull -q "$STOCK_IMAGE" >/dev/null
docker pull -q "victoriametrics/vminsert:$VM_TAG" >/dev/null
docker pull -q "victoriametrics/vmselect:$VM_TAG" >/dev/null
docker pull -q alpine:3.24.3 >/dev/null
$DC down -v >/dev/null 2>&1 || true

gen 1 0 "$SERIES_PER_BATCH"
gen 2 "$SERIES_PER_BATCH" "$SERIES_PER_BATCH"

# Step 1: stock vmstorage, ingest, force merge, reference results.
export VMSTORAGE_IMAGE=$STOCK_IMAGE CHUNK_ROWS=0
$DC up -d >/dev/null
wait_ready
ingest 1
curl -fsS "http://localhost:18482/internal/force_merge" >/dev/null; sleep 5
start_storage "$STOCK_IMAGE" 8   # stock must ignore the env var
check_step "1-stock" 8

# Steps 2 and 3: plain restarts (no merges) with the fork.
start_storage "$FORK_IMAGE" 0;   check_step "2-fork-N0" 0
start_storage "$FORK_IMAGE" 64;  check_step "3-fork-N64" 64
[ "${MI_BYTES[3-fork-N64]}" -lt "${MI_BYTES[2-fork-N0]}" ] || fail "metaindex RAM did not drop with N=64 (${MI_BYTES[3-fork-N64]} vs ${MI_BYTES[2-fork-N0]})"

# Step 4: N=256, ingest more, force merge. Results change (more series), so take a new reference.
start_storage "$FORK_IMAGE" 256; check_step "4a-fork-N256" 256
ingest 2
curl -fsS "http://localhost:18482/internal/force_merge" >/dev/null; sleep 5
REF=""; HASHES=""
start_storage "$FORK_IMAGE" 256; check_step "4b-fork-N256-merged" 256

# Steps 5 and 6: back to N=0, then stock opens parts written and merged by the fork.
start_storage "$FORK_IMAGE" 0;   check_step "5-fork-N0" 0
start_storage "$STOCK_IMAGE" 0;  check_step "6-stock" 0
start_storage "$FORK_IMAGE" 64;  check_step "7-fork-N64" 64

echo "metaindex RAM at equal data: N=0 ${MI_BYTES[2-fork-N0]} bytes; N=64 ${MI_BYTES[3-fork-N64]} bytes"
echo "ALL STEPS PASSED"
$DC down -v >/dev/null
