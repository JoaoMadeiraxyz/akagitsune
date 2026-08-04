#!/usr/bin/env bash
#
# calibrate.sh — prove loadgen reports the truth before trusting it on the gateway
#
# Every case below runs against examples/refserver.rs, whose behaviour is fixed on
# purpose, so the correct answer is known by arithmetic. A metric that misses its
# predicted value inside the stated tolerance is a defect in the harness, not a
# property of anything being measured. Exits non-zero if any case fails.
#
# USAGE: scripts/calibrate.sh [--loadgen <path>]

set -euo pipefail

cd "$(dirname "$0")/.."
# shellcheck source=scripts/lib.sh
source scripts/lib.sh

LOADGEN=target/release/examples/loadgen
REFSERVER=target/release/examples/refserver

while [ $# -gt 0 ]; do
    case $1 in
        --loadgen) LOADGEN=$2; shift 2 ;;
        *) echo "unknown argument $1" >&2; exit 2 ;;
    esac
done

cargo build --release --examples >/dev/null

WORK=$(mktemp -d)
REPORT="$WORK/report"
REF_PID=""
failures=0
: >"$REPORT"

cleanup() {
    [ -n "$REF_PID" ] && kill "$REF_PID" 2>/dev/null || true
    rm -rf "$WORK"
}
trap cleanup EXIT

start_ref() {
    local port=$1
    shift
    "$REFSERVER" --port "$port" "$@" 2>"$WORK/ref-$port.log" &
    REF_PID=$!
    wait_for_port "$port" 15
}

stop_ref() {
    sleep 1.5
    kill "$REF_PID" 2>/dev/null || true
    wait "$REF_PID" 2>/dev/null || true
    REF_PID=""
}

ref_deliveries() {
    awk '/^deliveries /{n=$2} END{print n+0}' "$WORK/ref-$1.log"
}

assert() {
    local label=$1 expected=$2 measured=$3 tol=$4 mode=$5 verdict
    if awk -v e="$expected" -v m="$measured" -v t="$tol" -v mode="$mode" 'BEGIN {
            d = m - e; if (d < 0) d = -d
            if (mode == "rel") ok = (e == 0) ? (m == 0) : (d / e * 100 <= t)
            else if (mode == "abs") ok = (d <= t)
            else ok = (m >= e)
            exit ok ? 0 : 1
        }'; then
        verdict=ok
    else
        verdict=FAIL
        failures=$((failures + 1))
    fi
    printf '%-38s %13s %13s %10s  %s\n' \
        "$label" "$expected" "$measured" "$(tolerance_text "$tol" "$mode")" "$verdict" >>"$REPORT"
}

note() {
    printf '%-38s %13s %13s %10s  %s\n' "$1" "-" "$2" "-" "observed" >>"$REPORT"
}

tolerance_text() {
    case $2 in
        rel) echo "±$1%" ;;
        abs) echo "±$1" ;;
        *) echo "min" ;;
    esac
}

echo "=== case 1: counting, throughput and delivery (reference has no delay, no loss) ==="
PORT=3101
CONNS=200 SENDERS=5 RATE=50 SECS=12 WARMUP=2
MEASURED=$((SECS - WARMUP))
SENT_EXP=$((SENDERS * RATE * MEASURED))
RECV_EXP=$((SENT_EXP * (CONNS - 1)))
THRU_EXP=$((RECV_EXP / MEASURED))

require_fds "$CONNS"
start_ref "$PORT"
CASE1=$("$LOADGEN" --url "ws://127.0.0.1:$PORT/ws" --connections "$CONNS" --senders "$SENDERS" \
    --rate "$RATE" --seconds "$SECS" --warmup "$WARMUP" --json)
stop_ref
DELIVERED=$(ref_deliveries "$PORT")

assert "case1 established connections" "$CONNS" "$(json_field "$CASE1" established)" 0 abs
assert "case1 frames published" "$SENT_EXP" "$(json_field "$CASE1" sent)" 0 abs
assert "case1 schedule arithmetic" "$SENT_EXP" "$(json_field "$CASE1" sent_expected)" 0 abs
assert "case1 frames delivered" "$RECV_EXP" "$(json_field "$CASE1" received)" 0.05 rel
assert "case1 delivery %" 100 "$(json_field "$CASE1" delivery_pct)" 0.01 abs
assert "case1 throughput msg/s" "$THRU_EXP" "$(json_field "$CASE1" throughput_msg_s)" 1 rel
assert "case1 backpressure warnings" 0 "$(json_field "$CASE1" warnings)" 0 abs
assert "case1 malformed frames" 0 "$(json_field "$CASE1" malformed)" 0 abs
assert "case1 server-counted deliveries" "$DELIVERED" "$(json_field "$CASE1" received_all)" 0.5 rel
note "case1 service p50 ms" "$(json_field "$CASE1" service_p50_ms)"
note "case1 service p99 ms" "$(json_field "$CASE1" service_p99_ms)"
note "case1 response p50 ms" "$(json_field "$CASE1" response_p50_ms)"
note "case1 generator slip mean ms" "$(json_field "$CASE1" slip_mean_ms)"

echo "=== case 2: known 50ms delivery delay, low load so the timer is not the bottleneck ==="
PORT=3102
start_ref "$PORT" --delay-ms 50
CASE2=$("$LOADGEN" --url "ws://127.0.0.1:$PORT/ws" --connections 20 --senders 1 \
    --rate 20 --seconds 12 --warmup 2 --json)
stop_ref

assert "case2 service p50 ms" 50 "$(json_field "$CASE2" service_p50_ms)" 2 abs
assert "case2 service p99 ms" 50 "$(json_field "$CASE2" service_p99_ms)" 5 abs
assert "case2 delivery %" 100 "$(json_field "$CASE2" delivery_pct)" 0.01 abs
assert "case2 warnings" 0 "$(json_field "$CASE2" warnings)" 0 abs
note "case2 measurement overhead ms" \
    "$(awk -v p="$(json_field "$CASE2" service_p50_ms)" 'BEGIN{printf "%.3f", p - 50}')"
note "case2 response p50 ms" "$(json_field "$CASE2" response_p50_ms)"

echo "=== case 3: known loss, one delivery in ten discarded ==="
PORT=3103
start_ref "$PORT" --drop-1-in 10
CASE3=$("$LOADGEN" --url "ws://127.0.0.1:$PORT/ws" --connections 100 --senders 5 \
    --rate 50 --seconds 12 --warmup 2 --json)
stop_ref

assert "case3 delivery %" 90 "$(json_field "$CASE3" delivery_pct)" 0.2 abs
assert "case3 warnings" 0 "$(json_field "$CASE3" warnings)" 0 abs

# No single frame can absorb the whole freeze: the worst-placed one still arrives up to one
# inter-arrival gap after the freeze began, so the predicted floor is 500ms - 2.5ms, not 500ms.
echo "=== case 4: 500ms freeze mid-window, publishers back-pressured ==="
PORT=3104
start_ref "$PORT" --stall-at 8 --stall-ms 500
CASE4=$("$LOADGEN" --url "ws://127.0.0.1:$PORT/ws" --connections 10 --senders 2 \
    --rate 200 --payload-bytes 32768 --seconds 14 --warmup 2 --json)
stop_ref

assert "case4 response max ms (freeze-gap)" 497 "$(json_field "$CASE4" response_max_ms)" 0 min
assert "case4 publisher slip ms >= 250" 250 "$(json_field "$CASE4" slip_max_ms)" 0 min
note "case4 service max ms" "$(json_field "$CASE4" service_max_ms)"
note "case4 response p99 ms" "$(json_field "$CASE4" response_p99_ms)"
note "case4 delivery %" "$(json_field "$CASE4" delivery_pct)"

echo
printf '%-38s %13s %13s %10s  %s\n' metric predicted measured tolerance verdict
printf '%.0s-' {1..92}; echo
cat "$REPORT"
echo

if [ "$failures" -gt 0 ]; then
    echo "$failures calibration check(s) failed — loadgen numbers are not trustworthy yet"
    exit 1
fi

echo "all calibration checks passed"
