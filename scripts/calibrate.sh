#!/usr/bin/env bash
#
# calibrate.sh — prove loadgen reports the truth before trusting it on the gateway
#
# Every case below runs against examples/refserver.rs, whose behaviour is fixed on
# purpose, so the correct answer is known by arithmetic. A metric that misses its
# predicted value inside the stated tolerance is a defect in the harness, not a
# property of anything being measured. Cases run under both wire protocols where
# both apply. Exits non-zero if any case fails.
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
    "$REFSERVER" --port "$port" --protocol "$PROTO" "$@" 2>"$WORK/ref-$port.log" &
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

loadgen() {
    local port=$1
    shift
    "$LOADGEN" --url "ws://127.0.0.1:$port/ws" --protocol "$PROTO" --json "$@"
}

assert() {
    local label=$1 expected=$2 measured=$3 tol=$4 mode=$5 verdict
    if awk -v e="$expected" -v m="$measured" -v t="$tol" -v mode="$mode" 'BEGIN {
            if (mode == "null") { exit (m == "null") ? 0 : 1 }
            if (m == "null" || m == "") exit 1
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
    printf '%-46s %13s %13s %10s  %s\n' \
        "$PROTO $label" "$expected" "$measured" "$(tolerance_text "$tol" "$mode")" "$verdict" >>"$REPORT"
}

note() {
    printf '%-46s %13s %13s %10s  %s\n' "$PROTO $1" "-" "$2" "-" "observed" >>"$REPORT"
}

tolerance_text() {
    case $2 in
        rel) echo "±$1%" ;;
        abs) echo "±$1" ;;
        null) echo "null" ;;
        *) echo "min" ;;
    esac
}

pct_of() {
    awk -v part="$1" -v whole="$2" 'BEGIN { if (part == "null" || whole == 0) print "null"; else printf "%.4f", part * 100 / whole }'
}

CONNS=200 SENDERS=5 RATE=50 SECS=12 WARMUP=2
MEASURED=$((SECS - WARMUP))
SENT_EXP=$((SENDERS * RATE * MEASURED))
RECV_EXP=$((SENT_EXP * (CONNS - 1)))
THRU_EXP=$((RECV_EXP / MEASURED))
require_fds "$CONNS"

TOPIC_CONNS=200 TOPIC_COUNT=10 TOPIC_SENDERS=10
TOPIC_SENT_EXP=$((TOPIC_SENDERS * RATE * MEASURED))
TOPIC_RECV_EXP=$((TOPIC_SENT_EXP * (TOPIC_CONNS / TOPIC_COUNT - 1)))
TOPIC_MISROUTED_EXP=$((TOPIC_SENT_EXP * (TOPIC_CONNS - 1 - (TOPIC_CONNS / TOPIC_COUNT - 1))))
EXTRA_RATE=10
EXTRA_SENT_EXP=$((EXTRA_RATE * MEASURED))
EXTRA_EXPECTED_EXP=$((EXTRA_SENT_EXP * TOPIC_CONNS))
TOPIC_ARGS=(--connections "$TOPIC_CONNS" --topics "$TOPIC_COUNT" --senders "$TOPIC_SENDERS"
    --rate "$RATE" --seconds "$SECS" --warmup "$WARMUP")

for PROTO in legacy topics; do
    echo "=== $PROTO case 1: counting, throughput and delivery (no delay, no loss) ==="
    PORT=3101
    start_ref "$PORT"
    CASE1=$(loadgen "$PORT" --connections "$CONNS" --senders "$SENDERS" --rate "$RATE" \
        --seconds "$SECS" --warmup "$WARMUP")
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
    assert "case1 unaccounted" 0 "$(json_field "$CASE1" unaccounted)" 0 abs
    note "case1 service p50 ms" "$(json_field "$CASE1" service_p50_ms)"
    note "case1 service p99 ms" "$(json_field "$CASE1" service_p99_ms)"
    note "case1 response p50 ms" "$(json_field "$CASE1" response_p50_ms)"
    note "case1 generator slip mean ms" "$(json_field "$CASE1" slip_mean_ms)"

    echo "=== $PROTO case 9: case 1 with binary frames ==="
    PORT=3109
    start_ref "$PORT"
    CASE9=$(loadgen "$PORT" --connections "$CONNS" --senders "$SENDERS" --rate "$RATE" \
        --seconds "$SECS" --warmup "$WARMUP" --binary)
    stop_ref

    assert "case9 frames published" "$SENT_EXP" "$(json_field "$CASE9" sent)" 0 abs
    assert "case9 frames delivered" "$RECV_EXP" "$(json_field "$CASE9" received)" 0.05 rel
    assert "case9 delivery %" 100 "$(json_field "$CASE9" delivery_pct)" 0.01 abs
    assert "case9 malformed frames" 0 "$(json_field "$CASE9" malformed)" 0 abs
    assert "case9 unaccounted" 0 "$(json_field "$CASE9" unaccounted)" 0 abs

    echo "=== $PROTO case 2: known 50ms delivery delay, low load so the timer is not the bottleneck ==="
    PORT=3102
    start_ref "$PORT" --delay-ms 50
    CASE2=$(loadgen "$PORT" --connections 20 --senders 1 --rate 20 --seconds 12 --warmup 2)
    stop_ref

    assert "case2 service p50 ms" 50 "$(json_field "$CASE2" service_p50_ms)" 2 abs
    assert "case2 service p99 ms" 50 "$(json_field "$CASE2" service_p99_ms)" 5 abs
    assert "case2 delivery %" 100 "$(json_field "$CASE2" delivery_pct)" 0.01 abs
    assert "case2 warnings" 0 "$(json_field "$CASE2" warnings)" 0 abs
    note "case2 measurement overhead ms" \
        "$(awk -v p="$(json_field "$CASE2" service_p50_ms)" 'BEGIN{printf "%.3f", p - 50}')"
    note "case2 response p50 ms" "$(json_field "$CASE2" response_p50_ms)"

    echo "=== $PROTO cases 3 and 7: known silent loss, one delivery in ten discarded ==="
    PORT=3103
    start_ref "$PORT" --drop-1-in 10
    CASE3=$(loadgen "$PORT" --connections 100 --senders 5 --rate 50 --seconds 12 --warmup 2)
    stop_ref

    if [ "$PROTO" = legacy ]; then SCOPE_EXP=95; else SCOPE_EXP=100; fi
    assert "case3 delivery %" 90 "$(json_field "$CASE3" delivery_pct)" 0.2 abs
    assert "case3 warnings" 0 "$(json_field "$CASE3" warnings)" 0 abs
    assert "case7 dropped" 0 "$(json_field "$CASE3" dropped)" 0 abs
    assert "case7 unaccounted scope" "$SCOPE_EXP" "$(json_field "$CASE3" unaccounted_scope)" 0 abs
    assert "case7 unaccounted % of scope expected" 10 \
        "$(pct_of "$(json_field "$CASE3" unaccounted)" "$(json_field "$CASE3" scope_expected)")" 0.2 abs

    echo "=== $PROTO case 8: the same loss, reported with warnings ==="
    PORT=3108
    start_ref "$PORT" --drop-1-in 10 --warn-drops
    CASE8=$(loadgen "$PORT" --connections 100 --senders 5 --rate 50 --seconds 12 --warmup 2)
    stop_ref

    assert "case8 delivery %" 90 "$(json_field "$CASE8" delivery_pct)" 0.2 abs
    assert "case8 dropped % of scope expected" 10 \
        "$(pct_of "$(json_field "$CASE8" dropped)" "$(json_field "$CASE8" scope_expected)")" 0.5 abs
    assert "case8 unaccounted" 0 "$(json_field "$CASE8" unaccounted)" 0 abs

    # No single frame can absorb the whole freeze: the worst-placed one still arrives up to one
    # inter-arrival gap after the freeze began, so the predicted floor is 500ms - 2.5ms, not 500ms.
    echo "=== $PROTO case 4: 500ms freeze mid-window, publishers back-pressured ==="
    PORT=3104
    start_ref "$PORT" --stall-at 8 --stall-ms 500
    CASE4=$(loadgen "$PORT" --connections 10 --senders 2 --rate 200 --payload-bytes 32768 \
        --seconds 14 --warmup 2)
    stop_ref

    assert "case4 response max ms (freeze-gap)" 497 "$(json_field "$CASE4" response_max_ms)" 0 min
    assert "case4 publisher slip ms >= 250" 250 "$(json_field "$CASE4" slip_max_ms)" 0 min
    note "case4 service max ms" "$(json_field "$CASE4" service_max_ms)"
    note "case4 response p99 ms" "$(json_field "$CASE4" response_p99_ms)"
    note "case4 delivery %" "$(json_field "$CASE4" delivery_pct)"

    echo "=== $PROTO case 11: 2s delivery delay with a 500ms drain limit must not drain ==="
    PORT=3111
    start_ref "$PORT" --delay-ms 2000
    CASE11=$(loadgen "$PORT" --connections 20 --senders 1 --rate 20 --seconds 12 --warmup 2 \
        --drain-max-ms 500)
    stop_ref

    assert "case11 drained (0 = false)" 0 "$(json_field "$CASE11" drained | sed 's/false/0/;s/true/1/')" 0 abs
    assert "case11 unaccounted is null" null "$(json_field "$CASE11" unaccounted)" 0 null
    assert "case11 churn_failed is null" null "$(json_field "$CASE11" churn_failed)" 0 null

    echo "=== $PROTO case 12: the same delay with the default drain limit drains fully ==="
    PORT=3112
    start_ref "$PORT" --delay-ms 2000
    CASE12=$(loadgen "$PORT" --connections 20 --senders 1 --rate 20 --seconds 12 --warmup 2)
    stop_ref

    assert "case12 drained (1 = true)" 1 "$(json_field "$CASE12" drained | sed 's/false/0/;s/true/1/')" 0 abs
    assert "case12 drain seconds >= 2" 2 "$(json_field "$CASE12" drain_seconds)" 0 min
    assert "case12 delivery %" 100 "$(json_field "$CASE12" delivery_pct)" 0.01 abs
    assert "case12 unaccounted" 0 "$(json_field "$CASE12" unaccounted)" 0 abs

    if [ "$PROTO" = legacy ]; then
        continue
    fi

    echo "=== $PROTO case 5: ten topics of twenty, one sender each ==="
    PORT=3105
    start_ref "$PORT"
    CASE5=$(loadgen "$PORT" "${TOPIC_ARGS[@]}")
    stop_ref

    assert "case5 frames published" "$TOPIC_SENT_EXP" "$(json_field "$CASE5" sent)" 0 abs
    assert "case5 frames delivered" "$TOPIC_RECV_EXP" "$(json_field "$CASE5" received)" 0 abs
    assert "case5 delivery %" 100 "$(json_field "$CASE5" delivery_pct)" 0.01 abs
    assert "case5 misrouted" 0 "$(json_field "$CASE5" misrouted)" 0 abs
    assert "case5 unaccounted" 0 "$(json_field "$CASE5" unaccounted)" 0 abs
    assert "case5 subscribe_failed" 0 "$(json_field "$CASE5" subscribe_failed)" 0 abs
    note "case5 subscribe ack p99 ms" "$(json_field "$CASE5" subscribe_ack_p99_ms)"

    echo "=== $PROTO case 6: case 5 against a server that ignores topics ==="
    PORT=3106
    start_ref "$PORT" --ignore-topics
    CASE6=$(loadgen "$PORT" "${TOPIC_ARGS[@]}")
    stop_ref

    assert "case6 frames delivered" "$TOPIC_RECV_EXP" "$(json_field "$CASE6" received)" 0 abs
    assert "case6 delivery %" 100 "$(json_field "$CASE6" delivery_pct)" 0.01 abs
    assert "case6 misrouted" "$TOPIC_MISROUTED_EXP" "$(json_field "$CASE6" misrouted)" 0 abs

    echo "=== $PROTO case 10: case 5 with twenty churning sockets ==="
    PORT=3110
    start_ref "$PORT"
    CASE10=$(loadgen "$PORT" "${TOPIC_ARGS[@]}" --churn 20 --churn-rate 5)
    stop_ref

    assert "case10 delivery %" 100 "$(json_field "$CASE10" delivery_pct)" 0.01 abs
    assert "case10 unaccounted" 0 "$(json_field "$CASE10" unaccounted)" 0 abs
    assert "case10 churn_failed" 0 "$(json_field "$CASE10" churn_failed)" 0 abs
    assert "case10 misrouted" 0 "$(json_field "$CASE10" misrouted)" 0 abs

    echo "=== $PROTO case 13: case 5 with an extra quiet topic every socket joins ==="
    PORT=3113
    start_ref "$PORT"
    CASE13=$(loadgen "$PORT" "${TOPIC_ARGS[@]}" --extra-topic-rate "$EXTRA_RATE")
    stop_ref

    assert "case13 frames published" "$TOPIC_SENT_EXP" "$(json_field "$CASE13" sent)" 0 abs
    assert "case13 frames delivered" "$TOPIC_RECV_EXP" "$(json_field "$CASE13" received)" 0 abs
    assert "case13 extra sent" "$EXTRA_SENT_EXP" "$(json_field "$CASE13" extra_sent)" 0 abs
    assert "case13 extra expected" "$EXTRA_EXPECTED_EXP" "$(json_field "$CASE13" extra_expected)" 0 abs
    assert "case13 extra delivery %" 100 "$(json_field "$CASE13" extra_delivery_pct)" 0.01 abs
    assert "case13 unaccounted" 0 "$(json_field "$CASE13" unaccounted)" 0 abs
done

echo
printf '%-46s %13s %13s %10s  %s\n' metric predicted measured tolerance verdict
printf '%.0s-' {1..100}; echo
cat "$REPORT"
echo

if [ "$failures" -gt 0 ]; then
    echo "$failures calibration check(s) failed — loadgen numbers are not trustworthy yet"
    exit 1
fi

echo "all calibration checks passed"
