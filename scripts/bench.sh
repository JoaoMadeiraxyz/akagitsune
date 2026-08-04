#!/usr/bin/env bash
#
# bench.sh — measure the gateway and record the conditions with the numbers
#
# Calibrates the harness first (scripts/calibrate.sh) and refuses to produce
# baselines if that fails, then runs each scenario against a freshly started
# gateway, samples the server's RSS and CPU alongside, and writes a CSV plus a
# markdown table shaped for the Measured baselines section of docs/architecture.md.
#
# USAGE: scripts/bench.sh [--quick] [--goal] [--all] [--skip-calibration] [--port <N>]
#
# --quick   one cheap fanout run
# --goal    performance-goal and stretch scenarios only (see docs/architecture.md)
# --all     baseline sweep plus goal and stretch scenarios
# default   baseline sweep (latency/cost at known loads, plus the cliff row)

set -euo pipefail

cd "$(dirname "$0")/.."
# shellcheck source=scripts/lib.sh
source scripts/lib.sh

PORT=3000
QUICK=0
GOAL=0
ALL=0
SKIP_CALIBRATION=0

while [ $# -gt 0 ]; do
    case $1 in
        --quick) QUICK=1; shift ;;
        --goal) GOAL=1; shift ;;
        --all) ALL=1; shift ;;
        --skip-calibration) SKIP_CALIBRATION=1; shift ;;
        --port) PORT=$2; shift 2 ;;
        *) echo "unknown argument $1" >&2; exit 2 ;;
    esac
done

MODE_COUNT=$((QUICK + GOAL + ALL))
if [ "$MODE_COUNT" -gt 1 ]; then
    echo "--quick, --goal and --all are mutually exclusive" >&2
    exit 2
fi

GATEWAY=target/release/realtime-gateway
LOADGEN=target/release/examples/loadgen

cargo build --release >/dev/null
cargo build --release --examples >/dev/null

if [ "$SKIP_CALIBRATION" -eq 0 ]; then
    echo "### calibrating the harness before measuring anything"
    if ! scripts/calibrate.sh; then
        echo "calibration failed; refusing to produce baselines from an unverified harness" >&2
        exit 1
    fi
    echo
fi

# name connections senders rate payload_bytes seconds
# offered deliveries/s = senders x rate x (connections - 1)
BASELINE_SCENARIOS=(
    "fanout-200 200 5 50 0 15"
    "fanout-500 500 5 50 0 15"
    "fanout-1000 1000 5 50 0 15"
    "ingest-50 50 50 200 0 15"
    "payload-4k 200 5 50 4096 15"
    "cliff-300 300 30 400 0 15"
)
GOAL_SCENARIOS=(
    "goal-1m-fanout 501 10 200 0 15"
    "goal-1m-ingest 101 50 200 0 15"
    "goal-1m-mesh 201 50 100 0 15"
    "beyond-1.5m-fanout 501 15 200 0 15"
    "beyond-1.5m-ingest 101 50 300 0 15"
    "beyond-1.5m-mesh 251 50 120 0 15"
    "beyond-2m-fanout 1001 10 200 0 15"
    "beyond-2m-mesh 201 50 200 0 15"
    "beyond-3m-explore 301 50 200 0 15"
)
if [ "$QUICK" -eq 1 ]; then
    SCENARIOS=("fanout-200 200 5 50 0 10")
elif [ "$GOAL" -eq 1 ]; then
    SCENARIOS=("${GOAL_SCENARIOS[@]}")
elif [ "$ALL" -eq 1 ]; then
    SCENARIOS=("${BASELINE_SCENARIOS[@]}" "${GOAL_SCENARIOS[@]}")
else
    SCENARIOS=("${BASELINE_SCENARIOS[@]}")
fi

WORK=$(mktemp -d)
GATEWAY_PID=""
mkdir -p bench-results
STAMP=$(date +%Y%m%d-%H%M%S)
if [ "$GOAL" -eq 1 ]; then
    CSV="bench-results/${STAMP}-goal.csv"
elif [ "$ALL" -eq 1 ]; then
    CSV="bench-results/${STAMP}-all.csv"
else
    CSV="bench-results/$STAMP.csv"
fi
COMMIT=$(git rev-parse --short HEAD 2>/dev/null || echo unknown)
DIRTY=$(git status --porcelain 2>/dev/null | head -1)
[ -n "$DIRTY" ] && COMMIT="$COMMIT+dirty"

cleanup() {
    [ -n "$GATEWAY_PID" ] && kill "$GATEWAY_PID" 2>/dev/null || true
    rm -rf "$WORK"
}
trap cleanup EXIT

echo "scenario,connections,senders,rate,payload_bytes,seconds,measured_seconds,\
throughput_msg_s,service_p50_ms,service_p99_ms,service_p999_ms,service_max_ms,\
response_p50_ms,response_p99_ms,response_max_ms,delivery_pct,sent,sent_expected,received,expected,\
warnings,errors,failed_conns,closed_early,slip_mean_ms,slip_max_ms,\
idle_rss_mib,peak_rss_mib,rss_per_conn_kib,gateway_cpu_pct,loadgen_cpu_pct" >"$CSV"

for scenario in "${SCENARIOS[@]}"; do
    read -r NAME CONNS SENDERS RATE PAYLOAD SECS <<<"$scenario"
    require_fds "$CONNS"

    echo "### $NAME: $CONNS connections, $SENDERS senders, $RATE/s, ${PAYLOAD}B padding, ${SECS}s"

    GATEWAY_ADDR="127.0.0.1:$PORT" RUST_LOG=warn "$GATEWAY" >"$WORK/gateway.log" 2>&1 &
    GATEWAY_PID=$!
    wait_for_port "$PORT" 15
    sleep 1
    IDLE_RSS=$(ps -o rss= -p "$GATEWAY_PID" | awk '{printf "%.1f", $1/1024}')

    : >"$WORK/gateway.samples"
    : >"$WORK/loadgen.samples"

    "$LOADGEN" --url "ws://127.0.0.1:$PORT/ws" --connections "$CONNS" --senders "$SENDERS" \
        --rate "$RATE" --payload-bytes "$PAYLOAD" --seconds "$SECS" --json \
        >"$WORK/result.json" 2>"$WORK/loadgen.log" &
    LOADGEN_PID=$!

    sample_process "$GATEWAY_PID" "$WORK/gateway.samples" &
    SAMPLER_GATEWAY=$!
    disown "$SAMPLER_GATEWAY" 2>/dev/null || true
    sample_process "$LOADGEN_PID" "$WORK/loadgen.samples" &
    SAMPLER_LOADGEN=$!
    disown "$SAMPLER_LOADGEN" 2>/dev/null || true

    wait "$LOADGEN_PID" || { echo "loadgen failed:"; cat "$WORK/loadgen.log"; exit 1; }
    kill "$SAMPLER_LOADGEN" 2>/dev/null || true

    PEAK_RSS=$(peak_rss_mib "$WORK/gateway.samples")
    GATEWAY_CPU=$(peak_cpu_pct "$WORK/gateway.samples")
    LOADGEN_CPU=$(peak_cpu_pct "$WORK/loadgen.samples")

    kill "$GATEWAY_PID" 2>/dev/null || true
    wait "$GATEWAY_PID" 2>/dev/null || true
    kill "$SAMPLER_GATEWAY" 2>/dev/null || true
    GATEWAY_PID=""

    RESULT=$(cat "$WORK/result.json")
    PER_CONN=$(awk -v idle="$IDLE_RSS" -v peak="$PEAK_RSS" -v n="$CONNS" \
        'BEGIN{printf "%.1f", (peak - idle) * 1024 / n}')

    for field in measured_seconds throughput_msg_s service_p50_ms service_p99_ms service_p999_ms \
        service_max_ms response_p50_ms response_p99_ms response_max_ms delivery_pct sent sent_expected \
        received expected warnings errors failed_conns closed_early slip_mean_ms slip_max_ms; do
        printf -v "F_$field" '%s' "$(json_field "$RESULT" "$field")"
    done

    echo "$NAME,$CONNS,$SENDERS,$RATE,$PAYLOAD,$SECS,$F_measured_seconds,\
$F_throughput_msg_s,$F_service_p50_ms,$F_service_p99_ms,$F_service_p999_ms,$F_service_max_ms,\
$F_response_p50_ms,$F_response_p99_ms,$F_response_max_ms,$F_delivery_pct,$F_sent,$F_sent_expected,\
$F_received,$F_expected,$F_warnings,$F_errors,$F_failed_conns,$F_closed_early,$F_slip_mean_ms,\
$F_slip_max_ms,$IDLE_RSS,$PEAK_RSS,$PER_CONN,$GATEWAY_CPU,$LOADGEN_CPU" >>"$CSV"

    printf '    %s msg/s   service p50 %sms p99 %sms   delivery %s%%   warnings %s\n' \
        "$F_throughput_msg_s" "$F_service_p50_ms" "$F_service_p99_ms" "$F_delivery_pct" "$F_warnings"
    printf '    gateway rss %s -> %s MiB (%s KiB/conn)   cpu %s%% gateway, %s%% loadgen\n\n' \
        "$IDLE_RSS" "$PEAK_RSS" "$PER_CONN" "$GATEWAY_CPU" "$LOADGEN_CPU"

    sleep 2
done

echo "results written to $CSV"
echo
echo "| Date | Commit | Invocation | Throughput | service p50 | service p99 | Delivery | Warnings |"
echo "|------|--------|------------|------------|-------------|-------------|----------|----------|"
awk -F, -v today="$(date +%Y-%m-%d)" -v commit="$COMMIT" 'NR > 1 {
    printf "| %s | `%s` | `--connections %s --senders %s --rate %s --payload-bytes %s --seconds %s` | %s msg/s | %s ms | %s ms | %s%% | %s |\n",
        today, commit, $2, $3, $4, $5, $6, $8, $9, $10, $16, $21
}' "$CSV"
echo
echo "A row with non-zero warnings is not a throughput figure; it is a record of the cliff."

if [ "$GOAL" -eq 1 ] || [ "$ALL" -eq 1 ]; then
    echo
    echo "### goal verdict (service p99 ≤ 20 ms, delivery ≥ 99.9%, rate sustained, warnings 0)"
    echo
    echo "| Scenario | Offered | Throughput | service p99 | Delivery | Warnings | Rate held | Verdict |"
    echo "|----------|---------|------------|-------------|----------|----------|-----------|---------|"
    # CSV columns after header:
    # 1 name 2 conns 3 senders 4 rate 5 payload 6 secs 7 measured_s
    # 8 thru 9 p50 10 p99 11 p999 12 smax 13 rp50 14 rp99 15 rmax
    # 16 delivery 17 sent 18 sent_expected 19 received 20 expected
    # 21 warnings 22 errors 23 failed 24 closed 25 slip_mean 26 slip_max
    # 27 idle_rss 28 peak_rss 29 per_conn 30 gw_cpu 31 lg_cpu
    awk -F, '
    function offered(senders, rate, conns) { return senders * rate * (conns - 1) }
    function num(x) { return x + 0 }
    NR == 1 { next }
    {
        name = $1
        conns = num($2); senders = num($3); rate = num($4)
        thru = num($8); p99 = num($10); delivery = num($16)
        sent = num($17); sent_expected = num($18); warnings = num($21)
        gw_cpu = num($30); lg_cpu = num($31)
        off = offered(senders, rate, conns)

        rate_held = 1
        if (sent_expected > 0 && (sent < sent_expected * 0.995 || sent > sent_expected * 1.005)) {
            rate_held = 0
        }

        is_goal = (index(name, "goal-1m-") == 1)
        is_beyond = (index(name, "beyond-") == 1)

        if (p99 <= 20.0 && delivery >= 99.9 && warnings == 0 && rate_held == 1 && thru >= off * 0.999) {
            verdict = "pass"
        } else if (rate_held == 0) {
            verdict = "harness-bound"
        } else if (delivery < 99.9 || warnings > 0) {
            verdict = "cliff"
        } else if (p99 > 20.0) {
            verdict = "latency"
        } else if (lg_cpu > gw_cpu * 1.5 && lg_cpu > 200) {
            verdict = "harness-bound"
        } else {
            verdict = "miss"
        }

        printf "| %s | %d | %s | %s ms | %s%% | %s | %s | %s |\n", \
            name, off, $8, $10, $16, $21, (rate_held ? "yes" : "no"), verdict

        if (is_goal && verdict == "pass") goal_pass++
        if (is_beyond && verdict == "pass") beyond_pass++
        if (is_goal) goal_total++
        if (is_beyond) beyond_total++
    }
    END {
        print ""
        if (goal_pass > 0) {
            print "1M goal: HIT (" goal_pass " of " goal_total " goal-1m-* scenarios passed SLO at offered load)"
        } else {
            print "1M goal: MISS (no goal-1m-* scenario sustained offered load within SLO)"
        }
        print "stretch: " beyond_pass " of " beyond_total " beyond-* scenarios within SLO"
        print ""
        print "pass          = offered load held, delivery ≥ 99.9%, service p99 ≤ 20 ms, warnings 0"
        print "harness-bound = generator did not sustain the requested publish rate (or dwarfed gateway CPU)"
        print "cliff         = delivery below 99.9% or non-zero warnings"
        print "latency       = service p99 above 20 ms with otherwise clean delivery"
        print "miss          = other failure to meet the bar"
        print ""
        print "Exit code stays 0 on MISS — this sweep measures the gap, it does not gate CI."
    }
    ' "$CSV"
fi
