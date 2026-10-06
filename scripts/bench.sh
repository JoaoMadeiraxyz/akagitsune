#!/usr/bin/env bash
#
# bench.sh — measure the gateway and record the conditions with the numbers
#
# Calibrates the harness first (scripts/calibrate.sh) and refuses to produce
# baselines if that fails, then runs each scenario against a freshly started
# gateway, samples the server's RSS and CPU alongside, and writes a CSV plus a
# markdown table shaped for the Measured baselines section of docs/architecture.md.
#
# USAGE: scripts/bench.sh [--quick] [--goal] [--all]
#                         [--skip-calibration] [--port <N>]
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

# name connections topics senders rate payload_bytes seconds [loadgen flags...]
# offered deliveries/s = senders x rate x (connections / topics - 1), extra topic not included
BASELINE_SCENARIOS=(
    "fanout-200 200 1 5 50 0 15"
    "fanout-500 500 1 5 50 0 15"
    "fanout-1000 1000 1 5 50 0 15"
    "ingest-50 50 1 50 200 0 15"
    "payload-4k 200 1 5 50 4096 15"
    "cliff-300 300 1 30 400 0 15"
    "topics-1k 1000 100 100 100 0 15"
    "topics-5k 5000 500 500 100 0 15"
    "binary-200 200 1 5 50 0 15 --binary"
    "overlap-200 200 1 5 50 0 15 --extra-topic-rate 5"
    "overlap-cliff 300 1 30 400 0 15 --extra-topic-rate 5"
    "churn-500 500 1 5 50 0 15 --churn 200 --churn-rate 50"
)
GOAL_SCENARIOS=(
    "goal-1m-fanout 501 1 10 200 0 15"
    "goal-1m-ingest 101 1 50 200 0 15"
    "goal-1m-mesh 201 1 50 100 0 15"
    "goal-1m-topics 2100 100 100 500 0 15"
    "beyond-1.5m-fanout 501 1 15 200 0 15"
    "beyond-1.5m-ingest 101 1 50 300 0 15"
    "beyond-1.5m-mesh 251 1 50 120 0 15"
    "beyond-2m-fanout 1001 1 10 200 0 15"
    "beyond-2m-mesh 201 1 50 200 0 15"
    "beyond-3m-explore 301 1 50 200 0 15"
)
if [ "$QUICK" -eq 1 ]; then
    SCENARIOS=("fanout-200 200 1 5 50 0 10")
elif [ "$GOAL" -eq 1 ]; then
    SCENARIOS=("${GOAL_SCENARIOS[@]}")
elif [ "$ALL" -eq 1 ]; then
    SCENARIOS=("${BASELINE_SCENARIOS[@]}" "${GOAL_SCENARIOS[@]}")
else
    SCENARIOS=("${BASELINE_SCENARIOS[@]}")
fi

flag_value() {
    local name=$1 default=$2
    shift 2
    while [ $# -gt 0 ]; do
        if [ "$1" = "$name" ]; then
            echo "$2"
            return
        fi
        shift
    done
    echo "$default"
}

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
STATS_FIELDS=(measured_seconds throughput_msg_s service_p50_ms service_p99_ms service_p999_ms
    service_max_ms response_p50_ms response_p99_ms response_max_ms delivery_pct sent sent_expected
    received expected warnings errors failed_conns closed_early slip_mean_ms slip_max_ms
    extra_expected extra_received extra_delivery_pct subscribe_failed subscribe_ack_p99_ms
    churn_failed misrouted dropped unaccounted unaccounted_scope drained drain_seconds)
DIRTY=$(git status --porcelain 2>/dev/null | head -1)
[ -n "$DIRTY" ] && COMMIT="$COMMIT+dirty"

cleanup() {
    [ -n "$GATEWAY_PID" ] && kill "$GATEWAY_PID" 2>/dev/null || true
    rm -rf "$WORK"
}
trap cleanup EXIT

HEADER="scenario,connections,topics,senders,rate,payload_bytes,seconds,binary,churn,churn_rate,extra_topic_rate"
for field in "${STATS_FIELDS[@]}"; do
    HEADER="$HEADER,$field"
done
echo "$HEADER,idle_rss_mib,peak_rss_mib,rss_per_conn_kib,gateway_cpu_pct,loadgen_cpu_pct" >"$CSV"

for scenario in "${SCENARIOS[@]}"; do
    read -r NAME CONNS TOPICS SENDERS RATE PAYLOAD SECS FLAGS <<<"$scenario"
    read -r -a EXTRA_FLAGS <<<"${FLAGS:-}"
    BINARY=false
    case " ${FLAGS:-} " in *" --binary "*) BINARY=true ;; esac
    CHURN=$(flag_value --churn 0 ${EXTRA_FLAGS[@]+"${EXTRA_FLAGS[@]}"})
    CHURN_RATE=$(flag_value --churn-rate 1 ${EXTRA_FLAGS[@]+"${EXTRA_FLAGS[@]}"})
    EXTRA_RATE=$(flag_value --extra-topic-rate 0 ${EXTRA_FLAGS[@]+"${EXTRA_FLAGS[@]}"})
    EXTRA_SOCKETS=0
    [ "$EXTRA_RATE" -gt 0 ] && EXTRA_SOCKETS=1
    OPEN_CONNS=$((CONNS + CHURN + EXTRA_SOCKETS))
    require_fds "$OPEN_CONNS"

    echo "### $NAME: $CONNS connections, $TOPICS topic(s), $SENDERS senders, $RATE/s, ${PAYLOAD}B padding, ${SECS}s ${FLAGS:-}"

    GATEWAY_ADDR="127.0.0.1:$PORT" RUST_LOG=warn "$GATEWAY" >"$WORK/gateway.log" 2>&1 &
    GATEWAY_PID=$!
    wait_for_port "$PORT" 15
    sleep 1
    IDLE_RSS=$(ps -o rss= -p "$GATEWAY_PID" | awk '{printf "%.1f", $1/1024}')

    : >"$WORK/gateway.samples"
    : >"$WORK/loadgen.samples"

    "$LOADGEN" --url "ws://127.0.0.1:$PORT/ws" --connections "$CONNS" \
        --topics "$TOPICS" --senders "$SENDERS" --rate "$RATE" --payload-bytes "$PAYLOAD" \
        --seconds "$SECS" ${EXTRA_FLAGS[@]+"${EXTRA_FLAGS[@]}"} --json \
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
    PER_CONN=$(awk -v idle="$IDLE_RSS" -v peak="$PEAK_RSS" -v n="$OPEN_CONNS" \
        'BEGIN{printf "%.1f", (peak - idle) * 1024 / n}')

    ROW="$NAME,$CONNS,$TOPICS,$SENDERS,$RATE,$PAYLOAD,$SECS,$BINARY,$CHURN,$CHURN_RATE,$EXTRA_RATE"
    for field in "${STATS_FIELDS[@]}"; do
        ROW="$ROW,$(json_field "$RESULT" "$field")"
    done
    echo "$ROW,$IDLE_RSS,$PEAK_RSS,$PER_CONN,$GATEWAY_CPU,$LOADGEN_CPU" >>"$CSV"

    printf '    %s msg/s   service p50 %sms p99 %sms   delivery %s%%   warnings %s\n' \
        "$(json_field "$RESULT" throughput_msg_s)" "$(json_field "$RESULT" service_p50_ms)" \
        "$(json_field "$RESULT" service_p99_ms)" "$(json_field "$RESULT" delivery_pct)" \
        "$(json_field "$RESULT" warnings)"
    printf '    misrouted %s   unaccounted %s   drained %s   extra delivery %s\n' \
        "$(json_field "$RESULT" misrouted)" "$(json_field "$RESULT" unaccounted)" \
        "$(json_field "$RESULT" drained)" "$(json_field "$RESULT" extra_delivery_pct)"
    printf '    gateway rss %s -> %s MiB (%s KiB/conn)   cpu %s%% gateway, %s%% loadgen\n\n' \
        "$IDLE_RSS" "$PEAK_RSS" "$PER_CONN" "$GATEWAY_CPU" "$LOADGEN_CPU"

    sleep 2
done

echo "results written to $CSV"
echo
COLUMNS_AWK='NR == 1 { for (i = 1; i <= NF; i++) col[$i] = i; next }
function v(name) { return $(col[name]) }
function invocation(   flags) {
    flags = ""
    if (v("binary") == "true") flags = flags " --binary"
    if (v("extra_topic_rate") + 0 > 0) flags = flags " --extra-topic-rate " v("extra_topic_rate")
    if (v("churn") + 0 > 0) flags = flags " --churn " v("churn") " --churn-rate " v("churn_rate")
    return sprintf("--connections %s --topics %s --senders %s --rate %s --payload-bytes %s --seconds %s%s", \
        v("connections"), v("topics"), v("senders"), v("rate"), v("payload_bytes"), v("seconds"), flags)
}'

echo "| Date | Commit | Scenario | Invocation | Throughput | service p50 | service p99 | Delivery | Warnings | Misrouted | Unaccounted |"
echo "|------|--------|----------|------------|------------|-------------|-------------|----------|----------|-----------|-------------|"
awk -F, -v today="$(date +%Y-%m-%d)" -v commit="$COMMIT" "$COLUMNS_AWK"'
{
    printf "| %s | `%s` | %s | `%s` | %s msg/s | %s ms | %s ms | %s%% | %s | %s | %s |\n",
        today, commit, v("scenario"), invocation(), v("throughput_msg_s"), v("service_p50_ms"),
        v("service_p99_ms"), v("delivery_pct"), v("warnings"), v("misrouted"), v("unaccounted")
}' "$CSV"
echo
echo "A row with non-zero warnings is not a throughput figure; it is a record of the cliff."

if [ "$GOAL" -eq 1 ] || [ "$ALL" -eq 1 ]; then
    echo
    echo "### goal verdict (service p99 ≤ 20 ms, delivery ≥ 99.9%, rate sustained, warnings 0, routing correct)"
    echo
    echo "| Scenario | Offered | Throughput | service p99 | Delivery | Warnings | Rate held | Verdict |"
    echo "|----------|---------|------------|-------------|----------|----------|-----------|---------|"
    awk -F, "$COLUMNS_AWK"'
    function offered(senders, rate, conns, topics) { return senders * rate * (conns / topics - 1) }
    function num(x) { return x + 0 }
    {
        name = v("scenario")
        thru = num(v("throughput_msg_s")); p99 = num(v("service_p99_ms")); delivery = num(v("delivery_pct"))
        sent = num(v("sent")); sent_expected = num(v("sent_expected")); warnings = num(v("warnings"))
        gw_cpu = num(v("gateway_cpu_pct")); lg_cpu = num(v("loadgen_cpu_pct"))
        drained = (v("drained") == "true")
        off = offered(num(v("senders")), num(v("rate")), num(v("connections")), num(v("topics")))

        rate_held = 1
        if (sent_expected > 0 && (sent < sent_expected * 0.995 || sent > sent_expected * 1.005)) {
            rate_held = 0
        }

        is_goal = (index(name, "goal-1m-") == 1)
        is_beyond = (index(name, "beyond-") == 1)

        if (num(v("misrouted")) > 0 || num(v("subscribe_failed")) > 0 || \
            (drained && (num(v("unaccounted")) != 0 || num(v("churn_failed")) > 0))) {
            verdict = "incorrect"
        } else if (p99 <= 20.0 && delivery >= 99.9 && warnings == 0 && rate_held == 1 && thru >= off * 0.999) {
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
        if (!drained) verdict = verdict " (undrained)"

        printf "| %s | %d | %s | %s ms | %s%% | %s | %s | %s |\n", \
            name, off, v("throughput_msg_s"), v("service_p99_ms"), v("delivery_pct"), v("warnings"), \
            (rate_held ? "yes" : "no"), verdict

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
        print "stretch: " beyond_pass + 0 " of " beyond_total + 0 " beyond-* scenarios within SLO"
        print ""
        print "incorrect     = misrouted, unacknowledged subscriptions, or (after a full drain) frames"
        print "                lost without a warning, more reported dropped than lost, or"
        print "                unacknowledged churn; overrides everything"
        print "pass          = offered load held, delivery ≥ 99.9%, service p99 ≤ 20 ms, warnings 0"
        print "harness-bound = generator did not sustain the requested publish rate (or dwarfed gateway CPU)"
        print "cliff         = delivery below 99.9% or non-zero warnings"
        print "latency       = service p99 above 20 ms with otherwise clean delivery"
        print "miss          = other failure to meet the bar"
        print "(undrained)   = delivery had not settled at the drain limit; unaccounted is not judged"
        print ""
        print "Exit code stays 0 on MISS — this sweep measures the gap, it does not gate CI."
    }
    ' "$CSV"
fi
