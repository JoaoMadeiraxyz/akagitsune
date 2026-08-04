# shellcheck shell=bash

json_field() {
    awk -v key="\"$2\":" '
        {
            at = index($0, key)
            if (at == 0) { print ""; exit }
            rest = substr($0, at + length(key))
            sub(/[,}].*$/, "", rest)
            print rest
        }
    ' <<<"$1"
}

wait_for_port() {
    local port=$1 timeout=${2:-15} waited=0
    while ! nc -z 127.0.0.1 "$port" >/dev/null 2>&1; do
        sleep 0.2
        waited=$((waited + 1))
        if [ "$waited" -gt $((timeout * 5)) ]; then
            echo "port $port did not open within ${timeout}s" >&2
            return 1
        fi
    done
}

require_fds() {
    local needed=$(($1 * 2 + 64)) limit
    limit=$(ulimit -n)
    if [ "$limit" != "unlimited" ] && [ "$limit" -lt "$needed" ]; then
        echo "ulimit -n is $limit but $needed descriptors are needed; connections will fail as if the server refused them" >&2
        echo "raise it with: ulimit -n $needed" >&2
        return 1
    fi
}

sample_process() {
    local pid=$1 out=$2
    while kill -0 "$pid" 2>/dev/null; do
        ps -o rss=,%cpu= -p "$pid" 2>/dev/null >>"$out" || true
        sleep 0.5
    done
}

peak_rss_mib() {
    awk 'BEGIN{m=0} {if ($1+0 > m) m=$1+0} END{printf "%.1f", m/1024}' "$1"
}

peak_cpu_pct() {
    awk 'BEGIN{m=0} {if ($2+0 > m) m=$2+0} END{printf "%.0f", m}' "$1"
}
