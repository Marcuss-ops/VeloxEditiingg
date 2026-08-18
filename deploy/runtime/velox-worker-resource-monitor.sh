#!/usr/bin/env bash
set -euo pipefail

readonly STATE_DIR="/var/lib/velox-worker/maintenance"
readonly JSON_FILE="$STATE_DIR/resource-status.json"
readonly PROM_FILE="$STATE_DIR/resource-status.prom"
readonly WARN=75
readonly HIGH=85
readonly CRITICAL=90

mkdir -p "$STATE_DIR"

read_cpu() {
  read -r _ user nice system idle iowait irq softirq steal _ _ < /proc/stat
  cpu_total=$((user + nice + system + idle + iowait + irq + softirq + steal))
  cpu_idle=$((idle + iowait))
  cpu_iowait=$iowait
}

read_cpu
total_before=$cpu_total
idle_before=$cpu_idle
iowait_before=$cpu_iowait
sleep 1
read_cpu
total_delta=$((cpu_total - total_before))
idle_delta=$((cpu_idle - idle_before))
iowait_delta=$((cpu_iowait - iowait_before))
if (( total_delta > 0 )); then
  cpu_used=$(( (100 * (total_delta - idle_delta)) / total_delta ))
  iowait_pct=$(( (100 * iowait_delta) / total_delta ))
else
  cpu_used=0
  iowait_pct=0
fi

read -r mem_total mem_available < <(awk '/^(MemTotal|MemAvailable):/ { print $2 }' /proc/meminfo | paste -sd ' ' -)
if (( mem_total > 0 )); then
  memory_used=$((100 - (100 * mem_available / mem_total)))
else
  memory_used=0
fi

disk_used="$(df -P / | awk 'NR == 2 { gsub(/%/, "", $5); print $5 }')"
load1="$(awk '{ print $1 }' /proc/loadavg)"
worker_id="$(sed -n 's/^VELOX_WORKER_ID=//p' /etc/velox-worker/worker.env 2>/dev/null | tail -n 1)"
worker_id="${worker_id:-unknown}"
timestamp="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

level_for() {
  local value="$1"
  if (( value >= CRITICAL )); then printf 'CRITICAL'
  elif (( value >= HIGH )); then printf 'HIGH'
  elif (( value >= WARN )); then printf 'WARN'
  else printf 'OK'
  fi
}

disk_level="$(level_for "$disk_used")"
memory_level="$(level_for "$memory_used")"
cpu_level="$(level_for "$cpu_used")"
if (( iowait_pct >= 30 )); then iowait_level=CRITICAL
elif (( iowait_pct >= 20 )); then iowait_level=HIGH
elif (( iowait_pct >= 10 )); then iowait_level=WARN
else iowait_level=OK
fi

overall=OK
for level in "$disk_level" "$memory_level" "$cpu_level" "$iowait_level"; do
  case "$level" in
    CRITICAL) overall=CRITICAL ;;
    HIGH) [[ "$overall" == OK || "$overall" == WARN ]] && overall=HIGH ;;
    WARN) [[ "$overall" == OK ]] && overall=WARN ;;
  esac
done

tmp_json="$(mktemp "$STATE_DIR/resource-status.json.tmp.XXXXXX")"
tmp_prom="$(mktemp "$STATE_DIR/resource-status.prom.tmp.XXXXXX")"
trap 'rm -f "$tmp_json" "$tmp_prom"' EXIT
printf '{"timestamp":"%s","worker_id":"%s","overall":"%s","disk_used_percent":%d,"memory_used_percent":%d,"cpu_used_percent":%d,"cpu_iowait_percent":%d,"load1":"%s","levels":{"disk":"%s","memory":"%s","cpu":"%s","iowait":"%s"}}\n' \
  "$timestamp" "$worker_id" "$overall" "$disk_used" "$memory_used" "$cpu_used" "$iowait_pct" "$load1" \
  "$disk_level" "$memory_level" "$cpu_level" "$iowait_level" >"$tmp_json"
printf '# HELP velox_worker_host_resource_percent Current host resource pressure.\n# TYPE velox_worker_host_resource_percent gauge\nvelox_worker_host_resource_percent{worker_id="%s",resource="disk"} %d\nvelox_worker_host_resource_percent{worker_id="%s",resource="memory"} %d\nvelox_worker_host_resource_percent{worker_id="%s",resource="cpu"} %d\nvelox_worker_host_resource_percent{worker_id="%s",resource="iowait"} %d\n' \
  "$worker_id" "$disk_used" "$worker_id" "$memory_used" "$worker_id" "$cpu_used" "$worker_id" "$iowait_pct" >"$tmp_prom"
chown root:root "$tmp_json" "$tmp_prom"
chmod 0644 "$tmp_json" "$tmp_prom"
mv -f "$tmp_json" "$JSON_FILE"
mv -f "$tmp_prom" "$PROM_FILE"

message="worker=$worker_id overall=$overall disk=${disk_used}% memory=${memory_used}% cpu=${cpu_used}% iowait=${iowait_pct}% load1=$load1"
if [[ "$overall" == OK ]]; then
  logger -t velox-worker-resource -- "$message" 2>/dev/null || true
else
  logger -p daemon.warning -t velox-worker-resource -- "$message thresholds=disk:${WARN}/${HIGH}/${CRITICAL},memory:${WARN}/${HIGH}/${CRITICAL},cpu:${WARN}/${HIGH}/${CRITICAL},iowait:10/20/30" 2>/dev/null || true
fi
printf '%s\n' "$message"
