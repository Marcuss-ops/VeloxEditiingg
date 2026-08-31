#!/usr/bin/env bash
# Run the real Matt Damon fleet benchmark at controlled concurrency levels.
# Results are printed to stdout; no payloads or reports are persisted.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PAYLOAD_FILE="${ROOT_DIR}/ops/jobs/matt-damon-30-funny-clips.generate.json"
MASTER_URL="${VELOX_MASTER_URL:-http://127.0.0.1:8000}"
LEVELS="1,2,4,8"
POLL_TIMEOUT_S="${VELOX_BENCHMARK_POLL_TIMEOUT_S:-300}"

now_ms() {
	printf '%s\n' "$(( $(date +%s%N) / 1000000 ))"
}

if [[ "${1:-}" == "--levels" ]]; then
  LEVELS="${2:?missing value for --levels}"
  shift 2
fi
if [[ "${1:-}" == "--recipe" ]]; then
  recipe="${2:?missing value for --recipe}"
  [[ "$recipe" == "matt-damon-30-clips" ]] || { echo "unsupported recipe: $recipe" >&2; exit 4; }
  shift 2
fi
[[ $# -eq 0 ]] || { echo "usage: $0 [--levels 1,2,4,8] [--recipe matt-damon-30-clips]" >&2; exit 4; }
[[ -r "$PAYLOAD_FILE" ]] || { echo "payload not found: $PAYLOAD_FILE" >&2; exit 4; }

if [[ -z "${VELOX_ADMIN_TOKEN:-}" ]]; then
  # Canonical operator helper loads the token without printing it.
  source "${ROOT_DIR}/scripts/operator/with-production-env.sh" >/dev/null 2>&1
fi

admin_header=( -H "Authorization: Bearer ${VELOX_ADMIN_TOKEN}" )
client_id="concurrency-benchmark-$(date +%s)-$$"
key_json="$(curl -fsS -X POST "${admin_header[@]}" -H 'Content-Type: application/json' \
  --data "{\"client_id\":\"${client_id}\",\"description\":\"controlled concurrency benchmark (ephemeral)\",\"scopes\":[\"jobs.submit\"],\"rate_limit_rps\":20,\"rate_limit_burst\":32,\"quota_max_scenes\":1000,\"quota_max_total_secs\":14400}" \
  "${MASTER_URL}/api/v1/admin/m2m/keys")"
m2m_token="$(jq -er '.plaintext_secret' <<<"$key_json")"

cleanup() {
  curl -fsS -X DELETE "${admin_header[@]}" "${MASTER_URL}/api/v1/admin/m2m/keys/${client_id}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

printf 'level\tjobs\tterminal\tpending_p50_ms\tpending_max_ms\twall_ms\tthroughput_video_h\n'
IFS=',' read -r -a levels <<< "$LEVELS"
for level in "${levels[@]}"; do
  [[ "$level" =~ ^[1-9][0-9]*$ ]] || { echo "invalid level: $level" >&2; exit 4; }
  ids=()
  submitted=()
  level_start="$(now_ms)"
  for ((i=1; i<=level; i++)); do
    nonce="matt-damon-concurrency-${level}-$(date +%s%N)-${i}"
    body="$(jq --arg key "$nonce" --arg name "Matt Damon concurrency L${level} #${i}" \
      '.idempotency_key=$key | .video_name=$name | .delivery_plan[0].destination_id="local-fallback"' "$PAYLOAD_FILE")"
    response="$(curl -fsS -X POST -H "Authorization: Bearer ${m2m_token}" -H 'Content-Type: application/json' \
      --data-binary "$body" "${MASTER_URL}/api/v1/jobs")"
    ids+=("$(jq -er '.job_id' <<< "$response")")
    submitted+=("$(now_ms)")
  done

  statuses=()
  for id in "${ids[@]}"; do statuses+=(PENDING); done
  deadline=$(( $(date +%s) + POLL_TIMEOUT_S ))
  while :; do
    done_count=0
    for i in "${!ids[@]}"; do
      detail="$(curl -fsS "${admin_header[@]}" "${MASTER_URL}/api/v1/admin/jobs/${ids[$i]}")"
      status="$(jq -r '.job.status // "UNKNOWN"' <<< "$detail")"
      statuses[$i]="$status"
      case "$status" in SUCCEEDED|FAILED|CANCELLED) ((done_count+=1)) ;; esac
    done
    (( done_count == level )) && break
    (( $(date +%s) >= deadline )) && break
    sleep 2
  done

  pending_values=()
  for i in "${!ids[@]}"; do
    detail="$(curl -fsS "${admin_header[@]}" "${MASTER_URL}/api/v1/admin/jobs/${ids[$i]}")"
    started="$(jq -r '.job.started_at // ""' <<< "$detail")"
    if [[ -n "$started" && "$started" != 0001-* ]]; then
      start_epoch="$(( $(date -d "$started" +%s 2>/dev/null || printf '%s' "$(( submitted[i] / 1000 ))") * 1000 ))"
      pending_values+=("$((start_epoch - submitted[i]))")
    else
      pending_values+=("$(( $(now_ms) - submitted[i] ))")
    fi
  done
  IFS=$'\n' read -r -d '' -a sorted_pending < <(printf '%s\n' "${pending_values[@]}" | sort -n && printf '\0') || true
  p50="${sorted_pending[$(( ${#sorted_pending[@]} / 2 ))]:-0}"
  pmax="${sorted_pending[$(( ${#sorted_pending[@]} - 1 ))]:-0}"
  wall="$(( $(now_ms) - level_start ))"
  terminal="$(printf '%s,' "${statuses[@]}" | sed 's/,$//')"
  throughput="$(awk -v n="$level" -v ms="$wall" 'BEGIN { if (ms > 0) printf "%.2f", n*3600000/ms; else print "0.00" }')"
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$level" "$level" "$terminal" "$p50" "$pmax" "$wall" "$throughput"

  for i in "${!ids[@]}"; do
    case "${statuses[$i]}" in SUCCEEDED|FAILED|CANCELLED) ;; *)
      curl -fsS -X POST "${admin_header[@]}" "${MASTER_URL}/api/v1/admin/jobs/${ids[$i]}/cancel" >/dev/null 2>&1 || true ;;
    esac
  done
done
