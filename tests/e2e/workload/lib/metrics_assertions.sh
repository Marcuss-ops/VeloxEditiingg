# shellcheck shell=bash
# shellcheck disable=SC2154

assert_master_metrics() {
  info "Verification 5: Prometheus metrics (strict: velox_taskrunner_serial_work_ms > 0)"
  local metrics="" tr_val=""
  for _ in $(seq 1 30); do
    local tmp_metrics
    tmp_metrics="$(curl -sS -m 5 "http://127.0.0.1:${MASTER_PORT}/metrics" 2>/dev/null || true)"
    if [[ -n "$tmp_metrics" ]]; then
      metrics="$tmp_metrics"
      tr_val="$(echo "$metrics" | grep -E '^velox_taskrunner_serial_work_ms\b' | awk '{print $NF}' || true)"
    fi
    [[ -n "$tr_val" ]] && break
    sleep 1
  done
  if [[ -z "$metrics" ]]; then
    fail "/metrics returned empty — Prometheus endpoint disabled or master unhealthy"
    exit 1
  fi
  if [[ -z "$tr_val" ]]; then
    info "available taskrunner metric lines:"
    echo "$metrics" | grep -E '^velox_taskrunner_' || true
    fail "metrics: velox_taskrunner_serial_work_ms missing after 30s"
    exit 1
  fi
  if ! awk -v v="$tr_val" 'BEGIN{ exit !(v+0 > 0) }'; then
    fail "metrics: velox_taskrunner_serial_work_ms value $tr_val is not > 0"
    exit 1
  fi
  pass "metrics: velox_taskrunner_serial_work_ms = $tr_val ms"
}

sql_query() {
  sqlite3 -separator '|' "$DATA_DIR/velox.db" "$1" 2>/dev/null
}

assert_database_state() {
  info "Verification 6: Database state assertions (4-part, blocking)"
  local db="$DATA_DIR/velox.db"
  if ! command -v sqlite3 >/dev/null 2>&1; then
    fail "sqlite3 missing — cannot verify DB state"
    exit 1
  fi
  if [[ ! -f "$db" ]]; then
    fail "DB file not found at $db"
    exit 1
  fi

  local attempts_succ
  attempts_succ="$(sql_query "SELECT COUNT(*) FROM task_attempts WHERE job_id = '${JOB_ID}' AND status='SUCCEEDED'" || true)"
  if [[ "${attempts_succ:-0}" =~ ^[1-9][0-9]*$ ]]; then
    pass "DB (a): task_attempts SUCCEEDED count=$attempts_succ for job_id=$JOB_ID"
  else
    fail "DB (a): no SUCCEEDED row in task_attempts for job_id=$JOB_ID (got '$attempts_succ')"
    exit 1
  fi

  local arts_ready
  arts_ready="$(sql_query "SELECT COUNT(*) FROM artifacts WHERE job_id = '${JOB_ID}' AND status='READY'" || true)"
  if [[ "${arts_ready:-0}" =~ ^[1-9][0-9]*$ ]]; then
    pass "DB (b): artifacts READY count=$arts_ready for job_id=$JOB_ID"
  else
    fail "DB (b): no READY row in artifacts for job_id=$JOB_ID (got '$arts_ready')"
    exit 1
  fi

  local db_sha
  db_sha="$(sql_query "SELECT sha256 FROM artifacts WHERE job_id = '${JOB_ID}' AND status='READY' AND type='final_video' ORDER BY verified_at DESC LIMIT 1" || true)"
  if [[ -z "$db_sha" ]]; then
    fail "DB (c): artifacts.sha256 missing/empty for job_id=$JOB_ID"
    exit 1
  fi
  # sha is produced by assert_artifact_sha256 in the caller.
  # shellcheck disable=SC2154
  if [[ "$db_sha" != "$sha" ]]; then
    fail "DB (c): sha256 mismatch (artifacts.sha256=$db_sha, downloaded=$sha, expected_baseline=${E2E_EXPECTED_SHA256:-<unset>})"
    exit 1
  fi
  pass "DB (c): artifacts.sha256 matches downloaded file ($db_sha)"

  local jobs_completed_at db_verified_at jobs_epoch art_epoch
  jobs_completed_at="$(sql_query "SELECT completed_at FROM jobs WHERE job_id = '${JOB_ID}' LIMIT 1" || true)"
  db_verified_at="$(sql_query "SELECT verified_at FROM artifacts WHERE job_id = '${JOB_ID}' AND status='READY' AND type='final_video' ORDER BY verified_at DESC LIMIT 1" || true)"
  if [[ -z "$jobs_completed_at" || -z "$db_verified_at" ]]; then
    fail "DB (d): missing timestamp (jobs.completed_at='$jobs_completed_at', artifacts.verified_at='$db_verified_at')"
    exit 1
  fi
  jobs_epoch="$(date -d "$jobs_completed_at" +%s 2>/dev/null || true)"
  art_epoch="$(date -d "$db_verified_at" +%s 2>/dev/null || true)"
  if [[ -z "$jobs_epoch" || ! "$jobs_epoch" =~ ^[0-9]+$ ]]; then
    fail "DB (d): jobs.completed_at='$jobs_completed_at' is not a valid RFC3339 timestamp"
    exit 1
  fi
  if [[ -z "$art_epoch" || ! "$art_epoch" =~ ^[0-9]+$ ]]; then
    fail "DB (d): artifacts.verified_at='$db_verified_at' is not a valid RFC3339 timestamp"
    exit 1
  fi
  if (( jobs_epoch >= art_epoch )); then
    pass "DB (d): jobs.completed_at (epoch=$jobs_epoch, ${jobs_completed_at}) >= artifacts.verified_at (epoch=$art_epoch, ${db_verified_at}) — finalization ordering holds"
  else
    fail "DB (d): jobs.completed_at (epoch=$jobs_epoch, ${jobs_completed_at}) is BEFORE artifacts.verified_at (epoch=$art_epoch, ${db_verified_at}) — ordering bug"
    exit 1
  fi
}

assert_worker_metrics() {
  info "Verification 7: worker Prometheus /metrics (velox_cache_* families)"
  if ! grep -q "Prometheus metrics server starting on :${WORKER_PROMETHEUS_PORT}" "$WORKER_LOG"; then
    fail "worker log missing Prometheus startup line for :${WORKER_PROMETHEUS_PORT}"
    grep -E "TELEMETRY|Prometheus" "$WORKER_LOG" | tail -5 || true
    exit 1
  fi
  local wmetrics=""
  for _ in $(seq 1 10); do
    wmetrics="$(curl -sS -m 5 "http://127.0.0.1:${WORKER_PROMETHEUS_PORT}/metrics" 2>/dev/null || true)"
    [[ -n "$wmetrics" ]] && break
    sleep 1
  done
  if [[ -z "$wmetrics" ]]; then
    fail "worker /metrics on :${WORKER_PROMETHEUS_PORT} returned empty — Prometheus endpoint not serving"
    exit 1
  fi
  for name in \
    velox_cache_requests_total velox_cache_downloads_total \
    velox_cache_download_bytes_total velox_cache_download_duration_seconds \
    velox_cache_sha_verify_duration_seconds velox_cache_cleanup_duration_seconds \
    velox_cache_evictions_total velox_cache_cleanup_skipped_total \
    velox_cache_size_bytes velox_cache_entries; do
    if ! echo "$wmetrics" | grep -qE "^# HELP ${name} "; then
      fail "worker /metrics missing family ${name}"
      exit 1
    fi
  done
  local bad_request_labels
  bad_request_labels="$(echo "$wmetrics" | grep -E '^velox_cache_requests_total\{' | grep -vE 'result="(hit|miss|other)"' || true)"
  if [[ -n "$bad_request_labels" ]]; then
    fail "high-cardinality result label detected on velox_cache_requests_total:"
    echo "$bad_request_labels"
    exit 1
  fi
  pass "worker /metrics live on :${WORKER_PROMETHEUS_PORT} with all velox_cache_* families + low-cardinality labels"
}
