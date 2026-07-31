# Sourced gRPC control-plane case: case_6_parallel_one_accept_one_reject

case_6_parallel_one_accept_one_reject() {
  local id="case-6-parallel-one-accept-one-reject"
  local good_id="e2e-worker-tls-good-6"
  local bad_id="e2e-worker-tls-bad-6"
  local case_dir="$WORKDIR/cases/$id"
  local pki_good_dir="$WORKDIR/pki/${id}-good"
  local pki_bad_dir="$WORKDIR/pki/${id}-bad"
  local master_env="$case_dir/master.env"
  local worker_good_cfg="$case_dir/worker-good.json"
  local worker_bad_cfg="$case_dir/worker-bad.json"
  mkdir_p "$case_dir" "$pki_good_dir" "$pki_bad_dir"
  mkdir -p "$case_dir/data" "$case_dir/run" "$case_dir/videos"
  touch "$case_dir/data/velox.db"

  "$ROOT/certs/generate-dev-pki.sh" "$pki_good_dir" "$good_id" 7 365 >/dev/null
  "$ROOT/certs/generate-dev-pki.sh" "$pki_bad_dir"  "$bad_id"  7 365 >/dev/null

  patch_env "$ROOT/configs/master.env.example" "$master_env" \
    -e "s|^VELOX_RUNTIME_DIR=.*|VELOX_RUNTIME_DIR=$case_dir/run|" \
    -e "s|^VELOX_DATA_DIR=.*|VELOX_DATA_DIR=$case_dir/data|" \
    -e "s|^VELOX_DB_PATH=.*|VELOX_DB_PATH=$case_dir/data/velox.db|" \
    -e "s|^VELOX_VIDEOS_DIR=.*|VELOX_VIDEOS_DIR=$case_dir/videos|" \
    -e "s|^VELOX_ALLOWED_WORKERS=.*|VELOX_ALLOWED_WORKERS=$good_id,$bad_id|" \
    -e "s|^# VELOX_GRPC_TLS_CERT_FILE=.*|VELOX_GRPC_TLS_CERT_FILE=$pki_good_dir/server.crt|" \
    -e "s|^# VELOX_GRPC_TLS_KEY_FILE=.*|VELOX_GRPC_TLS_KEY_FILE=$pki_good_dir/server.key|" \
    -e "s|^# VELOX_GRPC_TLS_CA_FILE=.*|VELOX_GRPC_TLS_CA_FILE=$pki_good_dir/ca.crt|"

  cp "$ROOT/configs/worker-tls.json" "$worker_good_cfg"
  sed -i \
    -e "s|WORKER_ID_PLACEHOLDER|$good_id|" \
    -e "s|WORK_DIR_PLACEHOLDER|$WORKDIR/work|" \
    -e "s|STATE_DIR_PLACEHOLDER|$WORKDIR/work/state|" \
    -e "s|TEMP_DIR_PLACEHOLDER|$WORKDIR/work/temp|" \
    -e "s|BUNDLE_HASH_PLACEHOLDER|e2e-bundle-hash|" \
    -e "s|CERT_DIR_PLACEHOLDER|$pki_good_dir|g" \
    "$worker_good_cfg"
  cp "$ROOT/configs/worker-tls.json" "$worker_bad_cfg"
  sed -i \
    -e "s|WORKER_ID_PLACEHOLDER|$bad_id|" \
    -e "s|WORK_DIR_PLACEHOLDER|$WORKDIR/work|" \
    -e "s|STATE_DIR_PLACEHOLDER|$WORKDIR/work/state|" \
    -e "s|TEMP_DIR_PLACEHOLDER|$WORKDIR/work/temp|" \
    -e "s|BUNDLE_HASH_PLACEHOLDER|e2e-bundle-hash|" \
    -e "s|CERT_DIR_PLACEHOLDER|$pki_bad_dir|g" \
    "$worker_bad_cfg"

  lib_reset_children
  spawn_master "$id" "$master_env"
  if ! wait_for_master_ready "$MASTER_URL" "e2e-admin-token" 15 "$id"; then
    lib_kill_all TERM
    assert_fail "case-6: master never became ready"
    aggregate_record "$id" "FAIL"
    return
  fi

  local good_log="$WORKDIR/$id/worker-${good_id}.log"
  local bad_log="$WORKDIR/$id/worker-${bad_id}.log"

  set +m
  "$VELOX_WORKER_BIN" --config "$worker_good_cfg" >"$good_log" 2>&1 &
  lib_push_pid $! "worker-$good_id"
  set -m
  if wait_for_worker_connection "$WORKDIR/$id/master.log" "$good_id" 12; then
    sleep 1
  fi
  for pid in "${_LIB_CHILD_PIDS[@]}"; do
    wait "$pid" 2>/dev/null || true
  done
  # Remove only the worker PID — master MUST stay in _LIB_CHILD_PIDS so
  # the trap handler (on_exit → lib_kill_all TERM) can reap it.
  local new_pids=() new_labels=()
  for i in "${!_LIB_CHILD_PIDS[@]}"; do
    if [[ "${_LIB_CHILD_LABELS[$i]}" != "worker-$good_id" ]]; then
      new_pids+=("${_LIB_CHILD_PIDS[$i]}")
      new_labels+=("${_LIB_CHILD_LABELS[$i]}")
    fi
  done
  _LIB_CHILD_PIDS=("${new_pids[@]}")
  _LIB_CHILD_LABELS=("${new_labels[@]}")

  set +m
  "$VELOX_WORKER_BIN" --config "$worker_bad_cfg" >"$bad_log" 2>&1 &
  lib_push_pid $! "worker-$bad_id"
  set -m
  sleep 6
  for pid in "${_LIB_CHILD_PIDS[@]}"; do
    wait "$pid" 2>/dev/null || true
  done
  lib_kill_all TERM

  local good_ok=0 bad_ok=0
  grep -qE "(HelloAck|✓ HelloAck)" "$good_log" && good_ok=1
  grep -qiE "(handshake|verify|certificate|unknown authority|PermissionDenied|Unauthenticated)" "$bad_log" && bad_ok=1

  if (( good_ok == 1 && bad_ok == 1 )); then
    aggregate_record "$id" "PASS"
  else
    assert_fail "case-6: good_ok=$good_ok bad_ok=$bad_ok (good_log=$good_log, bad_log=$bad_log)"
    aggregate_record "$id" "FAIL"
  fi
}
