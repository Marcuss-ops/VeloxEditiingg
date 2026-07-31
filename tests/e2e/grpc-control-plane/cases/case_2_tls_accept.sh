# Sourced gRPC control-plane case: case_2_tls_accept

case_2_tls_accept() {
  local id="case-2-tls-accept"
  local worker_id="e2e-worker-tls-case-2"
  local case_dir="$WORKDIR/cases/$id"
  local pki_dir="$WORKDIR/pki/$id"
  local master_env="$case_dir/master.env"
  local worker_cfg="$case_dir/worker-config.json"
  mkdir_p "$case_dir" "$pki_dir"
  mkdir -p "$case_dir/data" "$case_dir/run" "$case_dir/videos"
  touch "$case_dir/data/velox.db"

  "$ROOT/certs/generate-dev-pki.sh" "$pki_dir" "$worker_id" 7 365 >/dev/null

  patch_env "$ROOT/configs/master.env.example" "$master_env" \
    -e "s|^VELOX_RUNTIME_DIR=.*|VELOX_RUNTIME_DIR=$case_dir/run|" \
    -e "s|^VELOX_DATA_DIR=.*|VELOX_DATA_DIR=$case_dir/data|" \
    -e "s|^VELOX_DB_PATH=.*|VELOX_DB_PATH=$case_dir/data/velox.db|" \
    -e "s|^VELOX_VIDEOS_DIR=.*|VELOX_VIDEOS_DIR=$case_dir/videos|" \
    -e "s|^VELOX_ALLOWED_WORKERS=.*|VELOX_ALLOWED_WORKERS=$worker_id|" \
    -e "s|^# VELOX_GRPC_TLS_CERT_FILE=.*|VELOX_GRPC_TLS_CERT_FILE=$pki_dir/server.crt|" \
    -e "s|^# VELOX_GRPC_TLS_KEY_FILE=.*|VELOX_GRPC_TLS_KEY_FILE=$pki_dir/server.key|" \
    -e "s|^# VELOX_GRPC_TLS_CA_FILE=.*|VELOX_GRPC_TLS_CA_FILE=$pki_dir/ca.crt|" \
    -e 's|^VELOX_GRPC_ALLOW_INSECURE_DEV=.*|VELOX_GRPC_ALLOW_INSECURE_DEV=false|'

  cp "$ROOT/configs/worker-tls.json" "$worker_cfg"
  sed -i \
    -e "s|WORKER_ID_PLACEHOLDER|$worker_id|" \
    -e "s|WORK_DIR_PLACEHOLDER|$WORKDIR/work|" \
    -e "s|STATE_DIR_PLACEHOLDER|$WORKDIR/work/state|" \
    -e "s|TEMP_DIR_PLACEHOLDER|$WORKDIR/work/temp|" \
    -e "s|BUNDLE_HASH_PLACEHOLDER|e2e-bundle-hash|" \
    -e "s|CERT_DIR_PLACEHOLDER|$pki_dir|g" \
    "$worker_cfg"

  lib_reset_children
  spawn_master "$id" "$master_env"
  if ! wait_for_master_ready "http://localhost:8000" "e2e-admin-token" 15 "$id"; then
    lib_kill_all TERM
    assert_fail "case-2: master never became ready"
    aggregate_record "$id" "FAIL"
    return
  fi
  spawn_worker_sync "$id" "$worker_id" "$worker_cfg"
  local rv=$?
  sleep 1
  lib_kill_all TERM
  if (( rv == 0 )); then
    aggregate_record "$id" "PASS"
  else
    aggregate_record "$id" "FAIL"
  fi
}
