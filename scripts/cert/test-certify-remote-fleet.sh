#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
FLEET_RUNNER="${ROOT_DIR}/scripts/cert/certify-remote-fleet.sh"
TMP_DIR="$(mktemp -d)"
trap 'rm -rf -- "$TMP_DIR"' EXIT

MOCK_SINGLE="${TMP_DIR}/mock-single.sh"
MOCK_DESTRUCTIVE="${TMP_DIR}/mock-destructive.sh"
cat >"$MOCK_SINGLE" <<'MOCK'
#!/usr/bin/env bash
set -eu
case "${1:-}" in
  --worker-json|--lifecycle-json|--update-json|--job-json)
    if [[ "${MOCK_FAIL_WORKER:-}" == "${WORKER_ID:-}" ]]; then
      printf '{"schema":"mock","worker_id":%s,"overall":"FAIL","checks":[]}' "$(jq -Rsa . <<<"${WORKER_ID:-}")"
    else
      printf '{"schema":"mock","worker_id":%s,"overall":"PASS","checks":[]}' "$(jq -Rsa . <<<"${WORKER_ID:-}")"
    fi
    ;;
  *) exit 22 ;;
esac
MOCK
cat >"$MOCK_DESTRUCTIVE" <<'MOCK'
#!/usr/bin/env bash
set -eu
report=''
worker=''
while (($#)); do
  case "$1" in
    --target-worker-id) worker="$2"; shift 2 ;;
    --report-json) report="$2"; shift 2 ;;
    *) shift ;;
  esac
done
jq -n --arg worker "$worker" '{schema:"mock-destructive",worker_id:$worker,status:"SUCCEEDED",overall:"PASS"}' >"$report"
MOCK
chmod +x "$MOCK_SINGLE" "$MOCK_DESTRUCTIVE"

export RW_FLEET_SINGLE_RUNNER="$MOCK_SINGLE"
export RW_FLEET_DESTRUCTIVE_RUNNER="$MOCK_DESTRUCTIVE"
export MASTER_URL=https://staging.example.test

run_fleet() {
  local mode="$1" workers="$2" output_dir="$3"
  RW_FLEET_ARTIFACT_DIR="$output_dir" \
    bash "$FLEET_RUNNER" --mode "$mode" --workers "$workers" --serial >"${output_dir}.stdout"
  jq -e --arg mode "$mode" '.overall == "PASS" and .mode == $mode and .serial == true and (.workers|length) == 2' \
    "${output_dir}/fleet-report.json" >/dev/null
  jq -e 'all(.workers[]; .status == "PASS")' "${output_dir}/fleet-report.json" >/dev/null
  test -s "${output_dir}/fleet-report.junit.xml"
  test -s "${output_dir}/report.json"
  test -s "${output_dir}/commands.log"
}

run_fleet quick worker-a,worker-b "${TMP_DIR}/quick"
run_fleet full worker-a,worker-b "${TMP_DIR}/full"
[[ "$(find "${TMP_DIR}/full" -mindepth 2 -name report.json | wc -l)" -eq 8 ]]

MOCK_FAIL_WORKER=worker-a RW_FLEET_ARTIFACT_DIR="${TMP_DIR}/mixed" \
  bash "$FLEET_RUNNER" --mode quick --workers worker-a,worker-b --serial >/dev/null 2>&1 || true
jq -e '.overall == "FAIL" and (.workers|length) == 2 and ([.workers[] | select(.worker_id == "worker-b" and .status == "PASS")] | length) == 1' \
  "${TMP_DIR}/mixed/fleet-report.json" >/dev/null

if bash "$FLEET_RUNNER" --mode quick --workers 'worker a,worker-b' --artifact-dir "${TMP_DIR}/invalid-whitespace" >/dev/null 2>&1; then
  printf 'FAIL: whitespace worker ID unexpectedly accepted\n' >&2
  exit 1
fi

if bash "$FLEET_RUNNER" --mode quick --workers worker-a,worker-a --artifact-dir "${TMP_DIR}/duplicate" >/dev/null 2>&1; then
  printf 'FAIL: duplicate workers unexpectedly accepted\n' >&2
  exit 1
fi

if VELOX_CERT_ENV=production VELOX_CERT_ALLOW_DESTRUCTIVE=1 \
   VELOX_CERT_DESTRUCTIVE_ACK=I_UNDERSTAND_DESTRUCTIVE_CERT \
   RW_WORKER_CRASH_CMD='true' RW_JOB_DESTINATION_ID=destination \
   bash "$FLEET_RUNNER" --mode destructive --workers worker-a --artifact-dir "${TMP_DIR}/prod" >/dev/null 2>&1; then
  printf 'FAIL: destructive production run unexpectedly accepted\n' >&2
  exit 1
fi

VELOX_CERT_ENV=test VELOX_CERT_ALLOW_DESTRUCTIVE=1 \
VELOX_CERT_DESTRUCTIVE_ACK=I_UNDERSTAND_DESTRUCTIVE_CERT \
RW_WORKER_CRASH_CMD='true' RW_JOB_DESTINATION_ID=destination \
RW_FLEET_ARTIFACT_DIR="${TMP_DIR}/destructive" \
bash "$FLEET_RUNNER" --mode destructive --workers worker-a,worker-b --serial >/dev/null
jq -e '.overall == "PASS" and .mode == "destructive" and (.workers|length) == 2' \
  "${TMP_DIR}/destructive/fleet-report.json" >/dev/null

printf 'PASS: fleet certification wrapper offline tests\n'
