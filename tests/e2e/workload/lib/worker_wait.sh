# shellcheck shell=bash

wait_for_worker_registration() {
  local pid="$1"
  for i in $(seq 1 20); do
    if grep -qE "Worker ${WORKER_ID} connected" "$MASTER_LOG" 2>/dev/null \
      || grep -q "Registration successful" "$WORKER_LOG" 2>/dev/null; then
      pass "worker registered after ${i}s"
      sleep 2
      return 0
    fi
    if ! kill -0 "$pid" 2>/dev/null; then
      fail "worker crashed during registration"
      tail -40 "$WORKER_LOG" 2>/dev/null || true
      exit 1
    fi
    sleep 2
  done
  fail "worker did not register within 40s"
  tail -20 "$MASTER_LOG" 2>/dev/null || true
  tail -20 "$WORKER_LOG" 2>/dev/null || true
  exit 1
}
