#!/usr/bin/env bash
# Self-test for check-ac-taskresult-convergence.sh. Uses isolated SQLite
# fixtures so it is safe to run in CI and never touches a production DB.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
GATE="$REPO_ROOT/scripts/ci/check-ac-taskresult-convergence.sh"
TMP="$(mktemp -d /tmp/velox-ac-convergence-test.XXXXXX)"
trap 'rm -rf "$TMP"' EXIT

DB="$TMP/master.db"
SPOOL="$TMP/spool.db"
LOG="$TMP/worker.log"
JOB="job-convergence-test"
TASK="task-convergence-test"
ATTEMPT="attempt-convergence-test"
COMMIT="commit-convergence-test"
ARTIFACT="artifact-convergence-test"
DECL="declaration-convergence-test"
DELIVERY="delivery-convergence-test"
NOW="2026-08-04T12:00:00Z"

sqlite3 "$DB" <<SQL
CREATE TABLE jobs (job_id TEXT PRIMARY KEY, status TEXT, updated_at TEXT);
CREATE TABLE tasks (task_id TEXT PRIMARY KEY, job_id TEXT, status TEXT, lease_expires_at TEXT, winning_attempt_id TEXT);
CREATE TABLE task_attempts (id TEXT PRIMARY KEY, task_id TEXT, job_id TEXT, status TEXT, worker_id TEXT, lease_id TEXT);
CREATE TABLE attempt_commits (commit_id TEXT PRIMARY KEY, task_id TEXT, attempt_id TEXT, job_id TEXT, status TEXT);
CREATE TABLE task_output_declarations (declaration_id TEXT PRIMARY KEY, commit_id TEXT, task_id TEXT, attempt_id TEXT, artifact_id TEXT, status TEXT);
CREATE TABLE artifacts (id TEXT PRIMARY KEY, job_id TEXT, output_kind TEXT, status TEXT, sha256 TEXT, size_bytes INTEGER);
CREATE TABLE artifact_uploads (upload_id TEXT PRIMARY KEY, artifact_id TEXT, job_id TEXT, status TEXT, expires_at TEXT);
CREATE TABLE delivery_destinations (destination_id TEXT PRIMARY KEY, provider TEXT);
CREATE TABLE job_deliveries (delivery_id TEXT PRIMARY KEY, artifact_id TEXT, destination_id TEXT, status TEXT, remote_id TEXT);
INSERT INTO delivery_destinations VALUES ('destination-convergence','drive');
INSERT INTO jobs VALUES ('$JOB','SUCCEEDED','$NOW');
INSERT INTO tasks VALUES ('$TASK','$JOB','SUCCEEDED','','$ATTEMPT');
INSERT INTO task_attempts VALUES ('$ATTEMPT','$TASK','$JOB','SUCCEEDED','worker-convergence','lease-convergence');
INSERT INTO attempt_commits VALUES ('$COMMIT','$TASK','$ATTEMPT','$JOB','COMMITTED');
INSERT INTO artifacts VALUES ('$ARTIFACT','$JOB','final_video','READY','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',1234);
INSERT INTO task_output_declarations VALUES ('$DECL','$COMMIT','$TASK','$ATTEMPT','$ARTIFACT','READY');
INSERT INTO artifact_uploads VALUES ('upload-convergence','$ARTIFACT','$JOB','COMPLETED','$NOW');
INSERT INTO job_deliveries VALUES ('$DELIVERY','$ARTIFACT','destination-convergence','COMPLETED','drive-file-canonical-123');
SQL

sqlite3 "$SPOOL" <<SQL
CREATE TABLE worker_output_spool (spool_id TEXT PRIMARY KEY, task_id TEXT, status TEXT, updated_at TEXT);
CREATE TABLE task_result_outbox (task_id TEXT, attempt_id TEXT, report_hash TEXT);
SQL
cat > "$LOG" <<EOF
INFO [TASK_RESULT_OUTBOX] TaskResultAck received task=$TASK attempt=$ATTEMPT error=""
INFO ARTIFACT_PROTOCOL {"event":"TASK_COMMIT_ACK_RECEIVED","job_id":"$JOB","task_id":"$TASK","attempt_id":"$ATTEMPT","lease_id":"lease-convergence","commit_id":"$COMMIT"}
EOF

run_gate() {
  "$GATE" --db "$DB" --job-id "$JOB" --worker-spool-db "$SPOOL" --worker-log "$LOG"
}

run_gate >/dev/null

# Each mutation must make the gate fail closed. Restore the positive fixture
# after every assertion so the cases remain independent and deterministic.
expect_failure() {
  local name="$1" sql="$2"
  sqlite3 "$DB" "$sql"
  if run_gate >"$TMP/$name.out" 2>&1; then
    cat "$TMP/$name.out" >&2
    echo "FAIL: negative case $name unexpectedly passed" >&2
    exit 1
  fi
  echo "OK: negative case $name rejected"
}

expect_failure "running-task" "UPDATE tasks SET status='RUNNING' WHERE task_id='$TASK';"
sqlite3 "$DB" "UPDATE tasks SET status='SUCCEEDED' WHERE task_id='$TASK';"
expect_failure "missing-winning-attempt" "UPDATE tasks SET winning_attempt_id='' WHERE task_id='$TASK';"
sqlite3 "$DB" "UPDATE tasks SET winning_attempt_id='$ATTEMPT' WHERE task_id='$TASK';"
expect_failure "non-terminal-delivery" "UPDATE job_deliveries SET status='PENDING' WHERE delivery_id='$DELIVERY';"
sqlite3 "$DB" "UPDATE job_deliveries SET status='COMPLETED' WHERE delivery_id='$DELIVERY';"
expect_failure "expired-lease" "UPDATE tasks SET status='LEASED', lease_expires_at='2000-01-01T00:00:00Z' WHERE task_id='$TASK';"
sqlite3 "$DB" "UPDATE tasks SET status='SUCCEEDED', lease_expires_at='' WHERE task_id='$TASK';"
expect_failure "missing-drive-id" "UPDATE job_deliveries SET remote_id='' WHERE delivery_id='$DELIVERY';"
sqlite3 "$DB" "UPDATE job_deliveries SET remote_id='drive-file-canonical-123' WHERE delivery_id='$DELIVERY';"
sqlite3 "$SPOOL" "INSERT INTO task_result_outbox VALUES ('$TASK','$ATTEMPT','hash');"
if run_gate >"$TMP/pending-task-result.out" 2>&1; then
  cat "$TMP/pending-task-result.out" >&2
  echo "FAIL: negative case pending-task-result unexpectedly passed" >&2
  exit 1
fi
echo "OK: negative case pending-task-result rejected"
sqlite3 "$SPOOL" "DELETE FROM task_result_outbox;"
sqlite3 "$SPOOL" "INSERT INTO worker_output_spool VALUES ('stale-spool','$TASK','UPLOADING','2000-01-01T00:00:00Z');"
if run_gate >"$TMP/stale-spool.out" 2>&1; then
  cat "$TMP/stale-spool.out" >&2
  echo "FAIL: negative case stale-spool unexpectedly passed" >&2
  exit 1
fi
echo "OK: negative case stale-spool rejected"
sqlite3 "$SPOOL" "DELETE FROM worker_output_spool;"
# Remove the worker-side commit ACK marker; the DB remains converged, so
# this specifically proves the worker-consumed ACK evidence is mandatory.
TEMP_LOG="$TMP/worker-without-commit-ack.log"
sed '/TASK_COMMIT_ACK_RECEIVED/d' "$LOG" > "$TEMP_LOG"
if "$GATE" --db "$DB" --job-id "$JOB" --worker-spool-db "$SPOOL" --worker-log "$TEMP_LOG" >"$TMP/missing-commit-ack.out" 2>&1; then
  cat "$TMP/missing-commit-ack.out" >&2
  echo "FAIL: negative case missing-commit-ack unexpectedly passed" >&2
  exit 1
fi
echo "OK: negative case missing-commit-ack rejected"

# Restore the marker and prove the gate returns green again.
sed -i '/TASK_COMMIT_ACK_RECEIVED/d' "$LOG"
printf 'INFO ARTIFACT_PROTOCOL {"event":"TASK_COMMIT_ACK_RECEIVED","job_id":"%s","task_id":"%s","attempt_id":"%s"}\n' "$JOB" "$TASK" "$ATTEMPT" >> "$LOG"
run_gate >/dev/null
echo "[test-ac-taskresult-convergence] OK"
