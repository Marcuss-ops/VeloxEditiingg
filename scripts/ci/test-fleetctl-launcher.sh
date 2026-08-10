#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SCRIPT="$ROOT/scripts/fleetctl"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

MOCK="$TMP/fleetctl"
ARGS="$TMP/args"
ENV_OUT="$TMP/env"
cat >"$MOCK" <<'MOCK'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$@" >"$FLEETCTL_TEST_ARGS"
{
    printf 'master=%s\n' "${VELOX_MASTER_URL:-}"
    printf 'token=%s\n' "${VELOX_ADMIN_TOKEN:-}"
} >"$FLEETCTL_TEST_ENV"
MOCK
chmod +x "$MOCK"

TOKEN_FILE="$TMP/token"
printf 'VELOX_ADMIN_TOKEN=file-token\n' >"$TOKEN_FILE"
chmod 600 "$TOKEN_FILE"

FLEETCTL_TEST_ARGS="$ARGS" \
FLEETCTL_TEST_ENV="$ENV_OUT" \
FLEETCTL_GO_BIN="$MOCK" \
VELOX_MASTER_URL="https://master.example.test/" \
TOKEN_FILE="$TOKEN_FILE" \
VELOX_ADMIN_TOKEN='' \
    "$SCRIPT" inspect worker-1

expected_args=$'inspect\nworker-1\n--master=https://master.example.test'
actual_args="$(cat "$ARGS")"
[[ "$actual_args" == "$expected_args" ]] || {
    printf 'FAIL: delegated args differ\nwant:\n%s\ngot:\n%s\n' "$expected_args" "$actual_args" >&2
    exit 1
}
grep -Fxq 'master=https://master.example.test' "$ENV_OUT"
grep -Fxq 'token=file-token' "$ENV_OUT"

ARGS="$TMP/reason-args"
FLEETCTL_TEST_ARGS="$ARGS" \
FLEETCTL_TEST_ENV="$ENV_OUT" \
FLEETCTL_GO_BIN="$MOCK" \
VELOX_MASTER_URL="https://master.example.test" \
VELOX_ADMIN_TOKEN='env-token' \
    "$SCRIPT" drain worker-1 "manual drain"
expected_args=$'drain\nworker-1\n--reason\nmanual drain\n--master=https://master.example.test'
actual_args="$(cat "$ARGS")"
[[ "$actual_args" == "$expected_args" ]] || {
    printf 'FAIL: positional reason was not translated\nwant:\n%s\ngot:\n%s\n' "$expected_args" "$actual_args" >&2
    exit 1
}

ARGS="$TMP/status-args"
FLEETCTL_TEST_ARGS="$ARGS" \
FLEETCTL_TEST_ENV="$ENV_OUT" \
FLEETCTL_GO_BIN="$MOCK" \
VELOX_MASTER_URL="https://master.example.test" \
VELOX_ADMIN_TOKEN='env-token' \
    "$SCRIPT" status --json
expected_args=$'status\n--json\n--master=https://master.example.test'
actual_args="$(cat "$ARGS")"
[[ "$actual_args" == "$expected_args" ]] || {
    printf 'FAIL: status JSON delegation differs\nwant:\n%s\ngot:\n%s\n' "$expected_args" "$actual_args" >&2
    exit 1
}

ARGS="$TMP/update-args"
FLEETCTL_TEST_ARGS="$ARGS" \
FLEETCTL_TEST_ENV="$ENV_OUT" \
FLEETCTL_GO_BIN="$MOCK" \
VELOX_MASTER_URL="https://master.example.test" \
VELOX_ADMIN_TOKEN='env-token' \
    "$SCRIPT" update worker-1 ghcr.io/example/worker@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa "manual update"
expected_args=$'update\nworker-1\nghcr.io/example/worker@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\nmanual update\n--master=https://master.example.test'
actual_args="$(cat "$ARGS")"
[[ "$actual_args" == "$expected_args" ]] || {
    printf 'FAIL: update delegation differs\nwant:\n%s\ngot:\n%s\n' "$expected_args" "$actual_args" >&2
    exit 1
}

ARGS="$TMP/rollback-args"
FLEETCTL_TEST_ARGS="$ARGS" \
FLEETCTL_TEST_ENV="$ENV_OUT" \
FLEETCTL_GO_BIN="$MOCK" \
VELOX_MASTER_URL="https://master.example.test" \
VELOX_ADMIN_TOKEN='env-token' \
    "$SCRIPT" rollback worker-1 --digest sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb --reason "restore known-good"
expected_args=$'rollback\nworker-1\n--digest\nsha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\n--reason\nrestore known-good\n--master=https://master.example.test'
actual_args="$(cat "$ARGS")"
[[ "$actual_args" == "$expected_args" ]] || {
    printf 'FAIL: rollback delegation differs\nwant:\n%s\ngot:\n%s\n' "$expected_args" "$actual_args" >&2
    exit 1
}

ARGS="$TMP/operations-args"
FLEETCTL_TEST_ARGS="$ARGS" \
FLEETCTL_TEST_ENV="$ENV_OUT" \
FLEETCTL_GO_BIN="$MOCK" \
VELOX_MASTER_URL="https://master.example.test" \
VELOX_ADMIN_TOKEN='env-token' \
    "$SCRIPT" operations worker-1 RUNNING
expected_args=$'operations\nworker-1\nRUNNING\n--master=https://master.example.test'
actual_args="$(cat "$ARGS")"
[[ "$actual_args" == "$expected_args" ]] || {
    printf 'FAIL: operations delegation differs\nwant:\n%s\ngot:\n%s\n' "$expected_args" "$actual_args" >&2
    exit 1
}

ARGS="$TMP/wait-ready-args"
FLEETCTL_TEST_ARGS="$ARGS" \
FLEETCTL_TEST_ENV="$ENV_OUT" \
FLEETCTL_GO_BIN="$MOCK" \
VELOX_MASTER_URL="https://master.example.test" \
VELOX_ADMIN_TOKEN='env-token' \
    "$SCRIPT" wait-ready worker-1 --timeout 10 --poll 1
expected_args=$'wait-ready\nworker-1\n--timeout\n10\n--poll\n1\n--master=https://master.example.test'
actual_args="$(cat "$ARGS")"
[[ "$actual_args" == "$expected_args" ]] || {
    printf 'FAIL: wait-ready delegation differs\nwant:\n%s\ngot:\n%s\n' "$expected_args" "$actual_args" >&2
    exit 1
}

ARGS="$TMP/job-inspect-args"
FLEETCTL_TEST_ARGS="$ARGS" \
FLEETCTL_TEST_ENV="$ENV_OUT" \
FLEETCTL_GO_BIN="$MOCK" \
VELOX_MASTER_URL="https://master.example.test" \
VELOX_ADMIN_TOKEN='env-token' \
    "$SCRIPT" job inspect job-1 --json
expected_args=$'job\ninspect\njob-1\n--json\n--master=https://master.example.test'
actual_args="$(cat "$ARGS")"
[[ "$actual_args" == "$expected_args" ]] || {
    printf 'FAIL: job inspect delegation differs\nwant:\n%s\ngot:\n%s\n' "$expected_args" "$actual_args" >&2
    exit 1
}

ARGS="$TMP/job-watch-args"
FLEETCTL_TEST_ARGS="$ARGS" \
FLEETCTL_TEST_ENV="$ENV_OUT" \
FLEETCTL_GO_BIN="$MOCK" \
VELOX_MASTER_URL="https://master.example.test" \
VELOX_ADMIN_TOKEN='env-token' \
    "$SCRIPT" job watch job-1 --timeout 10 --poll 1 --json
expected_args=$'job\nwatch\njob-1\n--timeout\n10\n--poll\n1\n--json\n--master=https://master.example.test'
actual_args="$(cat "$ARGS")"
[[ "$actual_args" == "$expected_args" ]] || {
    printf 'FAIL: job watch delegation differs\nwant:\n%s\ngot:\n%s\n' "$expected_args" "$actual_args" >&2
    exit 1
}

ARGS="$TMP/job-metrics-args"
FLEETCTL_TEST_ARGS="$ARGS" \
FLEETCTL_TEST_ENV="$ENV_OUT" \
FLEETCTL_GO_BIN="$MOCK" \
VELOX_MASTER_URL="https://master.example.test" \
VELOX_ADMIN_TOKEN='env-token' \
    "$SCRIPT" job metrics job-1
expected_args=$'job\nmetrics\njob-1\n--master=https://master.example.test'
actual_args="$(cat "$ARGS")"
[[ "$actual_args" == "$expected_args" ]] || {
    printf 'FAIL: job metrics delegation differs\nwant:\n%s\ngot:\n%s\n' "$expected_args" "$actual_args" >&2
    exit 1
}

printf '{}\n' >"$TMP/payload.json"
ARGS="$TMP/job-submit-args"
FLEETCTL_TEST_ARGS="$ARGS" \
FLEETCTL_TEST_ENV="$ENV_OUT" \
FLEETCTL_GO_BIN="$MOCK" \
VELOX_MASTER_URL="https://master.example.test" \
VELOX_ADMIN_TOKEN='env-token' \
    "$SCRIPT" job submit --payload "$TMP/payload.json" 2>/dev/null || true
# A submit invocation must not be delegated to the Go mock. The Bash path
# reaches its own payload validation before any network call in this fixture.
[[ ! -s "$ARGS" ]] || {
    printf 'FAIL: job submit was delegated unexpectedly\n' >&2
    exit 1
}

echo 'fleetctl launcher delegation: PASS'
