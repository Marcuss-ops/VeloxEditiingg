#!/usr/bin/env bash
# DataServer/run-tests-postgres.sh
#
# Spins up a fresh postgres testcontainer, exports VELOX_TEST_POSTGRES_DSN
# so cmd/server's Postgres boot integration test can find it, runs the
# cmd/server test flow, and tears down the container on exit. Idempotent —
# rerunning the script stops any previous container with the same name
# and starts a fresh one so each CI run sees a clean database.
#
# Why this exists: cmd/server's TestBuildServerDeps_PostgresDispatch_*
# subtests are env-gated; without this helper every contributor who
# wants to repro the dispatch-path assertion has to remember the
# docker incantation. This script captures the canonical incantation
# in version control so it stays correct as Postgres versions change.
#
# The Postgres version (16-alpine) is pinned so the runner + tooling
# assumptions stay stable. pgx/v5 supports back to PG 11; bumping
# 16 → 17 is fine but should be a deliberate bump in this script.
#
# Usage:
#   DataServer/run-tests-postgres.sh           # spins up + tests + tears down
#   DataServer/run-tests-postgres.sh --keep    # keeps the container after exit
#                                              # (useful for poking around
#                                              # after a failed test)
#   VELOX_TEST_POSTGRES_DSN=postgres://... go test ./cmd/server/...   # bypass
#
# Exit codes:
#   0 — postgres came up + tests passed
#   1 — postgres didn't come up within the pg_isready timeout
#   2 — tests FAILED (or pre-flight checks failed: docker / curl missing)

set -euo pipefail

CONTAINER_NAME="velox-postgres-test"
PG_IMAGE="postgres:16-alpine"
PG_PORT="${VELOX_TEST_POSTGRES_PORT:-5432}"
PG_USER="${VELOX_TEST_POSTGRES_USER:-postgres}"
PG_PASSWORD="${VELOX_TEST_POSTGRES_PASSWORD:-velox-test-pass}"
PG_DB="${VELOX_TEST_POSTGRES_DB:-postgres}"
PG_READY_TIMEOUT="${VELOX_TEST_POSTGRES_READY_TIMEOUT:-30}"
KEEP_CONTAINER=0
if [ "${1:-}" = "--keep" ]; then
    KEEP_CONTAINER=1
fi

cleanup() {
    if [ "${KEEP_CONTAINER}" -eq 0 ]; then
        echo "==> Tearing down test container ${CONTAINER_NAME}"
        docker rm -f "${CONTAINER_NAME}" >/dev/null 2>&1 || true
    else
        echo "==> --keep set; leaving ${CONTAINER_NAME} running on host port ${PG_PORT}"
        echo "    To tear down manually: docker rm -f ${CONTAINER_NAME}"
    fi
}
trap cleanup EXIT

# Pre-flight: docker + curl (pg_isready check via docker exec so we
# don't pull a pg client into the host image).
command -v docker >/dev/null 2>&1 || { echo "ERROR: docker not on PATH"; exit 2; }
command -v curl   >/dev/null 2>&1 || echo "WARNING: curl missing — pg_isready check will use docker exec only"
command -v go     >/dev/null 2>&1 || { echo "ERROR: go not on PATH"; exit 2; }

# Idempotency: if a stale container with our name is around, stop it
# so this run starts against a fresh database.
if docker ps -a --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    echo "==> Found stale container ${CONTAINER_NAME}; removing"
    docker rm -f "${CONTAINER_NAME}" >/dev/null
fi

echo "==> Starting ${PG_IMAGE} as ${CONTAINER_NAME} on port ${PG_PORT}"
docker run -d \
    --name "${CONTAINER_NAME}" \
    -e POSTGRES_USER="${PG_USER}" \
    -e POSTGRES_PASSWORD="${PG_PASSWORD}" \
    -e POSTGRES_DB="${PG_DB}" \
    -p "${PG_PORT}:5432" \
    "${PG_IMAGE}" >/dev/null

# Compose the DSN with urlencoded password so special chars in the
# password don't break pgx's URL-form parser. Uses jq when present,
# python3 when not — neither requires the pg client.
PG_PASSWORD_ESCAPED="${PG_PASSWORD}"
if command -v jq >/dev/null 2>&1; then
    PG_PASSWORD_ESCAPED="$(printf %s "${PG_PASSWORD}" | jq -sRr @uri)"
elif command -v python3 >/dev/null 2>&1; then
    PG_PASSWORD_ESCAPED="$(python3 -c 'import sys, urllib.parse; print(urllib.parse.quote(sys.argv[1], safe=""))' "${PG_PASSWORD}")"
else
    echo "ERROR: need jq OR python3 to url-escape PG_PASSWORD; install one and rerun"
    exit 2
fi

export VELOX_TEST_POSTGRES_DSN="postgres://${PG_USER}:${PG_PASSWORD_ESCAPED}@127.0.0.1:${PG_PORT}/${PG_DB}?sslmode=disable"
echo "==> VELOX_TEST_POSTGRES_DSN=${VELOX_TEST_POSTGRES_DSN}"

echo "==> Waiting for postgres to accept connections (timeout ${PG_READY_TIMEOUT}s)"
deadline=$((SECONDS + PG_READY_TIMEOUT))
ready=0
while [ "${SECONDS}" -lt "${deadline}" ]; do
    if docker exec "${CONTAINER_NAME}" pg_isready -U "${PG_USER}" -d "${PG_DB}" >/dev/null 2>&1; then
        ready=1
        break
    fi
    sleep 1
done
if [ "${ready}" -ne 1 ]; then
    echo "ERROR: postgres not ready within ${PG_READY_TIMEOUT}s"
    docker logs "${CONTAINER_NAME}" >&2
    exit 1
fi
echo "==> Postgres is ready"

echo "==> Running: go test ./cmd/server/..."
cd "$(dirname "$0")"
go test -v ./cmd/server/...
