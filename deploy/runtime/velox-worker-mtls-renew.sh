#!/usr/bin/env bash
# Renew the worker mTLS bundle and reload the running worker only when the
# resolver selected a new bundle. A cache hit does not restart the worker.
set -euo pipefail

CERTS_ROOT="${VELOX_WORKER_CERTS_DIR:-/etc/velox-worker/certs}"
FETCHER="${VELOX_MTLS_FETCHER:-/opt/velox-worker/openbao-fetch-worker-secrets.sh}"
BEFORE="$(readlink -e "$CERTS_ROOT/current" 2>/dev/null || true)"
"$FETCHER" --renew
AFTER="$(readlink -e "$CERTS_ROOT/current" 2>/dev/null || true)"

if [[ -n "$AFTER" && "$AFTER" != "$BEFORE" ]]; then
    systemctl try-restart velox-worker.service
fi
