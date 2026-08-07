#!/usr/bin/env bash
# =============================================================================
# remote-worker-cert-config.sh — shared configuration for remote-worker certs.
#
# Source from a certification script:
#   source scripts/cert/remote-worker-cert-config.sh
#   rw_load_config
#   rw_remote_worker_preflight
#   admin_api GET "/api/v1/admin/workers/${WORKER_ID}"
#
# Direct execution performs local validation/certification through the modular
# implementation under scripts/cert/lib and never prints secret values.
# =============================================================================

RW_CERT_CONFIG_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

source "${RW_CERT_CONFIG_DIR}/lib/remote-worker-cert-artifacts.sh"
source "${RW_CERT_CONFIG_DIR}/lib/remote-worker-cert-config.sh"
source "${RW_CERT_CONFIG_DIR}/lib/remote-worker-cert-network.sh"
source "${RW_CERT_CONFIG_DIR}/lib/remote-worker-cert-worker.sh"
source "${RW_CERT_CONFIG_DIR}/lib/remote-worker-cert-lifecycle.sh"
source "${RW_CERT_CONFIG_DIR}/lib/remote-worker-cert-update.sh"
source "${RW_CERT_CONFIG_DIR}/lib/remote-worker-cert-job.sh"
source "${RW_CERT_CONFIG_DIR}/lib/remote-worker-cert-runner.sh"

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  rw_cert_main "$@"
fi
