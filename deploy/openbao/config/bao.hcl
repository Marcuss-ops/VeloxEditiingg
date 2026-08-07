# OpenBao server configuration — Velox central secrets manager.
# Loaded by the container via `server -config=/openbao/config/bao.hcl`.
# Reference: https://openbao.org/docs/configuration/
#
# NOTE: OpenBao 2.x removed the mlock requirement; `disable_mlock` is no
# longer a valid config key and MUST NOT be added back.

ui = true

# Advertised addresses. Raft REQUIRES cluster_addr. The single-node local
# deployment advertises the loopback addresses; when OpenBao moves onto a
# dedicated host, replace these with the real hostname (and regenerate the
# TLS cert with matching SANs — see scripts/gen-tls.sh).
api_addr     = "https://127.0.0.1:8200"
cluster_addr = "https://127.0.0.1:8201"

# Integrated (Raft) storage — HA-ready, recommended production backend.
# Path: /openbao/file — the ONLY data-capable directory shipped in the
# openbao container image (owned by the openbao user, uid 100). A named
# volume mounted there inherits that ownership, so the non-root container
# user can write without any chown gymnastics. (/openbao/data does NOT
# exist in the image; a volume there would be root-owned and crash the
# server with "permission denied" on vault.db.)
storage "raft" {
  path    = "/openbao/file"
  node_id = "openbao-1"
  # performance_multiplier = 1   # tune for production-grade timing
}

listener "tcp" {
  address         = "0.0.0.0:8200"
  tls_cert_file   = "/openbao/tls/server.crt"
  tls_key_file    = "/openbao/tls/server.key"
  tls_min_version = "tls12"
  tls_max_version = "tls13"
}

log_level = "info"
