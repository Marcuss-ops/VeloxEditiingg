# deploy/openbao/policies/master.hcl
# ─────────────────────────────────────────────────────────────────────────────
# Policy del MASTER (deployment): READ-ONLY su:
#   velox/production/master/*        (admin-token, JWT, social, HMAC)
#   velox/production/workers/*       (credential + certificati di TUTTI i worker)
#   velox/production/services/registry/*  (pull credentials, consumati dal
#                                          deploy master / worker image runtime)
#
# Il master NON deve poter scrivere/eliminare segreti: i write restano al
# provisioning (root token oggi, admin AppRole domani) e alla rotazione.
# KV v2 → i path reali sono velox/data/<path> (lettura) e
# velox/metadata/<path> (list/versioni).

path "velox/data/production/master/*" {
  capabilities = ["read", "list"]
}

path "velox/metadata/production/master/*" {
  capabilities = ["read", "list"]
}

path "velox/data/production/workers/*" {
  capabilities = ["read", "list"]
}

path "velox/metadata/production/workers/*" {
  capabilities = ["read", "list"]
}

path "velox/data/production/services/registry/*" {
  capabilities = ["read", "list"]
}

path "velox/metadata/production/services/registry/*" {
  capabilities = ["read", "list"]
}
