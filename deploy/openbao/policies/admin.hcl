# deploy/openbao/policies/admin.hcl
# ─────────────────────────────────────────────────────────────────────────────
# Policy ADMIN (operatore): più ampia di master, ma NON root.
#   velox/*                     → CRUD completo (provisioning, rotazione)
#   auth/approle/*              → gestione machine identity (crea/ruota role,
#                                 genera secret-id)
#   sys/policies/acl/*          → gestione policy
#   ssh/*                       → gestione CA SSH (config/ca, role, firma)
#   sys/health                  → sola lettura stato
#
# Non include path sudo (sys/raw, sys/auth, ecc.): il root token resta
# materiale di bootstrap/recovery separato.

path "velox/data/*" {
  capabilities = ["create", "read", "update", "delete", "list"]
}

path "velox/metadata/*" {
  capabilities = ["create", "read", "update", "delete", "list"]
}

path "auth/approle/*" {
  capabilities = ["create", "read", "update", "delete", "list"]
}

path "sys/policies/acl/*" {
  capabilities = ["create", "read", "update", "delete", "list"]
}

path "ssh/*" {
  capabilities = ["create", "read", "update", "delete", "list"]
}

path "sys/health" {
  capabilities = ["read"]
}
