# deploy/openbao/policies/ssh-operator.hcl
# ─────────────────────────────────────────────────────────────────────────────
# Policy SSH-OPERATOR (firma certificati SSH): il MINIMO per firmare le public
# key degli operatori contro la CA SSH di OpenBao e leggere la chiave pubblica
# della CA (per TrustedUserCAKeys sui nodi). Nessun accesso al KV velox/*,
# nessuna gestione di role/policy/auth.
#
# Consumatore: script deploy/openbao/scripts/sign-operator-ssh.sh con un token
# AppRole `ssh-operator` (crea con ./scripts/provision-approle.sh --principal
# ssh-operator). Nessun token admin/root è un percorso operativo supportato
# per la firma dei certificati; lo script verifica la policy del token via
# auth/token/lookup-self prima di firmare.
#
# NB: la firma NON è un segreto rivelandolo: il cert firmato ha TTL breve
# (default 30m) e principals limitati (velox-admin/velox-deploy) — la chiave
# privata dell'operatore non lascia mai la sua macchina.

path "ssh/sign/*" {
  capabilities = ["update"]
}

path "ssh/config/ca" {
  capabilities = ["read"]
}

path "ssh/roles/*" {
  capabilities = ["read"]
}

path "sys/health" {
  capabilities = ["read"]
}
