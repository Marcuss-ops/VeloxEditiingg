# Velox — Censimento dei Secret (Secrets Audit)

> **Stato:** operativo — censimento aggiornato al 2026-08-07
> **Scopo:** inventario completo dei secret oggi presenti nell'ecosistema Velox
> (repo, CI, master, worker, workstation operatore) come **baseline di partenza**
> per la migrazione verso un secrets manager centrale (OpenBao).
> **Metodo:** scansione di template committati (`*.example`), playbook Ansible,
> template Jinja, workflow GitHub Actions, script shell, codice Go e documentazione.
> Nessun valore reale di secret è riportato in questo documento — solo posizione,
> formato, referente e tipo.

---

## 1. Stato attuale in sintesi

Oggi i secret vivono in **9 aree diverse**, con 4 meccanismi principali:

| Meccanismo | Dove | Contenuto |
|---|---|---|
| **Ansible Vault** | `deploy/group_vars/vault.yml` (locale, cifrato) | token admin, JWT secret, social token, worker credentials, pubkey operatore |
| **File env su host** | `/etc/velox-server.env`, `/etc/velox-worker/worker.env`, `/etc/velox-worker-<host>.env` | token, JWT secret, `VELOX_WORKER_SECRET` |
| **File secret su disco** | `/etc/velox-worker/secrets/worker_credential`, `/opt/velox/secrets/admin-token`, `DataServer/data/drive/credentials/` | credential worker, admin token, OAuth Drive |
| **PKI / certificati** | `scripts/gen-production-pki.sh` (root CA air-gapped, intermediate online), cert leaf per worker | chiavi private + certificati mTLS |

Obiettivo della migrazione (fasi successive): ridurre tutto a **0 password statiche,
0 secret nel repo, 0 token condivisi** — con OpenBao come unica fonte di verità
(KV, machine identity per-worker, policy, SSH CA, PKI, credenziali DB dinamiche).

---

## 2. Catalogo per area

### 2.1 Ansible Vault — variabili `vault_velox_*`

File template: `deploy/group_vars/vault.yml.example` (il file reale `vault.yml` è
cifrato con `ansible-vault`, locale/gitignored; la password di decifratura sta in
`~/.vault-velox-pass` (0600) o in `secrets.ANSIBLE_VAULT_PASSWORD` per la CI).

| Variabile | Env renderizzato | Referente (consumatore) | Tipo |
|---|---|---|---|
| `vault_velox_admin_token` | `VELOX_ADMIN_TOKEN` (via `deploy/templates/velox-server.env.j2`) | Master REST `AdminAuthMiddleware`; playbook `fleet-*` (`fleet-status/rollback/update/drain/health/smoke`) come `Authorization: Bearer` | Token statico (admin) |
| `vault_velox_instaedit_control_jwt_secret` | `INSTAEDIT_CONTROL_JWT_SECRET` (j2) | `DataServer/internal/instaeditauth/verifier.go` (HS256, ≥32 byte); **valore accoppiato** con `VELOX_CONTROL_JWT_SECRET` lato InstaEdit BFF (Fly secrets) — drift = 401 su `/api/v1/velox/*` | Secret condiviso JWT |
| `vault_velox_social_api_token` | `SOCIAL_API_TOKEN` (j2) | `DataServer/internal/socialclient/client.go` (`ConfigFromEnv`), Bearer verso la Social API | Token statico (API) |
| `vault_velox_social_webhook_secret` | `SOCIAL_WEBHOOK_SECRET` (j2) | (forward-looking) HMAC per callback `social_repo` → Velox — nessun consumatore Go oggi | Secret HMAC (futuro) |
| `vault_velox_registry_username` | — | Dichiarata in `vault.yml.example` ("container registry pull credentials, used by worker image runtime") — **nessun consumatore Ansible/Jinja tracciato**; il pull GHCR in CI usa `secrets.GITHUB_TOKEN` | Credenziali registry |
| `vault_velox_registry_token` | — | come sopra | Token registry |
| `vault_velox_worker_credential_{1,2,3}` | `worker_secret` per-host → `VELOX_WORKER_SECRET` in `/etc/velox-worker-<host>.env` (via `canonical_worker_runtime.yml`, `no_log`) | Worker agent (`internal/bootstrap/config.go:181`) → SHA-256 `credential_hash` → validata contro tabella SQLite `worker_credentials` sul master | Credential per-worker (statica) |
| `vault_velox_operator_pubkey` | — | `deploy/playbooks/bootstrap-ssh.yml` → `/home/velox-deploy/.ssh/authorized_keys` | Chiave pubblica SSH (identità, bassa segretezza) |

> Nota storica: `vault_velox_sudo_password` e `vault_velox_social_gateway_api_key`
> sono stati **rimossi** (CHANGELOG: commit `09f5c9c`, PR-15.10). Il sudo è
> passwordless via `velox-deploy`; l'alias gateway è rinominato in `social_api_token`.

### 2.2 Master — env file

Template: `deploy/velox-server.env.example` → `/etc/velox-server.env` (renderizzato
da `deploy/templates/velox-server.env.j2`; valicato da `deploy/validate-master-env.sh`).

| Variabile | Referente | Tipo | Note |
|---|---|---|---|
| `VELOX_ADMIN_TOKEN` | AdminAuthMiddleware REST | Token statico | placeholder `CHANGE_ME_...` nel template; da vault |
| `INSTAEDIT_CONTROL_JWT_SECRET` | verifier InstaEdit (HS256) | Secret condiviso | ≥32 byte; accoppiato con InstaEdit |
| `SOCIAL_API_TOKEN` | socialclient | Token statico | da vault |
| `SOCIAL_WEBHOOK_SECRET` | (futuro) HMAC callback | Secret HMAC | da vault |
| `VELOX_COMMIT_HMAC_KEY` | Completion coordinator / firma HMAC artifact (`DataServer/internal/config/config.go:151`, `bootstrap_assets.go`) | Chiave HMAC (hex ≥32 byte) | **Obbligatoria in production** (`config.go:156`); NON è nel template j2 → iniettata manualmente in `/etc/velox-server.env` |
| `VELOX_GRPC_TLS_CERT_FILE/KEY_FILE/CA_FILE` | gRPC control plane (TLS opzionale) | Percorsi certificati | commentate nel template: `/etc/velox/certs/server.crt`, `server.key`, `ca.crt` |
| `VELOX_TLS_CERT_FILE/KEY_FILE` | REST TLS (opzionale) | Percorsi certificati | commentate |
| `VELOX_REMOTE_ENGINE_URL/TOKEN` | remote engine (`config_integrations.go:72`) | Token statico (opzionale) | commentate |
| `VELOX_SECRETS_DIR` | `/etc/velox/secrets` — root secret runtime | Percorso | TLS keys, OAuth creds, vault refs |

### 2.3 Master — directory secret su disco

| Percorso | Referente | Tipo |
|---|---|---|
| `/opt/velox/secrets/admin-token` | `fleetctl` (source #3), `scripts/ops/align-worker-digest.sh`, `scripts/ops/runtime-cert.sh` | Token admin (file) |
| `/etc/velox/secrets/` | master runtime (`VELOX_SECRETS_DIR`) | TLS keys, OAuth, refs |
| `DataServer/data/drive/credentials/credentials.json` | `DataServer/internal/integrations/drive/auth.go` (OAuth Desktop) | OAuth client_secret (gitignored, vivo su master) |
| `DataServer/data/drive/tokens/` | integrazione Drive | OAuth tokens (gitignored) |
| `DataServer/data/secrets/` | storico (`youtube/tokens/` legacy) | token legacy (gitignored) |

> Guardia CI: `scripts/ci/check-secrets.sh` scansiona il tree committato (chiavi
> private, `ghp_`/`github_pat_`, AKIA/ASIA, Stripe, AIza, `GOCSPX-`) e
> `scripts/ci/operator-history-scrub.sh` copre i percorsi storici
> (`VeloxEditing/refactored/DataServer/client_secret*.json`, `data/drive/tokens/*`,
> `data/secrets/youtube/tokens/*`).

### 2.4 Worker runtime — per VPS

Percorsi canonici installati da `deploy/runtime/prepare-host.sh` (+ verifica in
`deploy/runtime/sections_5_to_9.sh` sez. 7-8; documentati in `deploy/runtime/README.md`).

| Percorso (host) | Mount (container) | Permessi | Referente | Tipo |
|---|---|---|---|---|
| `/etc/velox-worker/secrets/worker_credential` | `/run/velox/secrets/worker_credential` (`VELOX_WORKER_CREDENTIAL_FILE`) | 0600, uid 10001 | worker agent (credential legacy/file-based) | Credential per-worker |
| `/etc/velox-worker/certs/worker.crt` | `/run/velox/certs/worker.crt` | 0644 root | mTLS gRPC (CN DEVE = `worker_id`, RW-PROD-001 A9) | Certificato client |
| `/etc/velox-worker/certs/worker.key` | `/run/velox/certs/worker.key` | 0600 uid 10001 | mTLS gRPC | Chiave privata |
| `/etc/velox-worker/certs/ca.crt` | `/run/velox/certs/ca.crt` | 0644 root | trust chain mTLS | Certificato CA |
| `/etc/velox-worker/worker.env` | env_file del container | 0600 root:root | `VELOX_WORKER_SECRET` + config | Env con secret |
| `/var/lib/velox-worker/worker_config.json` | `/opt/velox/worker_config.json:ro` | 0640 uid 10001 | solo percorsi TLS (nessun secret) | Config |

**Due flussi di credential worker coesistono:**
1. *File-based (legacy):* `/etc/velox-worker/secrets/worker_credential` → mount ro.
2. *Env-based (canonical):* `VELOX_WORKER_SECRET` in `/etc/velox-worker/worker.env`,
   scritto da `canonical_worker_runtime.yml` (`no_log`), vince sul secret preservato
   dal legacy; gate pre-restart che blocca il deploy se il master ha già una
   credential salvata per il worker e l'env non la porta.

Entrambi confluiscono in `credential_hash` (SHA-256 di `worker_id:secret`) nella
tabella SQLite `worker_credentials` del master (migration `020_worker_control_plane.sql`,
`DataServer/internal/store/store_worker_credentials.go`).

### 2.5 SSH

| Elemento | Dove | Referente | Tipo |
|---|---|---|---|
| Chiave privata operatore | `~/.ssh/velox` (o `~/.ssh/id_ed25519_velox`, vedi fixture test) | `bootstrap-ssh.yml`, inventory `ansible_ssh_private_key_file` | Chiave privata SSH |
| Chiave pubblica operatore (fallback transizione) | `vault_velox_operator_pubkey` → `/home/velox-deploy/.ssh/authorized_keys` | accesso SSH ai nodi | Chiave pubblica |
| **CA SSH OpenBao (privata)** | **solo dentro OpenBao** (secrets engine `ssh`, config/ca) — `deploy/openbao/scripts/provision-ssh-ca.sh` la genera una volta, MAI esportata | firma certificati operatore | Chiave privata CA (critica) |
| **CA SSH OpenBao (pubblica)** | `.velox/openbao/ssh-ca.pub` → `/etc/ssh/trusted-user-ca-keys.pem` via `bootstrap-ssh.yml` (`vault_velox_ssh_ca_pubkey`), `TrustedUserCAKeys` | tutti i nodi (verifica cert) | Chiave pubblica CA |
| **Certificato operatore firmato** | `sign-operator-ssh.sh` → `~/.ssh/<key>-cert.pub`; TTL **breve** (default 30m), principals `velox-admin`/`velox-deploy`; scade da solo (niente revoche manuali) | accesso SSH ai nodi | Certificato SSH (transitorio) |
| Hardening sshd | `PasswordAuthentication no`, `PermitRootLogin no`, `PubkeyAuthentication yes` (`bootstrap-ssh.yml`) | tutti i nodi | Policy |
| Sudo | passwordless via `velox-deploy` (no password) | playbook deploy | Policy |
| SecretResolver SSH | `DataServer/internal/handlers/remote/ansible/secrets.go` — file `ssh_host_<host>` (0600) sotto il secrets dir; la tabella `ansible_hosts` (SQLite) salva solo `secret_ref` (`file:ssh_host_<host>`), mai la password in chiaro | inventory dinamico Ansible (`manager_computers.go`) | Password SSH (transitorie) |

> Dismissione: le `authorized_keys` statiche e le password SSH restano come
> fallback/transitorie finché TUTTI gli operatori usano i certificati OpenBao;
> poi si rimuove il task authorized_keys di `bootstrap-ssh.yml` (vedi
> `docs/openbao-ssh-ca.md` §Dismissione).

### 2.6 PKI / certificati mTLS (3 livelli)

Documentazione: `docs/operations/PR-6-pki-rotation-runbook.md`, `docs/roadmap/13-mtls.md`.

| Elemento | Dove | Formato | Stato |
|---|---|---|---|
| Root CA key | dispositivo **air-gapped**, 2 copie in cassaforte | PEM | chiave privata — mai online |
| Root CA cert | committato in repo (`deploy/certs/`) | PEM | pubblico |
| Intermediate CA key | `/opt/velox/certs/intermediate/ca.key` sul master | PEM cifrata a riposo (ansible-vault) | online |
| Server cert master | `/opt/velox/certs/master/server.crt` + `.key` | PEM | leaf 30-90gg |
| Worker leaf cert | `/opt/velox/certs/workers/worker-<id>.{crt,key}` → distribuito via Ansible | PEM | leaf 7-30gg, CN=worker_id |
| Cert dev/CI | `scripts/gen-worker-certs.sh` (10 anni, self-signed); `tests/e2e/grpc-control-plane/certs/generate-dev-pki.sh` (7gg CA/1gg leaf) | PEM | solo test |
| Rotazione | cron `cert-rotation.sh` + `monitor-expiry.sh` (soglie 14/7/2 gg) | script | **TODO**: `cert-rotation.sh` e `alert-cert-expiry.sh` non ancora versionati |

### 2.7 Credential vault Go (lease/revoca) + OAuth Drive

| Componente | Dove | Referente | Tipo |
|---|---|---|---|
| `credentials.Vault` | `DataServer/internal/credentials/vault.go` — `Put/IssueAccessLease/Revoke/Rotate`, `Material{AccessToken,RefreshToken,ClientSecret}`, lease TTL 15 min | Delivery runner (`deliveries/runner.go` → `runner_process.go`), destinazioni publish | Credenziali dinamiche con lease |
| M2M admin keys | `/api/v1/admin/m2m/keys` (`admin_m2m_keys.go`, tabella `m2m_keys`) — `plaintext_secret` mostrato **una volta**; il master conserva solo SHA-256 | smoke/benchmark/CI (client effimeri) | Credenziali dinamiche (rotazione on-demand) |
| OAuth Drive | `DataServer/data/drive/credentials/credentials.json` + `tokens/` (gitignored) | `integrations/drive/auth.go` | OAuth client_secret + token |

### 2.8 CI / GitHub Actions

| Secret/var | Workflow | Referente |
|---|---|---|
| `secrets.ANSIBLE_VAULT_PASSWORD` | `deploy.yml` (→ `/tmp/vault-pass` 0600, `--vault-password-file`) | decifratura ansible-vault in CI |
| `secrets.VELOX_ADMIN_TOKEN` | `deploy.yml`, `nightly-jobs-smoke.yml` | health check + smoke su master prod |
| `secrets.GITHUB_TOKEN` | `worker-image.yml`, `master-image.yml`, `deploy.yml` | `docker/login-action` GHCR |
| `vars.VELOX_MASTER_URL` | `deploy.yml`, `nightly-jobs-smoke.yml` | URL master (non secret) |
| GitHub Environment `production` | — | contenitore di tutti i valori sopra |

### 2.9 Operatore locale (workstation)

| Elemento | Dove | Note |
|---|---|---|
| `.velox/production.env` | `.velox/` (gitignored, `chmod 600`) | `VELOX_MASTER_URL`, `VELOX_ADMIN_TOKEN`, GHCR repo, `ANSIBLE_VAULT_PASSWORD` opzionale; unico lettore: `scripts/operator/with-production-env.sh` |
| `~/.vault-velox-pass` | home operatore (0600) | password ansible-vault |
| `.velox-passwords.txt` | gitignored | store credenziali operatore (legacy) |

### 2.10 Test / staging (NON produzione — solo placeholder)

| File | Contenuto |
|---|---|
| `tests/e2e/staging-acceptance/staging.env.example` | `VELOX_STAGING_ADMIN_TOKEN`, `VELOX_WORKER_SECRET_A/B` (CHANGE_ME) |
| `scripts/cert/remote-worker-cert.env.example` | `VELOX_ADMIN_TOKEN`, `VELOX_M2M_TOKEN` (CHANGE_ME) |
| `scripts/ci/golden-e2e.sh`, `tests/e2e/workload/run.sh` | `VELOX_COMMIT_HMAC_KEY` di test (`0011…`), token e2e fissi (`e2e-test-admin-token`, `velox-dev-token`) |
| `scripts/pilot.sh`, `ops/jobs/*.sh` | `ADMIN_TOKEN=velox-dev-token` (solo dev) |

---

## 3. Matrice riepilogativa (unica tabella)

| # | Secret | Percorso / origine | Formato | Referente | Tipo | Criticità |
|---|---|---|---|---|---|---|
| 1 | `VELOX_ADMIN_TOKEN` | ansible vault → `/etc/velox-server.env`; `/opt/velox/secrets/admin-token`; CI secret | token | master REST, fleet-* playbook, fleetctl, operator | token statico | **alta** (god-mode REST) |
| 2 | `INSTAEDIT_CONTROL_JWT_SECRET` | vault → `/etc/velox-server.env` | HS256 ≥32B | verifier InstaEdit (accoppiato BFF) | secret condiviso | alta |
| 3 | `SOCIAL_API_TOKEN` | vault → `/etc/velox-server.env` | bearer | socialclient | token statico | alta |
| 4 | `SOCIAL_WEBHOOK_SECRET` | vault → env (non ancora consumato) | HMAC | futuro callback social | secret HMAC | media (futuro) |
| 5 | `VELOX_COMMIT_HMAC_KEY` | `/etc/velox-server.env` (manuale, non j2) | hex ≥32B | completion coordinator | chiave HMAC | alta |
| 6 | `registry_username/token` | vault.yml.example | credenziali | nessun consumatore tracciato | credenziali registry | media (orfane) |
| 7 | `VELOX_WORKER_SECRET` | vault → `worker_secret` → `/etc/velox-worker-<host>.env` | stringa | worker agent → `credential_hash` SQLite master | credential per-worker | alta (per-worker) |
| 8 | `worker_credential` (file) | `/etc/velox-worker/secrets/worker_credential` | file 0600 | worker agent (legacy) | credential per-worker | alta (per-worker) |
| 9 | `worker.key` | `/etc/velox-worker/certs/worker.key` | PEM 0600 | mTLS gRPC | chiave privata | alta (per-worker) |
| 10 | `worker.crt` / `ca.crt` | `/etc/velox-worker/certs/` | PEM | mTLS chain | certificati | media |
| 11 | root/intermediate CA key | air-gapped / `/opt/velox/certs/intermediate/` | PEM cifrata | PKI 3 livelli | chiave privata CA | **critica** |
| 12 | server.key master | `/opt/velox/certs/master/server.key` | PEM | gRPC/REST TLS | chiave privata | alta |
| 13 | chiave privata SSH operatore | `~/.ssh/velox` | chiave ed25519/rsa | accesso nodi | chiave privata SSH | alta |
| 14 | pubkey operatore | vault → `authorized_keys` (fallback transizione) | pubkey | bootstrap-ssh | identità | bassa |
| 14b | **CA SSH OpenBao (privata)** | dentro OpenBao (`ssh/config/ca`) — mai esportata | chiave privata CA | firma cert operatore | critica | critica |
| 14c | **cert operatore firmato** | `~/.ssh/<key>-cert.pub` (TTL ≤30m default) | cert SSH | accesso nodi | transitorio | bassa (scade) |
| 15 | password SSH host (transitorie) | SecretResolver → `ssh_host_<host>` (0600); `secret_ref` in SQLite | file | inventory dinamico Ansible | password | media |
| 16 | OAuth Drive client_secret/token | `DataServer/data/drive/credentials+tokens` (gitignored) | JSON | integrazione Drive | OAuth | alta |
| 17 | credenziali publish con lease | `credentials.Vault` (SQLite cifrata, lease 15min) | materiale OAuth | delivery runner | dinamico con lease | media (già gestito) |
| 18 | M2M admin keys | `/api/v1/admin/m2m/keys` (SHA-256 in DB) | client_id+secret | smoke/CI/operator | dinamico | media |
| 19 | `ANSIBLE_VAULT_PASSWORD` | `~/.vault-velox-pass`; CI secret | password | decifratura vault.yml | password vault | alta |
| 20 | `.velox/production.env` | workstation (600) | env | operator scripts | aggregato | alta |

---

## 4. Osservazioni e gap (baseline per la migrazione OpenBao)

1. **Ansible Vault è oggi la fonte primaria** di secret statici (token, JWT, worker
   creds) e viene già descritto nel template come "MUST be sourced from a secrets
   manager (1Password, Bitwarden, Vault, etc.)" — la direzione OpenBao è allineata.
2. **Già presenti i mattoni per il resolver:** `SecretResolver`
   (`DataServer/internal/handlers/remote/ansible/secrets.go`, scheme `file:`/`env:`)
   e `credentials.Vault` (lease/revoca/rotazione) — riusabili per un backend OpenBao
   senza cambiare il runtime.
3. **Sudo password già eliminata** (passwordless `velox-deploy`) e
   `vault_velox_sudo_password` rimosso — un obiettivo del piano è già raggiunto.
4. **Le password SSH statiche sono già transitorie** (SecretResolver li scrive come
   file 0600 e conserva solo `secret_ref`); nessuna password in SQLite.
5. **Gap individuati:**
   - `vault_velox_registry_*` dichiarate ma senza consumatore tracciato → decidere
     se migrarle o rimuoverle.
   - `VELOX_COMMIT_HMAC_KEY` non passa dal template j2 (iniezione manuale) → va
     gestita esplicitamente in OpenBao.
   - Script di rotazione PKI (`cert-rotation.sh`, `alert-cert-expiry.sh`) non
     versionati → da creare in parallelo alla PKI OpenBao.
   - Doppio flusso credential worker (file `worker_credential` + env
     `VELOX_WORKER_SECRET`) → consolidare a un solo percorso durante la migrazione.
6. **Riferimenti per le fasi successive:** `docs/operations/PR-6-pki-rotation-runbook.md`,
   `docs/roadmap/13-mtls.md`, `docs/SECURITY_RUNBOOK.md`, `docs/architecture/AGENT-CONTRACT.md`,
   `scripts/ci/check-secrets.sh`.
