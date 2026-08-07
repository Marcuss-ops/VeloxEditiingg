# Velox — Matrice Identità → Policy (OpenBao AppRole)

> **Stato:** operativo — allineato a `deploy/openbao/` (fase 4 della migrazione
> secrets, vedi `docs/secrets-audit.md` §4).
> **Scopo:** mappa univoca di chi è cosa in OpenBao: ogni macchina/ruolo ha una
> **machine identity AppRole distinta**, un **policy least-privilege** e un
> **percorso di accesso** ben definito. Nessun token condiviso, nessun secret in
> repo, nessun valore riportato qui.

---

## 1. Principi

1. **Una identità per macchina/ruolo**: ogni worker ha il SUO AppRole
   (`worker-<id>`), più `master` e `admin`. Nessun segreto condiviso tra macchine.
2. **Least privilege**: un worker legge SOLO il proprio ramo
   (`velox/production/workers/<id>/*`). Se compromesso, espone un worker, non la
   fleet.
3. **Materiale mai in repo**: `role-id` + `secret-id` vivono solo nello state-dir
   gitignored `<repo>/.velox/openbao/approle/<principal>/` (0600).
4. **Verifica end-to-end**: `verify-approle.sh` fa login reale e testa i permessi
   (positivi e negativi) ad ogni run.

## 2. Matrice

| Principal (AppRole role) | Policy OpenBao | Accesso KV (path) | Capabilities | Token TTL | Materiale | Consumatore |
|---|---|---|---|---|---|---|
| `worker-host_57_129_132_133` | `worker-host_57_129_132_133` | `velox/production/workers/host_57_129_132_133/*` | `read, list` | 1h (max 24h) | `.velox/openbao/approle/worker-host_57_129_132_133/{role-id,secret-id}` | worker VPS 57.129.132.133 |
| `worker-host_57_131_20_173` | `worker-host_57_131_20_173` | `velox/production/workers/host_57_131_20_173/*` | `read, list` | 1h (max 24h) | idem | worker VPS 57.131.20.173 |
| `worker-velox-worker-13197` | `worker-velox-worker-13197` | `velox/production/workers/velox-worker-13197/*` | `read, list` | 1h (max 24h) | idem | worker VPS 149.56.131.97 |
| `worker-velox-worker-523925eb` | `worker-velox-worker-523925eb` | `velox/production/workers/velox-worker-523925eb/*` | `read, list` | 1h (max 24h) | idem | worker VPS 51.222.204.158 |
| `master` | `master` | `velox/production/master/*` **+** `velox/production/workers/*` **+** `velox/production/services/registry/*` | `read, list` | 1h (max 24h) | `.velox/openbao/approle/master/{role-id,secret-id}` | deploy master (env, gRPC, fleet, registry pull) |
| `admin` | `admin` | `velox/*` + `auth/approle/*` + `sys/policies/acl/*` + `ssh/*` + `sys/health` | `create, read, update, delete, list` (velox/approle/policies/ssh); `read` (health) | 1h (max 24h) | `.velox/openbao/approle/admin/{role-id,secret-id}` | operatore (provisioning, rotazione, gestione CA SSH) |
| `ssh-operator` | `ssh-operator` | `ssh/sign/*` (update) + `ssh/config/ca` + `ssh/roles/*` (read) + `sys/health` | `update` (sign); `read` (ca/roles/health) | 1h (max 24h) | `.velox/openbao/approle/ssh-operator/{role-id,secret-id}` | unico percorso operativo per firma certificati SSH (fase 7) |

> Fleet canonica: `scripts/ops/align-worker-digest.sh` (righe 80-83) e
> `scripts/ops/runtime-cert.sh` (righe 46-49). Worker aggiuntivi si registrano con
> `OPENBAO_WORKERS="id1 id2" ./scripts/provision-policies.sh` +
> `./scripts/provision-approle.sh --workers "id1 id2"`. In entrambi gli script
> `--workers` **sostituisce** la fleet di default (i role/policy dei worker già
> registrati restano sul server, intatti); `provision-approle.sh` include sempre
> `master` e `admin` a meno di `--principal` esplicito. `ssh-operator` è la
> identità dedicata e l'unico percorso operativo per la firma dei certificati:
> va creata con `./scripts/provision-approle.sh --principal ssh-operator` e
> verificata con `./scripts/verify-approle.sh --principal ssh-operator`.

## 3. Gerarchia dei secret vista dalle identità

```text
velox/ (KV v2)
└── production/
    ├── master/          ← leggibile da: master, admin        (root)
    │   ├── admin-token
    │   ├── instaedit-control-jwt-secret
    │   ├── social-api-token
    │   ├── social-webhook-secret
    │   └── commit-hmac-key
    ├── workers/
    │   ├── host_57_129_132_133/   ← leggibile da: worker-host_57_129_132_133, master, admin
    │   │   └── credential         (+ futuro: cert mTLS nel proprio ramo)
    │   ├── host_57_131_20_173/
    │   ├── velox-worker-13197/
    │   └── velox-worker-523925eb/
    └── services/
        └── registry/     ← leggibile da: master, admin  (deploy master/worker image)
            ├── username
            └── token
```

**Isolamento worker**: `worker-A` NON può leggere `workers/B/*`, `master/*`,
`services/*` — verificato da `verify-approle.sh` (check negativi **fail-closed**:
se la verifica non riesce, il check fallisce — niente pass vacui).

## 4. Certificati (lettura del proprio certificato)

Il policy worker copre **tutto il proprio ramo**
(`velox/data/production/workers/<id>/*` + `velox/metadata/.../*`): quando i leaf
mTLS passeranno a OpenBao (fase PKI), la chiave/cert del worker vivrà nel proprio
ramo (`velox/production/workers/<id>/cert*`) e sarà già coperta — nessuna policy
aggiuntiva necessaria. Se in futuro la PKI userà un engine dedicato (`pki/`),
il template `worker.hcl.tmpl` andrà esteso con il path corrispondente.

## 5. Operazioni

| Operazione | Comando |
|---|---|
| Scrivere/aggiornare le policy | `./deploy/openbao/scripts/provision-policies.sh` |
| Registrare/ruotare le identità | `./deploy/openbao/scripts/provision-approle.sh` (`--force` = ruota i secret-id) |
| Verifica end-to-end | `./deploy/openbao/scripts/verify-approle.sh` (fail-closed: errori di verifica ⇒ FAIL) |
| Login manuale di test | `bao write auth/approle/login role_id=... secret_id=...` |
| TTL/limiti custom | `OPENBAO_TOKEN_TTL`, `OPENBAO_TOKEN_MAX_TTL`, `OPENBAO_SECRET_ID_TTL`, `OPENBAO_SECRET_ID_NUM_USES` |

> ⚠️ **Rotazione**: `--force` rigenera il `secret-id` — i vecchi secret-id (e i
> token emessi) decadono. Distribuire il nuovo materiale ai nodi PRIMA di
> revocare i vecchi (rollout in due fasi).

## 6. Prossimi passi (dipendenti da questa matrice)

1. ✅ ~~Distribuire `role-id`/`secret-id` ai nodi~~ — il materiale vive nello
   state-dir; su ogni host va copiato in `/etc/velox-worker/secrets/approle/`
   (0600, Ansible `no_log`).
2. ✅ ~~Migrazione `worker_credential` + mTLS in OpenBao~~ — risolta a livello
   di **host bootstrap** (NON del worker agent): `deploy/runtime/openbao-fetch-worker-secrets.sh`
   fa login AppRole per-worker e scrive `worker_credential` + coppia mTLS nei
   path canonici (permessi RW-PROD-001 A2), con **fallback sui file esistenti**
   finché la migrazione non è completa (`prepare-host.sh` §2.5). Verifica:
   `--check` (sha256 locale vs OpenBao) e `scripts/ci/test-openbao-worker-secrets.sh`.
3. Worker agent: sostituzione di `VELOX_WORKER_SECRET` come fonte di identità
   (login AppRole diretto nel processo — post-fase, quando OpenBao sarà
   raggiungibile dai nodi senza tunnel).
4. ✅ ~~Master env bootstrap da OpenBao~~ — `deploy/openbao/scripts/resolve-master-tokens.sh`
   (fase 6): login AppRole `master`, legge `master/*` + `services/registry/*` dal
   KV e scrive le extra-vars Ansible `vault_velox_*` in un file 0600 da passare
   con `-e @file` (vince su group_vars; senza il file resta il flusso
   ansible-vault legacy). `migrate-master-tokens.sh` fa la direzione inversa
   (env → KV, `--force`, fail-closed sui required, mai stampa valori).
   Integrato in `.github/workflows/deploy.yml` come step opzionale condizionale.
5. ✅ ~~SSH CA~~ (fase 7) — secrets engine `ssh` + CA + role `velox-operator`
   + identità di firma `ssh-operator` (opzionale); i nodi fidano con
   `TrustedUserCAKeys` via `deploy/playbooks/bootstrap-ssh.yml`
   (`vault_velox_ssh_ca_pubkey`). Docs: `docs/openbao-ssh-ca.md`.
6. `token_bound_cidrs` per-worker (limitare il login agli IP dei VPS) quando
   OpenBao sarà esposto oltre loopback.
7. PKI mTLS per-worker dal secrets engine `pki`, credenziali DB dinamiche,
   `SecretResolver` Go con backend OpenBao (`docs/secrets-audit.md` §4.2).
