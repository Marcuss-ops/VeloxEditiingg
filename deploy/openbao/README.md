# deploy/openbao — Secrets Manager centrale Velox (OpenBao)

Componente di infrastruttura che ospita il **secrets manager centrale** di
Velox. Obiettivo della migrazione (vedi `docs/secrets-audit.md`): eliminare le
password statiche come concetto operativo — KV per i secret statici, machine
identity per-worker (AppRole), policy, SSH CA, PKI mTLS e credenziali DB
dinamiche.

> ⚠️ **Regole non negoziabili**
> - Le **unseal keys** e il **root token** vengono scritti SOLO sotto il
>   state-dir gitignored `<repo>/.velox/openbao/` con mode 0600. **MAI nel repo**.
> - La porta 8200 è esposta **solo su loopback** (127.0.0.1).
> - Il listener usa **TLS** (self-signed, tls12+) — nessun traffico in chiaro.
> - OpenBao 2.x gira come utente non-root `openbao` e **non richiede mlock**:
>   il vecchio parametro `disable_mlock` non esiste più, non aggiungerlo.

---

## Struttura del componente

```text
deploy/openbao/
├── compose.yml            # servizio docker (loopback 8200, TLS, healthcheck)
├── config/bao.hcl         # config server: raft (/openbao/file) + listener TLS
├── .env.example           # template config non-segreta (→ .env, gitignored)
├── .gitignore             # guardia extra: mai committare chiavi/token/stato
├── policies/              # policy HCL: master, admin, ssh-operator, worker.hcl.tmpl
├── scripts/
│   ├── gen-tls.sh               # certificato TLS listener (state-dir, 0600/0640)
│   ├── bootstrap-init.sh        # init Shamir: unseal keys + root token (0600)
│   ├── bootstrap-unseal.sh      # unseal idempotente con le keys salvate
│   ├── status.sh                # stato seal/HA + cross-check health
│   ├── provision-kv.sh          # KV v2 velox/ (idempotente, REST, valori da env/stdin)
│   ├── verify-kv.sh             # albero KV con versioni (mai valori)
│   ├── provision-policies.sh    # policy ACL da policies/ (idempotente, change-detection)
│   ├── provision-approle.sh     # AppRole per principal + role-id/secret-id (0600)
│   ├── verify-approle.sh        # login reale + check autorizzazioni positivi/negativi
│   ├── migrate-master-tokens.sh # env → KV (una tantum, --force, fail-closed)
│   ├── resolve-master-tokens.sh # KV → extra-vars Ansible vault_velox_* (0600)
│   ├── provision-ssh-ca.sh      # engine ssh + CA (mai rigenerata) + role velox-operator
│   ├── sign-operator-ssh.sh     # firma cert operatore (TTL breve, principals limitati)
│   └── verify-ssh-ca.sh         # 11 check: engine, CA, role, firma di prova, negativo
└── README.md
```

Dettagli layout dati: lo storage Raft vive su un **named volume** montato a
`/openbao/file` — l'unica directory dati presente nell'immagine openbao
(proprietà utente `openbao` uid 100). Montare il volume su `/openbao/data`
(non esistente nell'immagine) creerebbe una directory root-owned e il server
crash-erebbe con `permission denied` su `vault.db`. L'entrypoint dell'immagine
inietta già `-config=/openbao/config`, quindi non si passa mai `-config`
esplicito nel comando.

## 1. Prerequisiti

| Tool | Note |
|---|---|
| Docker + compose plugin | già richiesto dai worker Velox |
| `bao` CLI | scaricalo dagli asset di release [openbao/openbao](https://github.com/openbao/openbao/releases) (es. `openbao_2.6.1_linux_amd64.tar.gz`) e mettilo su `PATH`:
|   | `curl -fsSL -o /tmp/openbao.tgz https://github.com/openbao/openbao/releases/download/v2.6.1/openbao_2.6.1_linux_amd64.tar.gz` |
|   | `tar -xzf /tmp/openbao.tgz -C ~/.local/bin bao && chmod +x ~/.local/bin/bao` |
| `openssl` | per la generazione del certificato TLS |
| `jq` | usato dagli script di bootstrap |

Verifica: `bao version`, `docker compose version`.

## 2. Configurazione

```bash
cd deploy/openbao
cp .env.example .env        # modifica solo se serve (versione immagine, porta host)
```

Il file `.env` è gitignored; contiene solo configurazione non-segreta.

## 3. Genera il certificato TLS del listener

```bash
./scripts/gen-tls.sh
```

> ⚠️ **Esegui questo script PRIMA di `docker compose up -d`.** Se compose parte
> per primo, Docker crea la directory del bind mount mancante come **root** e
> lo script (e openssl) falliscono con `Permission denied`. In quel caso:
> `docker compose down && sudo rm -rf .velox/openbao/tls && ./scripts/gen-tls.sh`

Scrive `server.crt` (0644) + `server.key` in `.velox/openbao/tls/` (gitignored).
Il container gira non-root (`openbao`, uid 100/gid 1000) e l'entrypoint NON
chowna `/openbao/tls`, quindi la chiave viene resa leggibile dal container con
`chgrp 1000` + mode 0640. Lo script **fallisce chiuso** se non puoi fare
`chgrp 1000` (es. non sei membro del gruppo 1000): in quel caso esegui
`sudo chgrp 1000 .velox/openbao/tls/server.key && sudo chmod 0640 ...` e
rilancia. Solo per dev/bootstrap esiste l'escape hatch esplicito
`OPENBAO_ALLOW_INSECURE_KEY_MODE=1` (chiave 0644 — mai in production).
I SAN di default coprono `localhost` e `127.0.0.1`. Quando OpenBao dovrà essere
raggiunto da altri host (master/worker remoti), rigenera con:

```bash
OPENBAO_TLS_CN=openbao.example.com \
OPENBAO_TLS_SANS='DNS:openbao.example.com,IP:10.0.0.10' \
./scripts/gen-tls.sh   # dopo aver cancellato la coppia esistente
```

e apri la porta con `OPENBAO_HOST_PORT` nel `.env` (sempre dietro valutazione
di sicurezza / tunnel).

## 4. Avvio

```bash
cd deploy/openbao
docker compose up -d
docker compose ps          # velox-openbao deve risultare running
docker compose logs -f openbao   # segue i log di avvio
```

> Nota: finché il nodo non è inizializzato e unsealato, lo healthcheck di
> compose resta `unhealthy` — è **atteso** (vedi §7).
>
> Se vedi `error initializing storage of type raft ... permission denied`:
> hai un volume vecchio creato su `/openbao/data` (pre-fix). Risolvi con
> `docker compose down -v` (perde dati) e riavvia.

## 5. Inizializzazione (una tantum)

```bash
./scripts/bootstrap-init.sh
```

Cosa fa:
- verifica che il server sia raggiungibile e **non ancora inizializzato**;
- genera `SHARES=5` unseal keys con `THRESHOLD=3` (override:
  `OPENBAO_KEY_SHARES` / `OPENBAO_KEY_THRESHOLD`);
- scrive `unseal-keys.txt` e `root-token` in `.velox/openbao/` (0600, dir 0700);
- **rifiuta di sovrascrivere** file esistenti.

## 6. Unseal

```bash
./scripts/bootstrap-unseal.sh
```

Applica le keys una alla volta fino a `Sealed: false`. È idempotente: da
rieseguire dopo ogni reboot/restart del container (da aggiungere in futuro a
un timer o a un auto-unseal KMS).

## 7. Verifica

```bash
./scripts/status.sh
```

Output atteso:
- `Sealed: false` e stato `active`;
- `GET https://127.0.0.1:8200/v1/sys/health -> HTTP 200 (ACTIVE + unsealed)`.

Significato dei codici health: **200** active · **429** standby · **501** non
inizializzato · **503** sealed.

Con lo stato raggiunto, `docker compose ps` passa a `healthy`.

## 8. KV store — gerarchia e provisioning dei secret statici

Mount KV **v2** (con versioning) su `velox/`, gerarchia allineata a
`docs/secrets-audit.md`:

```text
velox/ (KV v2)
└── production/
    ├── master/
    │   ├── admin-token
    │   ├── instaedit-control-jwt-secret
    │   ├── social-api-token
    │   ├── social-webhook-secret        (opzionale)
    │   └── commit-hmac-key              (opzionale)
    ├── workers/
    │   └── <worker-id>/credential
    └── services/
        └── registry/
            ├── username                 (opzionale)
            └── token                    (opzionale)
```

Ogni foglia contiene un singolo campo `value`. I secret obbligatori sono
`admin-token`, `instaedit-control-jwt-secret`, `social-api-token` e
`workers/<id>/credential`; gli altri sono opzionali (vengono saltati se non
forniti).

### Provisioning idempotente

```bash
# valori via env (precedenza massima)
OPENBAO_VALUE_ADMIN_TOKEN=... \
OPENBAO_VALUE_INSTAEDIT_JWT=... \
OPENBAO_VALUE_SOCIAL_API_TOKEN=... \
./scripts/provision-kv.sh

# credential di un worker
OPENBAO_VALUE_WORKER_CREDENTIAL=... ./scripts/provision-kv.sh --worker host_57_129_132_133

# valori da file gitignored 0600 (.velox/openbao/values.env, formato NOME=valore)
OPENBAO_VALUES_FILE=.velox/openbao/values.env ./scripts/provision-kv.sh

# sovrascrittura (nuova versione) · simulazione · verifica
./scripts/provision-kv.sh --force
./scripts/provision-kv.sh --dry-run
./scripts/verify-kv.sh
```

### Regole di sicurezza

- I valori **non provengono MAI da file committati**: env → file gitignored
  `0600` non tracciato (rifiutato altrimenti, con check `git ls-files` su path
  assoluto via `realpath`) → prompt interattivo `read -s`.
- Il file valori (`.velox/openbao/values.env`) è in formato `NOME=valore`,
  **una riga per secret, LF** (niente CRLF — il CR viene comunque strippato).
  ⚠️ **Niente valori multi-line qui**: i PEM dei cert worker
  (`WORKER_CERT/KEY/CA`) vanno passati SOLO via env
  (`OPENBAO_VALUE_WORKER_CERT="$(cat worker.crt)"` …), mai nel values-file
  (troncherebbe il PEM alla prima riga → chiave corrotta).
- I secret già presenti vengono **saltati** (idempotenza); `--force` crea una
  nuova versione KV.
- La scrittura avviene via **REST API** (`/v1/velox/data/...`) con il valore
  passato a `jq` dall'ambiente e il body da stdin — **mai in argv**
  (world-readable via `/proc/<pid>/cmdline`) né in file committati.
- `verify-kv.sh` stampa solo **path + numero di versione**, mai i valori.
- Fase attuale: il provisioning KV usa ancora il root token di bootstrap
  (serve WRITE su `velox/*`, che la policy `admin` copre): al wiring degli
  AppRole negli strumenti operativi si passerà al token AppRole `admin`
  (vedi §9 e `docs/openbao-identity-matrix.md`).

## 9. AppRole — machine identity per principal

Fase 4: una **identità AppRole distinta** per ogni worker + `master` + `admin`,
con policy least-privilege. Matrice completa: `docs/openbao-identity-matrix.md`.

```bash
./scripts/provision-policies.sh   # scrive master/admin/worker-<id> (idempotente)
./scripts/provision-approle.sh    # abilita approle, crea i role, salva role-id+secret-id
./scripts/verify-approle.sh       # login reale + check positivi/negativi
```

- **Policy** (`policies/`): `master.hcl` (read `master/*` + `workers/*` +
  `services/registry/*` — per i token migrati), `admin.hcl` (CRUD `velox/*` +
  `auth/approle/*` + `sys/policies/acl/*`), `worker.hcl.tmpl` → `worker-<id>`
  (read SOLO del proprio ramo — credential + cert mTLS sotto
  `velox/production/workers/<id>/*`).
- **Materiale**: `role-id` + `secret-id` in
  `.velox/openbao/approle/<principal>/{role-id,secret-id}` (0600, gitignored) —
  MAI in repo, MAI stampati da script.
- **Idempotenza**: la seconda run non tocca nulla; `--force` ruota i secret-id
  (i vecchi decadono → rollout in due fasi).
- **Verifica** (`verify-approle.sh`): login reale + capabilities via REST
  `/v1/sys/capabilities` (il CLI `bao` non ha il comando `capabilities`) —
  worker A NON legge il ramo di worker B né `master/*`; master legge ma non
  scrive; admin scrive. Check **fail-closed**: un errore di verifica fa
  fallire il check (mai pass vacui sui negativi). Esce 1 al primo FAIL.
- `--workers "id1 id2"` **sostituisce** la fleet di default (come
  `provision-policies.sh`); `master` e `admin` vengono sempre inclusi a meno
  di `--principal` esplicito.
- TTL custom: `OPENBAO_TOKEN_TTL` (1h), `OPENBAO_TOKEN_MAX_TTL` (24h),
  `OPENBAO_SECRET_ID_TTL` (0 = mai), `OPENBAO_SECRET_ID_NUM_USES` (0).

## 10. Migrazione dei token del Master (fase 6)

`VELOX_ADMIN_TOKEN`, `INSTAEDIT_CONTROL_JWT_SECRET`, `SOCIAL_API_TOKEN`,
`VELOX_COMMIT_HMAC_KEY` e le credenziali registry vengono spostati da
`vault.yml` a `velox/production/master/*` e `velox/production/services/registry/*`
in OpenBao. La policy `master` (read-only, §9) copre questi path — il deploy
risolve i token al momento del rilascio, senza credenziali statiche in repo.

### Migrazione una tantum (env → KV)

```bash
# da /etc/velox-server.env (default) o con --env-file; MAI stampa valori
./scripts/migrate-master-tokens.sh
./scripts/migrate-master-tokens.sh --env-file ./server.env --dry-run
./scripts/migrate-master-tokens.sh --registry-username u --registry-token t
```

- Precedenza: `OPENBAO_VALUE_*` (env) > `--env-file` > `/etc/velox-server.env`.
- **Fail-closed**: un required (admin / instaedit / social) assente → exit 1,
  nessuna migrazione.
- Usa `provision-kv.sh --force` (nuova versione KV); verifica con
  `./scripts/verify-kv.sh`.

### Risoluzione al deploy (KV → extra-vars Ansible)

```bash
OPENBAO_ADDR=https://127.0.0.1:8200 \
OPENBAO_ROLE_ID_FILE=/etc/velox/master/approle/role-id \
OPENBAO_SECRET_ID_FILE=/etc/velox/master/approle/secret-id \
OPENBAO_VARS_FILE=/tmp/openbao-vars.yml \
./scripts/resolve-master-tokens.sh [--require-all]
```

Scrive le **extra-vars** `vault_velox_*` (stessi nomi di `vault.yml`, quindi
`deploy/templates/velox-server.env.j2` non cambia) in un file **0600** da
passare ad ansible-playbook con `-e @/tmp/openbao-vars.yml`. Gli extra-vars
VINCONO su group_vars → i token vengono da OpenBao; senza `OPENBAO_ADDR`
(exit 0) o senza file, ansible usa `vault.yml` come oggi (fallback legacy).

- KV v2 → lettura via REST `/v1/velox/data/<path>` con il token AppRole.
- Opzionali (`social-webhook-secret`, `registry/*`) saltati se assenti; con
  `--require-all` un required assente (admin/instaedit/social) blocca il deploy
  (exit 1) invece di deployare con token mancanti.
- **Mai stampa valori**: logga solo i nomi; quoting YAML via `jq -Rs .`
  (NB: `$v | tojson` senza `-r` verrebbe ri-serializzato da jq ≥ 1.8).

### Integrazione CI (`.github/workflows/deploy.yml`)

Step opzionale `resolve-openbao-tokens` attivo solo con `OPENBAO_ADDR`
impostato: risolve il vars file e lo inietta con `-e @/tmp/openbao-vars.yml`
(extra-vars condizionali). Senza `OPENBAO_ADDR` il flusso ansible-vault resta
**identico** — il deploy non si rompe.

## 11. SSH Certificate Authority (fase 7)

Guida completa: `docs/openbao-ssh-ca.md`. Il secrets engine `ssh` di OpenBao
firma i certificati degli operatori (TTL **breve**, default 30m, principals
`velox-admin`/`velox-deploy`); i nodi fidano della CA con `TrustedUserCAKeys`
(`deploy/playbooks/bootstrap-ssh.yml`) — niente password SSH statiche, niente
`authorized_keys` per operatore nel lungo periodo.

```bash
./scripts/provision-ssh-ca.sh            # enable engine + CA (idempotente) + role + export pubkey
./scripts/sign-operator-ssh.sh --pubkey-file ~/.ssh/velox.pub      # cert 30m
./scripts/verify-ssh-ca.sh               # 11 check (firma di prova + negativo fail-closed)
```

- La **chiave privata della CA vive SOLO in OpenBao** (`ssh/config/ca`, generata
  una volta — mai sovrascritta: rigenerarla invaliderebbe ogni cert emesso).
- La **chiave pubblica** è esportata in `$STATE_DIR/ssh-ca.pub` (0644) e
  distribuita dai nodi come `/etc/ssh/trusted-user-ca-keys.pem`.
- Policy: `admin.hcl` copre `ssh/*` (gestione CA); `ssh-operator.hcl` è la
  policy di SOLA firma (`update` su `ssh/sign/*`, `read` su `ssh/config/ca` +
  `ssh/roles/*`) — AppRole dedicato con `./scripts/provision-approle.sh
  --principal ssh-operator` e verifica in `verify-approle.sh`.
- `scripts/ci/test-openbao-ssh-ca.sh`: sintassi + check strutturali (sempre) +
  smoke live completo (se OpenBao è raggiungibile).

## 12. Backup delle chiavi (FATTO SUBITO)

```bash
# fuori dal repo:
cp .velox/openbao/unseal-keys.txt /secure/velox-openbao-unseal-keys.txt
cp .velox/openbao/root-token      /secure/velox-openbao-root-token
chmod 600 /secure/velox-openbao-*
```

Conservare **offline** (password manager / cassaforte): senza keys + root
token, i secret in OpenBao sono irrecuperabili. Dopo il backup offline puoi
eliminare i file locali (il bootstrap li rigenera solo con un re-init, che
distrugge i dati).

## 13. Operazioni quotidiane

| Operazione | Comando |
|---|---|
| Stato | `./scripts/status.sh` |
| Re-unseal dopo reboot | `./scripts/bootstrap-unseal.sh` |
| Log | `docker compose -f deploy/openbao/compose.yml logs -f openbao` |
| Backup dati (raft) | `docker volume inspect velox-openbao_openbao-data` → snapshot del volume (es. `docker run --rm -v velox-openbao_openbao-data:/data -v $PWD:/backup alpine tar czf /backup/openbao-data.tgz -C /data .`) |
| Upgrade | aggiorna `OPENBAO_VERSION` in `.env`, poi `docker compose pull && docker compose up -d` |
| Interfaccia UI | https://127.0.0.1:8200/ui (richiede token; cert self-signed → accetta il warning) |

## 14. Prossimi passi della migrazione

1. ~~**KV store**~~ ✅ implementato (`deploy/openbao/scripts/provision-kv.sh`,
   gerarchia in §8 sopra).
2. ~~**AppRole per-worker** + policy per-worker (least privilege)~~ ✅ implementato
   (§9 + `docs/openbao-identity-matrix.md`).
3. ✅ ~~Migrazione `worker_credential` + mTLS in OpenBao~~ — bootstrap worker:
   `deploy/runtime/openbao-fetch-worker-secrets.sh` (login AppRole, fetch
   credential + cert dal KV, scrittura nei path canonici, `--check`),
   integrato in `prepare-host.sh` con fallback sui file esistenti; test in
   `scripts/ci/test-openbao-worker-secrets.sh`. I secret dei cert vivono in
   `velox/production/workers/<id>/cert/{cert,key,ca}` (opzionali).
4. Distribuire `role-id`/`secret-id` sui nodi worker/master
   (`/etc/velox-worker/secrets/approle/`, 0600, Ansible `no_log`).
5. ✅ ~~Migrazione dei token master~~ — `migrate-master-tokens.sh` (env → KV,
   una tantum) + `resolve-master-tokens.sh` (KV → extra-vars Ansible, al deploy),
   integrati in `.github/workflows/deploy.yml` come step opzionale (§10).
6. ✅ ~~SSH CA~~ — secrets engine `ssh` + CA + role `velox-operator` +
   firma operatore a TTL breve (§11 + `docs/openbao-ssh-ca.md`); da completare
   con la dismissione delle `authorized_keys` statiche una volta che tutti gli
   operatori usano i cert.
7. PKI mTLS (leaf per-worker dal secrets engine `pki`), credenziali DB
   dinamiche, `SecretResolver` in Go.

## 15. Riferimenti

- OpenBao docs: https://openbao.org/docs/
- Config: `config/bao.hcl` (raft + listener TLS)
- Audit dei secret: `docs/secrets-audit.md`
- Playbook mTLS/rotazione esistenti: `docs/operations/PR-6-pki-rotation-runbook.md`, `docs/roadmap/13-mtls.md`
