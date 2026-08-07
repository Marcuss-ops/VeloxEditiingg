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
├── scripts/
│   ├── gen-tls.sh         # genera server.crt/server.key nel state-dir (0600/0640)
│   ├── bootstrap-init.sh  # init Shamir: unseal keys + root token (0600, gitignored)
│   ├── bootstrap-unseal.sh# unseal idempotente con le keys salvate
│   └── status.sh          # stato seal/HA + cross-check /v1/sys/health
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
- I secret già presenti vengono **saltati** (idempotenza); `--force` crea una
  nuova versione KV.
- La scrittura avviene via **REST API** (`/v1/velox/data/...`) con il valore
  passato a `jq` dall'ambiente e il body da stdin — **mai in argv**
  (world-readable via `/proc/<pid>/cmdline`) né in file committati.
- `verify-kv.sh` stampa solo **path + numero di versione**, mai i valori.
- Fase attuale: autenticazione con il root token di bootstrap; al passaggio
  ad AppRole (fase 4) i provisioning useranno token con policy ristrette.

## 9. Backup delle chiavi (FATTO SUBITO)

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

## 10. Operazioni quotidiane

| Operazione | Comando |
|---|---|
| Stato | `./scripts/status.sh` |
| Re-unseal dopo reboot | `./scripts/bootstrap-unseal.sh` |
| Log | `docker compose -f deploy/openbao/compose.yml logs -f openbao` |
| Backup dati (raft) | `docker volume inspect velox-openbao_openbao-data` → snapshot del volume (es. `docker run --rm -v velox-openbao_openbao-data:/data -v $PWD:/backup alpine tar czf /backup/openbao-data.tgz -C /data .`) |
| Upgrade | aggiorna `OPENBAO_VERSION` in `.env`, poi `docker compose pull && docker compose up -d` |
| Interfaccia UI | https://127.0.0.1:8200/ui (richiede token; cert self-signed → accetta il warning) |

## 11. Prossimi passi della migrazione

1. ~~**KV store**~~ ✅ implementato (`deploy/openbao/scripts/provision-kv.sh`,
   gerarchia in §8 sopra).
2. **AppRole per-worker** + policy per-worker (least privilege).
3. Migrazione di `worker_credential` / `VELOX_WORKER_SECRET` dentro OpenBao.
4. Migrazione dei token master (`VELOX_ADMIN_TOKEN`, `INSTAEDIT_CONTROL_JWT_SECRET`,
   `SOCIAL_API_TOKEN`, `VELOX_COMMIT_HMAC_KEY`).
5. SSH CA, PKI mTLS, credenziali DB dinamiche, `SecretResolver` in Go.

## 12. Riferimenti

- OpenBao docs: https://openbao.org/docs/
- Config: `config/bao.hcl` (raft + listener TLS)
- Audit dei secret: `docs/secrets-audit.md`
- Playbook mTLS/rotazione esistenti: `docs/operations/PR-6-pki-rotation-runbook.md`, `docs/roadmap/13-mtls.md`
