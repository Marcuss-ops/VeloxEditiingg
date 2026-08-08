# PR 6 — Production PKI Rotation Runbook

Status: **operativo**

Owner: infrastructure

## 1. Architettura a 3 livelli

```
root CA / issuer approvato (offline o secondo la policy del Master)
  │
  └── Master gRPC/REST server cert (issuer separato, se TLS Master è configurato)

OpenBao production intermediate (interno a OpenBao)
  │
  ├── worker-01 leaf cert (7-30 giorni)
  ├── worker-02 leaf cert (7-30 giorni)
  └── worker-NN leaf cert ...

OpenBao listener TLS cert (listener/secrets-manager, separato dal worker PKI)
```

**Principi non negoziabili:**

1. La root CA **non è mai online**. Se usata dall'issuer Master, viene gestita secondo la cerimonia approvata e la chiave privata vive su supporto fisico in cassaforte.
2. L'intermediate OpenBao firma solo i leaf worker del proprio PKI mount; il certificato gRPC/REST del Master segue il suo issuer separato. Se l'intermediate OpenBao è compromessa, si revoca e si rigenera senza toccare l'issuer Master.
3. I certificati worker scadono in fretta (7-30 giorni) per limitare il blast radius di una fuga di chiave.
4. Durante la rotazione, il bundle corrente resta valido fino allo switch atomico del nuovo bundle sul worker, così il worker può ri-registrarsi senza downtime.
5. La scadenza è **monitorata** con alert a 14, 7 e 2 giorni.
6. Ogni handshake mTLS **logga l'identità certificata** (CN + serial + fingerprint SHA-256).

## 2. Directory layout

The OpenBao worker flow does not keep worker leaf material on the master.
OpenBao stores the PKI issuer and role configuration; each worker owns its
private key and its local runtime bundle:

```
/opt/velox/certs/
├── root-ca/
│   ├── ca.crt              # Root CA certificate (public trust material)
│   └── ca.key              # NEVER committed — air-gapped device only
├── intermediate/
│   ├── ca.crt              # Intermediate CA certificate/public chain
│   ├── ca.key              # Encrypted at rest; never a worker key
│   ├── ca.srl              # Serial number counter
│   └── index.txt           # Issued certificate log
└── master/
    ├── server.crt          # Master gRPC/REST cert (se TLS Master è configurato)
    └── server.key          # Master private key (se TLS Master è configurato)

# On each worker; the key never leaves this host:
/etc/velox-worker/certs/current/
├── worker.key              # Generated locally; never sent to OpenBao
├── worker.crt              # Returned by OpenBao after CSR signing
└── ca.crt                  # Returned trust chain/issuing CA
```

The canonical OpenBao paths are `velox/production/workers/<worker-id>/credential`
in KV and `pki/sign/worker-<worker-id>` in PKI. The Master gRPC/REST TLS triple
(`VELOX_GRPC_TLS_CERT_FILE`, `VELOX_GRPC_TLS_KEY_FILE`, `VELOX_GRPC_TLS_CA_FILE`)
is a separate Master configuration and is not emitted by the worker PKI role.
No worker `worker.key`,
`worker.crt`, `ca.crt`, or PEM bundle is stored in the KV engine.

## 3. Generazione iniziale

### 3.1 Root CA (air-gapped, una tantum)

La Root CA resta una cerimonia **offline e fuori dal repository**. Non usare un
secondo generatore OpenSSL locale per la PKI production. L'issuer intermedio
viene generato e custodito internamente da OpenBao; l'operatore porta fuori
solo il CSR per la firma approvata della Root CA:

```bash
# Su un dispositivo offline: firma il CSR ricevuto da OpenBao secondo il
# processo PKI approvato e restituisci solo la catena firmata.

# Sul nodo OpenBao, prima/dopo la firma offline:
./deploy/openbao/scripts/initialize-pki-intermediate.sh \
  --generate-csr /secure/velox/velox-production-intermediate.csr
./deploy/openbao/scripts/initialize-pki-intermediate.sh \
  --set-signed /secure/velox/signed-intermediate-chain.pem
```

**Cosa conservare:**
- `ca.key` → supporto fisico in cassaforte (due copie in luoghi diversi)
- `ca.crt` → committato in repo (è pubblico, serve per la catena di fiducia)

### 3.2 Intermediate CA (online, rinnovabile)

OpenBao mantiene internamente la chiave dell'intermediate. Dopo aver importato
la catena firmata, verifica l'issuer e configura i ruoli PKI senza esportare
chiavi private:

```bash
./deploy/openbao/scripts/initialize-pki-intermediate.sh --check
bash ./deploy/openbao/scripts/provision-pki.sh --workers \
  "<worker-id-1> <worker-id-2>"
```

La Root CA torna offline dopo la firma; la chiave dell'intermediate resta
custodita da OpenBao.

### 3.3 OpenBao listener certificate

Genera il certificato TLS del listener OpenBao con lo script canonical del
componente, prima di avviare il compose OpenBao:

```bash
cd deploy/openbao
./scripts/gen-tls.sh
```

Il certificato del listener (`.velox/openbao/tls/server.crt`) è distinto dai
certificati mTLS worker emessi dal mount PKI. Il Master usa il certificato
pubblico OpenBao come CA di connessione e non genera una seconda PKI production.

> **OpenBao workers — mandatory rule.** Do not execute the legacy master-side
> worker certificate generation or Ansible/scp distribution snippets below for
> a worker enrolled in OpenBao. The canonical flow generates `worker.key` on
> the worker and sends only a CSR to OpenBao PKI; the private key never leaves
> the worker and no worker PEM is stored in KV.

### 3.4 Worker leaf certificates — canonical OpenBao flow

The worker generates its private key locally and sends only a CSR to its
per-worker OpenBao PKI role. The operator does not generate or copy
`worker.key` from the master.

```bash
# On the OpenBao operator/admin side: configure only the PKI role and issuer.
bash ./deploy/openbao/scripts/provision-pki.sh --worker <worker-id>

# On the worker, after AppRole files and OpenBao CA are installed:
sudo /opt/velox-worker/openbao-fetch-worker-secrets.sh --provision
sudo /opt/velox-worker/openbao-fetch-worker-secrets.sh --check
```

The resolver generates `/etc/velox-worker/certs/current/worker.key` locally,
submits the CSR to `pki/sign/worker-<worker-id>`, validates the returned
certificate/CA chain, and switches the complete bundle atomically. Only the
public CSR and the resulting signed chain cross the OpenBao boundary; the
private key is never sent, stored, or provisioned through KV.

> The older local OpenSSL worker-generation and Ansible/scp distribution flow
> is retired from the canonical repository path. Workers must use the OpenBao
> AppRole/CSR flow below; do not mix an external leaf certificate with the
> OpenBao flow for the same worker.

## 4. Rotazione automatizzata

### 4.1 Finestra di overlap

La rotazione canonical non gestisce un secondo file `cert-next` sul master.
Il worker mantiene il bundle corrente fino a quando il resolver OpenBao ha
validato il nuovo certificato; poi seleziona il nuovo bundle con uno switch
atomico. Il vecchio bundle versionato resta disponibile localmente come
rollback tecnico, ma non viene distribuito dal master.

### 4.2 Rinnovo automatizzato lato worker

Il rinnovo è eseguito dal timer systemd installato da `prepare-host.sh`, non da
un cron job del Master e non da una scansione OpenSSL dei certificati sul Master:

```bash
# Sul worker, per un rinnovo controllato:
sudo systemctl start velox-worker-mtls-renew.service
# Verifica senza esporre valori segreti:
sudo /opt/velox-worker/openbao-fetch-worker-secrets.sh --check
```

`velox-worker-mtls-renew.service` invoca il resolver con `--renew`. Il resolver
genera la chiave privata sul worker, invia solo il CSR a `pki/sign/worker-<id>`,
valida certificato e catena e seleziona il bundle completo atomically; il worker
viene riavviato solo quando è stato selezionato un bundle nuovo. Non creare o
copiare `cert-next`, `worker.key` o PEM worker sul Master.

Gli script `cert-rotation.sh` e `alert-cert-expiry.sh` non sono percorsi
canonical versionati e non devono essere installati come cron del Master. Per
il monitoraggio usare lo stato del timer, i log del servizio e il controllo
OpenBao `--check`; il monitoraggio centralizzato può essere aggiunto solo con
un'integrazione che non materializzi chiavi private sul Master.

### 4.3 Worker-side reload

Il resolver cambia il bundle atomically e `velox-worker-mtls-renew.service`
riavvia il worker solo quando è stato selezionato un bundle nuovo. Un cache hit
non provoca restart; il worker continua a usare il bundle corrente. Il prossimo
processo/connessione usa quindi il bundle già selezionato dal resolver.

## 5. Monitoraggio scadenza

### 5.1 Controllo scadenza

Il controllo operativo canonical è eseguito dal resolver sul worker e dal timer
systemd. Non copiare certificati sul Master e non installare un monitor cron
basato su una PKI locale rimossa.

```bash
sudo systemctl is-active velox-worker-mtls-renew.timer
sudo /opt/velox-worker/openbao-fetch-worker-secrets.sh --check
```

### 5.2 Soglie di alert

| Stato | Azione |
|---|---|
| Bundle valido oltre la renewal window | Nessuna emissione; il resolver mantiene il bundle corrente |
| Rinnovo necessario | Il timer avvia `--renew` e il servizio riavvia il worker solo dopo lo switch atomico |
| OpenBao/issuer non disponibile | Alert operativo; usare solo il cache attestato e non copiare PEM manualmente |
| Certificato non coerente o scaduto | Fail closed; correggere AppRole/issuer e verificare con `--check` |

Il monitoraggio centralizzato deve consumare stato e log del servizio senza
materializzare chiavi private o reintrodurre `cert-rotation.sh`.

## 6. Revoca

### 6.1 Quando revocare

- Worker compromesso (chiave privata leaked)
- Worker decommissionato definitivamente
- Intermediate CA compromessa

### 6.2 Procedura revoca worker

La revoca deve essere eseguita nell'engine PKI di OpenBao dall'operatore
abilitato, senza usare `openssl ca`, senza esportare la chiave dell'intermediate
e senza spostare PEM dal Master. Questa è una procedura operatore, non un
comando repository pronto all'uso: il metodo concreto dipende dal token, dalla
policy e dalla versione OpenBao. Il percorso API canonical è `pki/revoke`,
invocato con il seriale registrato nel certificato e il token previsto dalla
policy; consultare il runbook OpenBao dell'ambiente prima di procedere.

Dopo la revoca:

1. Rimuovi o disabilita il worker nel `WorkerNodeRegistry` se è decommissionato.
2. Sul worker compromesso, interrompi il servizio e conserva i log di audit.
3. Reinstalla/ruota l'AppRole secondo la procedura operativa OpenBao.
4. Esegui il rinnovo locale con `velox-worker-mtls-renew.service` solo dopo
   aver ristabilito l'identità del worker.
5. Verifica con `openbao-fetch-worker-secrets.sh --check` e con il probe del
   Master; non copiare `worker.key` dal Master.

### 6.3 Revoca intermediate CA (emergenza)

La revoca dell'intermediate è una cerimonia OpenBao + Root CA offline: non
usare `openssl ca` con una chiave dell'intermediate presente sul Master.
Blocca prima l'emissione sospetta nell'engine PKI, conserva l'evidenza e
coordina la nuova catena con l'operatore Root CA. Sul nodo OpenBao:

```bash
./deploy/openbao/scripts/initialize-pki-intermediate.sh \
  --generate-csr /secure/velox/velox-production-intermediate-v2.csr
# Dopo la firma offline approvata:
./deploy/openbao/scripts/initialize-pki-intermediate.sh \
  --set-signed /secure/velox/signed-intermediate-chain-v2.pem
./deploy/openbao/scripts/initialize-pki-intermediate.sh --check
bash ./deploy/openbao/scripts/provision-pki.sh --workers "<worker-id-1> <worker-id-2>"
```

La riemissione dei leaf avviene sui worker tramite il resolver CSR e il timer
mTLS; non distribuire certificati o chiavi dal Master.

## 7. Runbook di emergenza

### Scenario: certificato TLS del listener OpenBao scaduto

**Sintomi:** il Master non riesce a verificare la connessione TLS verso OpenBao.

**Procedura:**
1. Sul nodo OpenBao, rigenera solo il certificato del listener OpenBao:
   ```bash
   cd deploy/openbao
   ./scripts/gen-tls.sh
   ```
   Questo comando non emette certificati mTLS per Master o worker.
2. Riavvia il solo servizio OpenBao secondo il compose/unità dell'ambiente.
3. Verifica il resolver Master e i test OpenBao senza stampare secret:
   ```bash
   bash scripts/ci/test-openbao-master-tokens.sh
   ```

Il certificato gRPC/REST del Master, se configurato separatamente tramite
`VELOX_GRPC_TLS_CERT_FILE`, `VELOX_GRPC_TLS_KEY_FILE` e `VELOX_GRPC_TLS_CA_FILE`,
deve essere rinnovato con la procedura del suo issuer; non usare `gen-tls.sh` come generatore
di una PKI production alternativa.

### Scenario: certificato worker scaduto — worker isolato

**Sintomi:** un worker non si connette. Log worker: `certificate has expired` / `handshake failure`.

**Procedura OpenBao:**
1. Verifica il tunnel e l'issuer/ruolo PKI per quel worker.
2. Sul worker esegui il rinnovo locale:
   ```bash
   sudo systemctl start velox-worker-mtls-renew.service
   sudo /opt/velox-worker/openbao-fetch-worker-secrets.sh --check
   ```
3. Il resolver genera una nuova chiave sul worker, invia solo il CSR,
   valida il certificato e cambia il bundle atomico; non copiare `worker.key`
   dal master e non scrivere PEM nel KV.
4. Verifica: `curl -s http://master:8000/api/v1/workers | grep <worker-id>`

Per ambienti non migrati a OpenBao, la procedura legacy può restare in vigore,
ma non deve essere mescolata con il flusso PKI canonico dello stesso worker.

### Scenario: chiave privata worker leaked

**Sintomi:** alert sicurezza, log sospetti.

**Procedura (immediata):**
1. Revoca il certificato nell'engine PKI OpenBao (vedi §6.2)
2. Rimuovi o disabilita il worker nel `WorkerNodeRegistry`
3. Ruota l'AppRole del worker e genera nuova chiave/certificato localmente tramite il resolver CSR
4. Ruota anche gli altri certificati worker se il vettore di attacco è condiviso
5. Indaga la causa della fuga e conserva l'audit trail

## 8. Log dell'identità certificata

### 8.1 Lato master (gRPC handler)

Il master deve loggare a ogni handshake mTLS riuscito:
```
[MTLS] worker authenticated: cn=worker-01 serial=03:AB:CD:EF:01 fingerprint=SHA256:ab12cd34... peer=10.0.0.5:54321
```

Campi obbligatori nel log:
- `cn` — CommonName del certificato client
- `serial` — Serial number del certificato
- `fingerprint` — SHA-256 del certificato DER
- `peer` — IP/porta del client

### 8.2 Lato worker (transport factory)

Il worker deve loggare a ogni connessione:
```
[MTLS] connected to master: cn=localhost serial=02:11:22:33:44 fingerprint=SHA256:ef56gh78...
```

### 8.3 Audit trail

Tutti i log di identità certificata vanno in un file separato:
```
/var/log/velox/mtls-audit.log
```

Rotazione log: 30 giorni, compressi.

## 9. Checklist operativa

### Setup iniziale
- [ ] Root CA generata su dispositivo air-gapped
- [ ] Chiave root CA in cassaforte (due copie)
- [ ] Certificato root CA committato in repo
- [ ] Intermediate CA generata e configurata
- [ ] Certificato TLS listener OpenBao emesso con `deploy/openbao/scripts/gen-tls.sh`
- [ ] Certificati worker emessi tramite OpenBao PKI + CSR locale
- [ ] `velox-worker-mtls-renew.timer` attivo su ogni worker
- [ ] Alert configurati (Slack / PagerDuty / email) senza materializzare chiavi sul Master
- [ ] Log `mtls-audit.log` configurato con rotazione

### Verifica pre-deploy
- [ ] `make e2e-grpc` passa (6 casi mTLS)
- [ ] `bash scripts/ci/test-openbao-worker-secrets.sh` → PASS
- [ ] `/opt/velox-worker/openbao-fetch-worker-secrets.sh --check` → exit 0
- [ ] `systemctl is-active velox-worker-mtls-renew.timer` → active

### Rotazione periodica (mensile)
- [ ] `openbao-fetch-worker-secrets.sh --check` → verificare coerenza remote/local
- [ ] `mtls-audit.log` → verificare fingerprint corrispondenti ai serial attesi
- [ ] audit OpenBao PKI → verificare emissioni/revoche secondo la policy

### Verifica DR (semestrale)
- [ ] Root CA accessibile dal supporto fisico
- [ ] Procedura di revoca intermediate CA testata in staging
- [ ] Procedura di ri-emissione completa testata in staging
- [ ] Backup cert e chiavi verificate

## 10. Riferimenti

- `deploy/openbao/scripts/initialize-pki-intermediate.sh` — cerimonia OpenBao intermediate + firma offline
- `deploy/openbao/scripts/provision-pki.sh` — ruoli PKI worker per CSR, senza private key in KV (invocare con `bash`)
- `deploy/openbao/scripts/gen-tls.sh` — certificato TLS del listener OpenBao
- `scripts/gen-worker-certs.sh` — generatore dev/CI (10 anni, solo self-signed)
- `tests/e2e/grpc-control-plane/certs/generate-dev-pki.sh` — generatore E2E (7d CA / 1d leaf)
- `deploy/runtime/velox-worker-mtls-renew.service` / `.timer` — rinnovo worker via CSR OpenBao
- `docs/roadmap/13-mtls.md` — specifica mTLS originale
- `docs/operations/PR-1-migration.md` — configurazione TLS lato worker (PR 1)
- `deploy/runtime/README.md` — installazione e verifica del runtime worker canonical
