# PR 6 — Production PKI Rotation Runbook

Status: **operativo**

Owner: infrastructure

## 1. Architettura a 3 livelli

```
root CA (offline, 5-10 anni)
  │
  └── intermediate CA (online, 6-12 mesi)
        │
        ├── master server cert (30-90 giorni)
        ├── worker-01 leaf cert (7-30 giorni)
        ├── worker-02 leaf cert (7-30 giorni)
        └── worker-NN leaf cert ...
```

**Principi non negoziabili:**

1. La root CA **non è mai online**. Generata su un dispositivo air-gapped, la chiave privata vive su supporto fisico in cassaforte.
2. Solo l'intermediate CA firma certificati operativi. Se compromessa, si revoca e si rigenera dalla root senza toccare i leaf.
3. I certificati worker scadono in fretta (7-30 giorni) per limitare il blast radius di una fuga di chiave.
4. Durante la rotazione, vecchio e nuovo certificato coesistono per una finestra di overlap (default: 7 giorni) così il worker può ri-registrarsi senza downtime.
5. La scadenza è **monitorata** con alert a 14, 7 e 2 giorni.
6. Ogni handshake mTLS **logga l'identità certificata** (CN + serial + fingerprint SHA-256).

## 2. Directory layout

```
/opt/velox/certs/
├── root-ca/
│   ├── ca.crt              # Root CA certificate (committed to deploy repo)
│   └── ca.key              # NEVER committed — air-gapped device only
├── intermediate/
│   ├── ca.crt              # Intermediate CA cert (committed)
│   ├── ca.key              # Encrypted at rest (ansible-vault or similar)
│   ├── ca.srl              # Serial number counter
│   └── index.txt           # Issued certificate log
├── master/
│   ├── server.crt          # Master's server cert (current)
│   ├── server.key          # Master's private key
│   ├── server-next.crt     # Next cert during rotation overlap
│   └── server-next.key
└── workers/
    ├── worker-<id>.crt     # Worker leaf cert
    ├── worker-<id>.key
    └── revoked/            # Revoked certificates (CRL-equivalent)
```

## 3. Generazione iniziale

### 3.1 Root CA (air-gapped, una tantum)

```bash
# Eseguito su dispositivo OFFLINE
./scripts/gen-production-pki.sh root-ca \
  --out-dir /secure/velox-root-ca \
  --cn "Velox Root CA" \
  --days 3650  # 10 anni
```

**Cosa conservare:**
- `ca.key` → supporto fisico in cassaforte (due copie in luoghi diversi)
- `ca.crt` → committato in repo (è pubblico, serve per la catena di fiducia)

### 3.2 Intermediate CA (online, rinnovabile)

```bash
# Eseguito sul master
./scripts/gen-production-pki.sh intermediate \
  --out-dir /opt/velox/certs/intermediate \
  --cn "Velox Intermediate CA v1" \
  --days 270 \
  --root-ca-cert /secure/velox-root-ca/ca.crt \
  --root-ca-key /secure/velox-root-ca/ca.key  # chiave root usata solo qui
```

**Dopo la generazione, la chiave root torna offline.** L'intermediate CA vive sul master.

### 3.3 Master server certificate

```bash
./scripts/gen-production-pki.sh server \
  --out-dir /opt/velox/certs/master \
  --cn "velox-master.internal.example.com" \
  --san "DNS:velox-master.internal.example.com,DNS:localhost,IP:127.0.0.1" \
  --days 90 \
  --intermediate-dir /opt/velox/certs/intermediate
```

### 3.4 Worker leaf certificates

```bash
# Uno per worker. CN DEVE corrispondere al worker_id.
./scripts/gen-production-pki.sh worker \
  --out-dir /opt/velox/certs/workers \
  --cn "worker-01" \
  --days 14 \
  --intermediate-dir /opt/velox/certs/intermediate
```

**Distribuzione:** il triplet `worker-<id>.crt` + `worker-<id>.key` + `intermediate/ca.crt`
va copiato sul worker via Ansible (`deploy/playbooks/deploy-worker-certs.yml`).

## 4. Rotazione automatizzata

### 4.1 Finestra di overlap

Durante la rotazione, il worker ha **due certificati validi**:
- `worker-<id>.crt` — corrente
- `worker-<id>-next.crt` — nuovo, generato N giorni prima della scadenza

Il worker carica entrambi e prova il nuovo per primo. Se l'handshake fallisce, usa il vecchio.
Il master accetta entrambi i certificati (stesso CN, serial diverso) durante la finestra.

**Timeline (worker leaf da 14 giorni):**

```
giorno 0:   cert emesso (scade giorno 14)
giorno 7:   generazione cert-next (scade giorno 21)
giorno 7-14: finestra overlap (worker ha entrambi, master accetta entrambi)
giorno 14:  cert corrente scade → rimosso dal worker
giorno 14-21: cert-next è il nuovo corrente
giorno 21:  prossima rotazione...
```

### 4.2 Cron job di rotazione (sul master)

```bash
# /etc/cron.daily/velox-cert-rotation
# Eseguito ogni notte. Genera cert-next per worker che scadono entro 7 giorni.

/opt/velox/scripts/cert-rotation.sh
```

Lo script:
1. Scansiona `/opt/velox/certs/workers/` per file `.crt`
2. Per ogni certificato, legge la data di scadenza via `openssl x509 -enddate`
3. Se scade entro 7 giorni E non esiste già un `-next.crt` → genera `worker-<id>-next.{crt,key}`
4. Se scade entro 1 giorno E il `-next.crt` esiste da > 6 giorni → promuove `-next` a corrente (mv)
5. Logga ogni azione in `/var/log/velox/cert-rotation.log`

> **TODO:** Lo script `cert-rotation.sh` non è ancora versionato in repo.
> Creare da template in `deploy/certs/` e copiare in `/opt/velox/scripts/`.
> Vedi `scripts/gen-production-pki.sh` per i comandi openssl di generazione.
> Vedi §4.1 per la logica della finestra di overlap.

### 4.3 Worker-side reload

Il worker-agent rileva la presenza di un nuovo certificato sul disco e lo carica
al prossimo tentativo di connessione (nessun restart necessario — PR 1 ha già il
path `tls_cert_file` nel config JSON).

## 5. Monitoraggio scadenza

### 5.1 Script di controllo

Vedi `deploy/certs/monitor-expiry.sh` per lo script versionato. Copiare in
`/opt/velox/scripts/monitor-expiry.sh` sul master.

```bash
# /opt/velox/scripts/monitor-expiry.sh
# Restituisce exit code 0 se tutto OK, 1/2/3 se ci sono scadenze imminenti.

USAGE: monitor-expiry.sh [--json] [--dir /opt/velox/certs]
```

**Output JSON (per integrazione con sistema di monitoring):**
```json
{
  "certs": [
    {
      "path": "/opt/velox/certs/master/server.crt",
      "cn": "velox-master.internal.example.com",
      "serial": "01:AB:CD:...",
      "expires_at": "2026-08-15T00:00:00Z",
      "days_left": 52,
      "status": "ok"
    },
    {
      "path": "/opt/velox/certs/workers/worker-01.crt",
      "cn": "worker-01",
      "serial": "02:EF:01:...",
      "expires_at": "2026-07-05T00:00:00Z",
      "days_left": 4,
      "status": "warning"
    }
  ],
  "critical_count": 0,
  "warning_count": 1
}
```

### 5.2 Soglie di alert

| Giorni alla scadenza | Livello | Azione |
|---|---|---|
| > 14 | OK | Nessuna |
| 14 | WARNING | Notifica canale #velox-ops |
| 7 | WARNING | Notifica + genera cert-next automaticamente |
| 2 | CRITICAL | Notifica #velox-ops + escalation on-call |
| 0 | EXPIRED | PagerDuty alert — intervento immediato |

### 5.3 Integrazione cron

```bash
# /etc/cron.d/velox-cert-monitor
# Eseguito ogni 6 ore
0 */6 * * * root /opt/velox/scripts/monitor-expiry.sh --json | \
  /opt/velox/scripts/alert-cert-expiry.sh
```

> **TODO:** Lo script `alert-cert-expiry.sh` non è ancora versionato in repo.
> Deve parsare il JSON da stdin e inviare notifiche via Slack webhook,
> PagerDuty, o email in base ai threshold configurati.

## 6. Revoca

### 6.1 Quando revocare

- Worker compromesso (chiave privata leaked)
- Worker decommissionato definitivamente
- Intermediate CA compromessa

### 6.2 Procedura revoca worker

```bash
# 1. Genera CRL entry
openssl ca -revoke /opt/velox/certs/workers/worker-03.crt \
  -keyfile /opt/velox/certs/intermediate/ca.key \
  -cert /opt/velox/certs/intermediate/ca.crt \
  -config /opt/velox/certs/intermediate/openssl.cnf

# 2. Rigenera CRL
openssl ca -gencrl \
  -keyfile /opt/velox/certs/intermediate/ca.key \
  -cert /opt/velox/certs/intermediate/ca.crt \
  -out /opt/velox/certs/intermediate/crl.pem \
  -config /opt/velox/certs/intermediate/openssl.cnf

# 3. Sposta il certificato revocato
mkdir -p /opt/velox/certs/workers/revoked
mv /opt/velox/certs/workers/worker-03.crt /opt/velox/certs/workers/revoked/
mv /opt/velox/certs/workers/worker-03.key /opt/velox/certs/workers/revoked/

# 4. Rimuovi il worker dall'allowlist
# Aggiorna VELOX_ALLOWED_WORKERS per escludere worker-03

# 5. Logga la revoca
echo "$(date -Iseconds) revoked worker-03 serial=$(openssl x509 -in /opt/velox/certs/workers/revoked/worker-03.crt -serial -noout | cut -d= -f2)" \
  >> /var/log/velox/cert-revocations.log
```

### 6.3 Revoca intermediate CA (emergenza)

```bash
# 1. Recupera la root CA dall'air-gapped storage
# 2. Revoca l'intermediate
openssl ca -revoke /opt/velox/certs/intermediate/ca.crt \
  -keyfile /secure/velox-root-ca/ca.key \
  -cert /secure/velox-root-ca/ca.crt

# 3. Genera nuova intermediate CA
./scripts/gen-production-pki.sh intermediate \
  --out-dir /opt/velox/certs/intermediate-v2 \
  --cn "Velox Intermediate CA v2" \
  --days 270 \
  --root-ca-cert /secure/velox-root-ca/ca.crt \
  --root-ca-key /secure/velox-root-ca/ca.key

# 4. Ri-emetti TUTTI i certificati leaf
# 5. Distribuisci a tutti i worker
# 6. RIavvia i worker o attendi il prossimo reload
```

## 7. Runbook di emergenza

### Scenario: certificato master scaduto

**Sintomi:** worker non si connettono. Log: `certificate has expired`.

**Procedura:**
1. Genera nuovo certificato master:
   ```bash
   ./scripts/gen-production-pki.sh server \
     --out-dir /opt/velox/certs/master \
     --cn "velox-master.internal.example.com" \
     --days 90 \
     --intermediate-dir /opt/velox/certs/intermediate
   ```
2. Aggiorna il file env del master:
   ```bash
   # /etc/velox-server.env
   VELOX_GRPC_TLS_CERT_FILE=/opt/velox/certs/master/server.crt
   VELOX_GRPC_TLS_KEY_FILE=/opt/velox/certs/master/server.key
   ```
3. Riavvia il master: `systemctl restart velox-server`
4. Verifica: `make e2e-grpc` (casi TLS)

### Scenario: certificato worker scaduto — worker isolato

**Sintomi:** un worker non si connette. Log worker: `certificate has expired` / `handshake failure`.

**Procedura:**
1. Genera nuovo certificato per quel worker sul master:
   ```bash
   ./scripts/gen-production-pki.sh worker \
     --out-dir /opt/velox/certs/workers \
     --cn "worker-05" --days 14 \
     --intermediate-dir /opt/velox/certs/intermediate
   ```
2. Copia il triplet sul worker:
   ```bash
   scp /opt/velox/certs/workers/worker-05.{crt,key} \
       /opt/velox/certs/intermediate/ca.crt \
       worker-05:/opt/velox/certs/
   ```
3. Il worker carica automaticamente i nuovi cert al prossimo tentativo di connessione (nessun restart).
4. Verifica: `curl -s http://master:8000/api/v1/workers | grep worker-05`

### Scenario: chiave privata worker leaked

**Sintomi:** alert sicurezza, log sospetti.

**Procedura (immediata):**
1. Revoca il certificato (vedi §6.2)
2. Rimuovi il worker dall'allowlist
3. Rigenera nuova chiave + certificato per quel worker
4. Ruota anche gli altri certificati worker se il vettore di attacco è condiviso
5. Indaga la causa della fuga

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
- [ ] Certificato server master emesso
- [ ] Certificati worker emessi per tutta la flotta
- [ ] Script `check-cert-expiry.sh` in esecuzione via cron
- [ ] Alert configurati (Slack / PagerDuty / email)
- [ ] Cron di rotazione automatica attivo
- [ ] Log `mtls-audit.log` configurato con rotazione

### Verifica pre-deploy
- [ ] `make e2e-grpc` passa (6 casi mTLS)
- [ ] `openssl verify -CAfile /opt/velox/certs/root-ca/ca.crt -untrusted /opt/velox/certs/intermediate/ca.crt /opt/velox/certs/master/server.crt` → OK
- [ ] `openssl verify -CAfile /opt/velox/certs/root-ca/ca.crt -untrusted /opt/velox/certs/intermediate/ca.crt /opt/velox/certs/workers/worker-01.crt` → OK
- [ ] `/opt/velox/scripts/check-cert-expiry.sh` → exit 0, nessuna scadenza imminente

### Rotazione periodica (mensile)
- [ ] `check-cert-expiry.sh --json` → verificare warning/critical count = 0
- [ ] `mtls-audit.log` → verificare fingerprint corrispondenti ai serial attesi
- [ ] `openssl crl -in /opt/velox/certs/intermediate/crl.pem -text` → verificare revoche recenti

### Verifica DR (semestrale)
- [ ] Root CA accessibile dal supporto fisico
- [ ] Procedura di revoca intermediate CA testata in staging
- [ ] Procedura di ri-emissione completa testata in staging
- [ ] Backup cert e chiavi verificate

## 10. Riferimenti

- `scripts/gen-production-pki.sh` — generatore PKI a 3 livelli
- `scripts/gen-worker-certs.sh` — generatore dev/CI (10 anni, solo self-signed)
- `tests/e2e/grpc-control-plane/certs/generate-dev-pki.sh` — generatore E2E (7d CA / 1d leaf)
- `deploy/certs/monitor-expiry.sh` — script di monitoraggio scadenza
- `docs/roadmap/13-mtls.md` — specifica mTLS originale
- `docs/operations/PR-1-migration.md` — configurazione TLS lato worker (PR 1)
- `deploy/certs/` — directory per script operativi (cert-rotation.sh, alert-cert-expiry.sh da creare)
