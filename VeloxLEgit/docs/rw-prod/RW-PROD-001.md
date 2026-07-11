# RW-PROD-001 — Identità worker e mTLS production fail-closed

**Priorità:** P0
**Dipendenze:** nessuna
**Stato attuale:** parzialmente coperto (PR-1 migration: rifiuto mix insecure+TLS, rifiuto insecure in production). Lacune su scadenza minima, permessi chiave, plaintext-vs-TLS server side, fingerprint su rifiuto.

---

## 1. Pain points

1. **`worker_id` non verificato formalmente.**
   `RemoteCodex/.../pkg/config/config.go` valida solo presenza/non-vuoto; non c'è check su shape (regex `^[a-z][a-z0-9-]{2,62}$`), non c'è divieto di collisione/double-prefix `host_*`.

2. **Scadenza minima 14gg non enforced.**
   `pkg/config/config.go:360` verifica **esistenza** e **coppia cert/key**, ma non parsa `notAfter` e non rifiuta cert già scaduti o con <14gg.

3. **Permessi chiave privata non controllati.**
   Nessun enforcement di `0600` né nel validatore Go né negli script (`scripts/gen-worker-certs.sh`, `scripts/gen-production-pki.sh`, `deploy/scripts/apply-local-worker-config.sh`).

4. **Cert fingerprint non loggato al rifiuto.**
   `pkg/config/config.go` ritorna stringhe tipo `"tls_cert_file not found at %q"` ma non emette fingerprint/serial/issuer del cert ricevuto (utile durante revoca/rotation).

5. **Plaintext-vs-TLS server non ha reject hard.**
   `DataServer/internal/grpcserver/authorizer.go` ha l'allowlist + dev bypass, ma non c'è un assert unitario che certifica "plaintext connection to TLS-only listener = REJECTED in production".

6. **Cert condivisi tra worker non vietati a livello Go.**
   `scripts/gen-production-pki.sh:308` genera un cert per ogni `worker_name` passato, ma nulla vieta di deployare lo stesso `worker.crt`/key su due host fisici.

---

## 2. Soluzione

Aggiungere quattro livelli di enforcement:

1. **Hardening validazione transport factory** (`RemoteCodex/.../pkg/config/config.go`):
   - Paring `notAfter` dal cert, rifiuto se `now + 14d > notAfter` o già scaduto.
   - `os.Stat(key).Mode().Perm() == 0o600` *o errore esplicito* (warning per ambienti dev ok, prod no).
   - Log strutturato con `fingerprint + serial + CN + SAN` su ogni rifiuto (codice logging nuovo `CodeMTLSReject`).

2. **Sanitizzazione `worker_id`**:
   - `NormalizeWorkerID` esiste già (`pkg/config/config.go`) ma non rifiuta shape non-conformi. Aggiungere check regex post-normalize, errori `worker_id_invalid_shape`.

3. **Server-side reject plaintext-into-TLS**:
   - Aggiungere `TestServer_PlaintextRejectedWhenTLSRequired` che fa dial TCP al listener TLS senza TLS handshake e asserisce rifiuto in <1s.
   - `DataServer/internal/grpcserver/bootstrap_grpc.go`: nuova env `VELOX_GRPC_REQUIRE_TLS=true` deve far fallire bootstrap se `tls_cert_file`/`tls_key_file` mancanti.

4. **Anti-condivisione cert**:
   - Nel bundle di deploy (`deploy/scripts/apply-local-worker-config.sh`) hash `worker.crt` + serial memorizzato in `LAST_CERT_HASH`/`LAST_CERT_SERIAL`. Se due host Ansible condividono lo stesso (warn, fail in `--strict`).
   - `ansible/prechecks.yml`: pre-check `worker_id unique` già presente, estendere con `cert_serial != altri host` (richiede `openssl x509 -serial -noout` durante gather_facts).

---

## 3. Azioni concrete

| # | File:line | Azione |
|---|-----------|--------|
| A1 | `RemoteCodex/.../pkg/config/config.go` (in `Validate()`, zona 245-265) | Aggiungere `cert, err := tls.LoadX509KeyPair(...)`, poi `leaf := cert.Leaf`; rifiuto se `leaf.NotAfter.Before(time.Now().Add(14 * 24 * time.Hour))`. |
| A2 | `RemoteCodex/.../pkg/config/config.go` `Validate()` (zona 245-265) | Aggiungere `info, _ := os.Stat(tlsCfg.KeyFile); if runtime.GOOS != "windows" && info.Mode().Perm() & 0o077 != 0 { return error("key_file permission must be 0600") }` (warn-only per environment=dev). |
| A3 | `RemoteCodex/.../pkg/config/config.go` | Log strutturato su rifiuto: `logger.LogCertRejected(workerID, fingerprint, serial, reason)`. Aggiungere in `pkg/logger`. |
| A4 | `RemoteCodex/.../pkg/config/config.go` | (dopo la normalizzazione) `if !workerIDRegexp.MatchString(cfg.WorkerID) { return ErrInvalidWorkerID }`. Definire regexp in `shared/identity/identity.go`. |
| A5 | `DataServer/internal/grpcserver/bootstrap_grpc.go` | Aggiungere env `VELOX_GRPC_REQUIRE_TLS=true` ⇒ `tlsConfig == nil ⇒ bootstrap panic`. |
| A6 | `DataServer/internal/grpcserver/authorizer_test.go` | Nuovo test `TestServer_PlaintextRejectedWhenTLSRequired`: dial in chiaro, verifica errore. |
| A7 | `scripts/check-share-cert.sh` (nuovo) | Diff `worker.crt` + `worker.key` tra host Ansible e fallisce se duplicati. |
| A8 | `deploy/scripts/apply-local-worker-config.sh` | Dopo applicazione config, scrivere `LAST_CERT_HASH` + `LAST_CERT_SERIAL` in `/opt/velox/worker_cert.meta`. |
| A9 | `scripts/gen-production-pki.sh` | Aggiungere assert: subject CN==worker_id deducibile da CLI `$WORKER_ID`, non da default. |

---

## 4. Criteri di accettazione (riferimento runbook)

- [ ] Cert valido + scadenza > 14gg + key 0600 + fogli CN corretti → registration OK.
- [ ] Cert scaduto → registration REJECTED.
- [ ] CA errata → handshake REJECTED.
- [ ] Cert di un altro worker (serial/fingerprint non nell'allowlist) → registration REJECTED.
- [ ] Plaintext verso TLS-only master → connection REJECTED in <1s.
- [ ] TLS parziale (solo key, no CA) → worker NON si avvia.
- [ ] Nessuna chiave privata, fingerprint, secret compare nei log.

---

## 5. Test obbligatori

- `pkg/config/config_test.go`: TS-1.1 scadenza OK, TS-1.2 scadenza 13d reject, TS-1.3 expired reject, TS-1.4 key 0644 reject prod, TS-1.5 key 0644 warn dev, TS-1.6 worker_id shape reject.
- `tests/e2e/grpc-control-plane/run.sh` (case 5) — deve restare verde dopo queste modifiche.
- `TestsServer_PlaintextRejectedWhenTLSRequired` (nuovo, vedi A6).
- `scripts/check-share-cert.sh` — eseguibile su `production.ini.example` deve passare con warning su host duplicati.

---

## 6. Evidenze richieste

- Output `openssl verify -CAfile ca.crt worker.crt` per ogni worker.
- Log handshake positivo (con `fingerprint + serial + CN`).
- Log rifiuto dei 5 casi negativi (vedi criteri).
- Report JSON per worker: `{worker_id, fingerprint, serial, not_before, not_after, owner_host}`.
