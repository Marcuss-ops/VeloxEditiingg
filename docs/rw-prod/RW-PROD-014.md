# RW-PROD-014 — Monitoraggio e rotazione PKI

**Priorità:** P0
**Dipendenze:** RW-PROD-001
**Stato attuale:** `scripts/gen-production-pki.sh` (3 livelli CA), `scripts/gen-worker-certs.sh` (dev), `deploy/certs/monitor-expiry.sh` emette OK/WARN/CRIT/EXPIRED. **Gap critico**: `monitor-expiry.sh` non fail-closed su directory assente / zero certificati / cert illeggibile — attualmente exit 0. Rotation live senza downtime non documentata.

---

## 1. Pain points

1. **`monitor-expiry.sh` directory assente → exit 0**
   - Lo script usa `find ... -print0` con fallback `|| true`; se directory non esiste, `CERTS=()` vuoto, `worst_exit=0` → ESCI 0 (KO: dovrebbe uscire 4).

2. **Zero cert validi → exit 0**
   - Stessa ragione: `find` ritorna vuoto → OK. La spec richiede errore.

3. **Cert illeggibile/corrotto → non viene contato**
   - `inspect_cert` fallisce con `|| continue` — silenziosamente scartato. Specifica vuole errore.

4. **Rotation senza downtime non documentata.**
   - Non c'è procedura per supportare overlap `worker.crt` (vecchio + nuovo) durante rollover.

5. **Revoca immediata non testata.**
   - Procedura manuale via `revoked/` directory + rimozione da allowlist, ma non automatizzata.

6. **`alert-cert-expiry.sh` non esiste** (cross-link RW-PROD-013).

---

## 2. Soluzione

1. **Fail-closed su directory assente / vuota:**
   - `monitor-expiry.sh`:
     ```bash
     if [[ ! -d "$CERT_DIR" ]]; then
       echo '{"error":"cert_dir missing","cert_dir":"'"$CERT_DIR"'"}' >&2
       exit 4
     fi
     if [[ ${#CERTS[@]} -eq 0 ]]; then
       echo '{"error":"zero_certificates","cert_dir":"'"$CERT_DIR"'"}' >&2
       exit 5
     fi
     ```
   - Inoltre a fine loop:
     ```bash
     for cert_path in "${CERTS[@]}"; do
       if ! "$OPENSSL" x509 -in "$cert_path" -noout >/dev/null 2>&1; then
         echo '{"error":"certificate_unreadable","path":"'"$cert_path"'"}' >&2
         exit 6
       fi
     done
     ```

2. **Documentare rotation live:**
   - Runbook `docs/operations/PR-6-pki-rotation-runbook.md` già esiste ma manca sezione "overlap". Aggiungere "Rotate worker without downtime".
   - Procedura: worker può presentare 2 cert (CN/SAN con serial vecchio + nuovo) durante finestra 7gg; master verifica entrambi?

3. **Revoca automatica:**
   - Directory `/opt/velox/certs/revoked/` con file `serial.fingerprint` → monitor li legge e pubblica evento a master.

4. **Archivio:**
   - `state/certs/issued_at_${serial}.json` con `{serial, fingerprint, issued_at, expires_at, owner_host, worker_id}`.
   - Master endpoint `GET /api/v1/certs/serial/:serial` per lookup.

---

## 3. Azioni concrete

| # | File:line | Azione |
|---|-----------|--------|
| A1 | `deploy/certs/monitor-expiry.sh` | Aggiungere fail-closed su directory assente, vuota, cert illeggibile (vedi snippet sopra). |
| A2 | `scripts/gen-production-pki.sh` | Aggiungere exit code con serial tracciato. |
| A3 | `docs/operations/PR-6-pki-rotation-runbook.md` | Nuova sezione "Rotate worker without downtime" con procedura overlap. |
| A4 | `DataServer/internal/grpcserver/authorizer.go` | Supportare più cert per worker durante overlap (allowlist serial/fingerprint multipli per worker). |
| A5 | `deploy/certs/revocation.sh` (nuovo) | Sposta cert in `revoked/` directory, genera evento revoca. |
| A6 | `DataServer/internal/store/store_worker_control.go` | Nuova tabella `cert_revocations` o campo JSON. |
| A7 | `scripts/audit-rom-issues.sh` (nuovo) | Lista tutti i cert con `issued_at, expires_at, owner_host`; expiry ranking. |
| A8 | `deploy/certs/monitor-expiry.sh` | Aggiungere emission evento `cert.renewal.due` su soglie 14/7/2gg. |

---

## 4. Criteri di accettazione

- [ ] Cert ruotato senza perdita job (overlap test).
- [ ] Cert revocato rifiutato immediatamente.
- [ ] Directory vuota → exit non-zero (5).
- [ ] Cert corrotto/non leggibile → exit non-zero (6).
- [ ] Cert in scadenza 14gg → alert + owner operativo.
- [ ] Tabella `cert_revocations` populated su revoke.

---

## 5. Test obbligatori

- `MonitorExpiry_DirMissing_Exits4`.
- `MonitorExpiry_ZeroCert_Exits5`.
- `MonitorExpiry_CertCorrupt_Exits6`.
- `MonitorExpiry_All14DaysTrend_Exits1Warn`.
- `MonitorExpiry_Expiring_2Day_Exits2Crit`.
- `MonitorExpiry_Expired_Exits3`.
- `TestRotation_Overlap_AcceptsOldAndNew`.
- `TestRevocation_ImmediateRefusal`.

---

## 6. Evidenze

- Log `event=cert.expiry.warning days_left=14`.
- Report `cert-audit.json` con tutti i serial.
- Test E2E rotation (vedi RW-PROD-001).
- Output Alertmanager simulation.
