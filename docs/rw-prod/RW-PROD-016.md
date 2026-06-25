# RW-PROD-016 — Comando `velox-worker-agent doctor`

**Priorità:** P0
**Dipendenze:** RW-PROD-001 → 015
**Stato attuale:** Non esiste. Solo `--version`, `--generate-config`, `--validate-config`. **Gap**: comando unico deterministico che ritorna `READY` o `NOT_READY` con JSON strutturato.

---

## 1. Pain points

1. **Nessun comando `doctor`.** Operatori eseguono check a mano (cron scripts, health endpoints, validate-config) senza standardizzazione.
2. **Reuse logica esistente.** Duplicare check da config package, registry, sampler è vietato (OWNERSHIP).
3. **Output JSON stabile e versionato.** Specifica vuole schema fisso.

---

## 2. Soluzione

Nuovo subcommand `velox-worker-agent doctor`:

```bash
velox-worker-agent doctor \
  --production \
  --config /opt/velox/worker_config.json \
  --json
```

**Interfaccia** (in `cmd/velox-worker-agent/main.go`):
- `doctorCmd` FlagSet con `--production`, `--json`, `--canary` (opzionale).
- Delega a `pkg/doctor.Run(cfg, opts) (Report, error)`.

**Controlli (riuso diretto dei validatori RW-PROD-002 + readiness RW-PROD-004 + canary RW-PROD-007):**

1. **Config e ambiente:** riusa `pkg/doctor.ValidateConfig`.
2. **Identità worker:** riusa `pkg/doctor.ValidateWorkerID`.
3. **Cert/chiave/CA/scadenza:** `pkg/doctor.ValidateCertExpiry` (RW-PROD-001).
4. **Permission chiave privata:** `pkg/doctor.ValidateKeyPermission`.
5. **DNS master:** `pkg/doctor.ValidateDNSReachability`.
6. **Handshake mTLS:** `pkg/doctor.ValidateMTLSHandshake` (dial + read hello).
7. **Motore C++ + FFmpeg:** RW-PROD-002/003.
8. **Executor registry:** `pkg/doctor.ValidateRegistry`.
9. **Cache/Blob/Temp Dir read+write+delete:** `pkg/doctor.ValidateDirs`.
10. **Disk free:** `pkg/doctor.ValidateDiskFree`.
11. **CPU/RAM:** `pkg/doctor.ValidateResources`.
12. **Health/metrics ports:** `pkg/doctor.ValidatePorts`.
13. **Versioni protocol/engine/bundle:** `pkg/doctor.VersionsConsistent`.
14. **Canary reale (opzionale `--canary`):** delega a RW-PROD-007.
15. **Visibility nel master:** `pkg/doctor.ValidateMasterVisibility` (HTTP `GET /api/v1/workers/:id`).

**Output JSON target:**

```json
{
  "worker_id":"worker-01",
  "verdict":"READY",
  "checked_at":"2026-06-24T12:00:00Z",
  "checks":[
    {"id":"mtls","status":"PASS","detail":"client certificate accepted"},
    {"id":"engine.binary","status":"PASS","detail":"velox-render-cpp executable"},
    {"id":"engine.self_render","status":"FAIL","detail":"timeout","remedy":"reinstall engine"}
  ]
}
```

**Exit code**: 0 solo se tutti `PASS`; ≥1 se anche 1 `FAIL`. In `--production`, WARN conta come FAIL.

---

## 3. Azioni concrete

| # | File:line | Azione |
|---|-----------|--------|
| A1 | `RemoteCodex/.../cmd/velox-worker-agent/main.go` | Nuovo subcommand `doctorCmd` con FlagSet proprio. |
| A2 | `RemoteCodex/.../pkg/doctor/` | Nuovo package con i singoli validatori (RW-PROD-002 li crea; qui li riusa + aggiunge handshake + master visibility). |
| A3 | `RemoteCodex/.../pkg/doctor/handshake.go` | Dial master + Hello → HelloAck → disconnect. |
| A4 | `RemoteCodex/.../pkg/doctor/visibility.go` | HTTP GET su master `/api/v1/workers/:worker_id`. |
| A5 | `RemoteCodex/.../pkg/doctor/canary.go` | Wrapper opzionale che delega a `cmd/canary`. |
| A6 | `RemoteCodex/.../pkg/doctor/report.go` | Struct `Report`, JSON marshal versionato `schema_version:"1"`. |
| A7 | `deploy/scripts/apply-local-worker-config.sh` | Dopo `--validate-config`, lanciare anche `doctor --json` (gate). |
| A8 | `docs/operations/03-build-deploy-and-ci-hardening.md` | Sezione "Worker doctor workflow". |

---

## 4. Criteri di accettazione

- [ ] Exit `0` solo se tutti i check `PASS`.
- [ ] Exit non-zero con almeno un check FAIL.
- [ ] Nessun segreto nell'output (test fuzz).
- [ ] Output stabile e versionato (`schema_version`).
- [ ] Modalità human-readable (table) e JSON.
- [ ] Testabile senza duplicare la logica del config package, registry, sampler (riuso).
- [ ] `doctor --production --canary` esegue anche un canary reale.

---

## 5. Test obbligatori

- `TestDoctor_AllGreen` (mock tutors).
- `TestDoctor_BadCert` (cert scaduto).
- `TestDoctor_EngineMissing`.
- `TestDoctor_DirNotWritable`.
- `TestDoctor_MasterUnreachable`.
- `TestDoctor_WorkerNotVisible`.
- `TestDoctor_Canary_Fails`.
- `TestDoctor_JSON_StableSchema` (golden vector).
- `TestDoctor_NoSecretsInOutput`.

---

## 6. Evidenze

- Output JSON `doctor-reports/${WORKER_ID}-${TS}.json`.
- Log doctor run con code structured.
- Diff JSON schema vs release version.
- Storico in `ops/doctor-reports/` consultabile via dashboard.
