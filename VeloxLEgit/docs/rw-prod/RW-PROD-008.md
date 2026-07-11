# RW-PROD-008 — Integrità artifact e finalizzazione

**Priorità:** P0
**Dipendenze:** RW-PROD-007
**Stato attuale:** `DataServer/internal/artifacts/sqlite_finalization_repository.go` `FinalizeVerified` esegue jobs/artifacts/attempts in **single tx** con CAS. SHA-256 calcolato master-side in `service_receive.go` e verificato in `service_finalize.go`. UNIQUE `(storage_provider, storage_key)` impedisce doppio READY. `internal/artifacts/scan_test.go` invariante `jobs.completed_at >= artifacts.verified_at`. **Gap**: late-report da attempt stale va verificato nel contratto, idempotency-key su FinalizeVerified, ffprobe post-finalize nell'ambito E2E.

---

## 1. Pain points

1. **`FinalizeVerified` non idempotente.** Se il client (worker) reinvia `MsgFinalizeVerified` con stesso upload_id dopo un retry, comportamento attuale? Verificare `Internal/.../service_finalize.go:166` — fa CAS `FINALIZING → COMPLETED`; retry idempotente se già terminal. Garantire con test.
2. **Report tardivo da attempt scaduto:** lo `task_attempts` ha `attempt_number` + revision. Se un worker zombie ri-invia TaskResult, il CAS su jobs/artifacts fallisce. Confermare con un test esplicito `TestFinalize_LateReportRejected`.
3. **`ffprobe` post-finalize non è parte del gate `Job=SUCCEEDED`.** E2E workload-mtls lo verifica al test, ma il codice master (`jobs.CAS SUCCEEDED`) si fida solo dei flag dell'upload. Aggiungere check `ffprobe` opzionale ma tracked.
4. **Size mismatch:** `ErrSizeMismatch` esiste in `errors.go`, ma il worker invia `size` stimato (header chunked)? — verificare.

---

## 2. Soluzione

1. **Rendere `FinalizeVerified` idempotente**:
   - Se al primo step (CAS RECEIVED → FINALIZING) la riga è già `COMPLETED`, ritornare nil + log `finalize_replay_idempotent`. No throw.

2. **Late report rejected test**:
   - In `internal/artifacts/sqlite_finalization_repository.go` inserire test che pre-imposta `task_attempts.status='EXPIRED'`, tenta FinalizeVerified, verifica `ErrAttemptMismatch`.

3. **`ffprobe` post-finalize opzionale**:
   - Aggiungere in `service_finalize.go` step opzionale (env `VELOX_FFPROBE_VERIFY_ON_FINALIZE=true`): chiama `ffprobe.duration` sul blob, parse intero ≥ 0. In `tests/e2e/workload-mtls/run.sh` la verifica ffprobe è già presente.

4. **Size/hash mismatch test**:
   - `TestReceive_HashMismatch_ReturnsErrHashMismatch` (worker invia hash, master recalcola, fail). `TestReceive_SizeMismatch_*`.

---

## 3. Azioni concrete

| # | File:line | Azione |
|---|-----------|--------|
| A1 | `DataServer/internal/artifacts/service_finalize.go` `Finalize` post CAS | Se upload già `COMPLETED`, ritorno nil con log `finalize_replay`. |
| A2 | `DataServer/internal/artifacts/sqlite_finalization_repository.go` | Test `TestFinalize_LateReportRejected` con attempt EXPIRED. |
| A3 | `DataServer/internal/artifacts/service_receive.go` | Già verifica SHA-256 master-side. Aggiungere test `TestReceive_HashMismatch_ReturnsErrHashMismatch`. |
| A4 | `DataServer/internal/artifacts/service_finalize.go` | Se `VELOX_FFPROBE_VERIFY_ON_FINALIZE=true`, chiamare `ffprobe` shell-out su blob, parse JSON. |
| A5 | `scripts/ci/check-architecture.sh` | Aggiungere guard: niente "size optional" o "sha optional" su receive/finalize. |
| A6 | `internal/artifacts/errors.go` | Doc comment che enumera tutti gli errori canonical. |
| A7 | `tests/e2e/workload-mtls/run.sh` | Verifica ffprobe già presente (assert `duration > 0`). |
| A8 | `internal/artifacts/reconciler.go` | Verifica rule 4 (stuck STAGING → FAILED) — docs "ffprobe rigida per fixture E2E" non significa obbligo master-side, ma E2E. |

---

## 4. Criteri di accettazione

- [ ] Hash DB uguale al file reale (`SELECT sha256 FROM artifacts == sha256sum blob`).
- [ ] `jobs.completed_at >= artifacts.verified_at` (invariante scan_test.go).
- [ ] Upload corrotto non finalizza Job.
- [ ] Report duplicato non produce doppio artifact READY.
- [ ] Attempt stale (EXPIRED/TIMED_OUT) non sovrascrive il vincitore.
- [ ] `FinalizeVerified` su upload già terminal → nil idempotente.

---

## 5. Test obbligatori

- `TestFinalize_DoubleReplay_Idempotent`.
- `TestFinalize_LateReportFromExpiredAttempt_Rejected`.
- `TestReceive_HashMismatch` (ErrHashMismatch).
- `TestReceive_SizeMismatch` (ErrSizeMismatch).
- `TestReceive_InterruptedUpload_DoesNotFinalize`.
- `TestE2E_FFProbeValidOnArtifact` (vedi script esistente, asserzione su `duration > 0`).

---

## 6. Evidenze

- Log structured `event=artifact.finalize.status` con `attempt_id`, `upload_id`, `worker_id`, hash.
- Report query `SELECT * FROM artifacts WHERE sha256 != (calcolato)` deve sempre essere vuoto.
- E2E test run `make e2e-workload-mtls` con V3/ffprobe asserts.
