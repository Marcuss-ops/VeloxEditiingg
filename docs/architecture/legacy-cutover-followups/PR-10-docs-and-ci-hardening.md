# PR-10 — Documentation and CI hardening

> **Audit anchor:** [§P1.7](../LEGACY_SSOT_AUDIT.md#p17--documentazione-non-sincronizzata) + §9 intero (CI guards).
> **Target milestone:** chiusura cutover.
> **Branch:** `cutover/pr-10-docs-and-ci-hardening`
> **Dipendenze:** PR-01 → PR-09 tutte merged (sequenziale).

## Contesto

L'audit elenca 6 guard da aggiungere (§9.1–9.6) e un bug di
documentazione (§P1.7): `OWNERSHIP.md` e diversi file di bootstrap
descrivono ancora il codice PRE-cutover (workflow esistente, costmodel
duplicato, Enqueuer basato su JobQueue, requirements duplicati in JSON,
handler workflow no-op).

## Scope

- Aggiornare `docs/architecture/OWNERSHIP.md` rimuovendo/i marcando
  come `DECOMMISSIONING` ogni riferimento che non è più vero.
- Aggiornare commenti di:
  - `velox-server/cmd/server/bootstrap_assets.go`,
  - `velox-server/internal/jobs/lifecycle_service.go`,
  - ogni altro file ancora fuorviante.
- Raggiungere i 6 guard dell'audit (§9.1–9.6) come script
  CI eseguibili in `.github/workflows/ci.yml`.
- Aggiungere un invariant test (§9.5, §9.6) alla CI.

## Files to touch

```text
docs/architecture/OWNERSHIP.md
docs/architecture/LEGACY_SSOT_AUDIT.md
docs/architecture/legacy-cutover-followups/README.md
docs/operations/01-workflow-taskgraph-cutover.md
velox-server/cmd/server/bootstrap_assets.go
velox-server/internal/jobs/lifecycle_service.go
scripts/ci/check-single-writer.sh
scripts/ci/check-no-legacy.sh
scripts/ci/check-architecture.sh
.github/workflows/ci.yml
.github/workflows/golden-e2e.yml
scripts/ci/lib/diff-scope.sh
```

## Sequenza operativa

### §9.1 — Unico writer `SUCCEEDED`

```bash
# scripts/ci/guards/guard_single_succeeded_writer.sh
# Fail se fuori da velox-server/internal/artifacts/ trovo:
#   StatusSucceeded | 'SUCCEEDED' | "SUCCEEDED"
#   SET status = 'SUCCEEDED' | SET status="SUCCEEDED"
#   in contesti UPDATE jobs, INSERT INTO jobs, SetStatus(...).
```

### §9.2 — Nessun runtime Job lease

```bash
# scripts/ci/guards/guard_no_job_runtime_lease.sh
# Fail se nel tree (escluso worker stub di test marcati) trovo:
#   ClaimNext( | ClaimNextForProfile( | RenewLease(
#   RecordRenderFinished( | JobOffer | JobLeaseGranted | JobResult
# dopo il tag di cutover-completed.
```

### §9.3 — Nessun payload mirror

```bash
# scripts/ci/guards/guard_payload_no_mirror.sh
# Fail se in un'unica funzione vedo due scritture:
#   payload["X"] = ... e payload["parameters"]["X"] = ...
# in scrittori di request_json / params.
```

### §9.4 — Nessun Attempt ID derivato dal lease

```bash
# scripts/ci/guards/guard_attempt_id_not_lease.sh
# Fail se trovo assegnazioni del tipo:
#   AttemptID = leaseID | AttemptID: leaseID | attempt_id = lease_id
```

### §9.5 — Nessuna Task RUNNING senza Attempt

```sql
-- scripts/ci/invariants/invariant_task_running_has_attempt.sql
SELECT COUNT(*) FROM tasks t
LEFT JOIN task_attempts a
  ON a.task_id = t.task_id AND a.status = 'RUNNING'
WHERE t.status = 'RUNNING' AND a.id IS NULL;
-- Il test deve ritornare sempre 0.
```

### §9.6 — Nessuna lease attiva senza expiry

```sql
-- scripts/ci/invariants/invariant_active_lease_has_expiry.sql
SELECT COUNT(*) FROM tasks
WHERE status IN ('LEASED','RUNNING')
  AND (worker_id IS NULL OR worker_id = ''
    OR lease_id IS NULL OR lease_id = ''
    OR lease_expires_at IS NULL OR lease_expires_at = ''
    OR NOT EXISTS (
      SELECT 1 FROM task_attempts a
       WHERE a.task_id = tasks.id
         AND a.status IN ('RUNNING','PENDING')
    ));
-- Il test deve ritornare sempre 0.
```

### Documentazione

- Aggiornamento `OWNERSHIP.md` con sezione "DECOMMISSIONED":
  - **REMOVED (DECOMMISSIONING — PR 01)**: `internal/workflow` package.
  - **NO-OP HANDLERS REMOVED (PR 02)**: `StepReadyHandler`,
    `JobSucceededHandler`, `ArtifactReadyHandler`,
    `DeliveryCreatedHandler`.
  - **JOB RUNTIME REMOVED (PR 07)**: JobOffer/Accepted/Rejected,
    LeaseGranted, Result, Progress, LeaseRenewal.
- Aggiornamento `docs/operations/01-workflow-taskgraph-cutover.md`
  con sezione finale "Cutover status" che linka a questo README e ai
  10 PR.
- Aggiornamento dei commenti dentro i file bootstrap e
  `lifecycle_service.go` con rimozione di frasi come
  "If JobQueue pool is active, switch …" rimaste da refactor
  precedenti.

## Acceptance criteria

- [ ] Tutti i 6 guard sono in `scripts/ci/guards/` ed eseguiti in CI.
- [ ] I due invariant test (§9.5, §9.6) sono integrati nei golden E2E
      e girano su DB di test.
- [ ] `OWNERSHIP.md` non cita più `internal/workflow` se non nella
      sezione DECOMMISSIONED.
- [ ] Bootstrap, lifecycle e ogni commento fuorviante è aggiornato.
- [ ] `check-architecture.sh` non segnala path duplicati.

## Test

- **Guard-rules smoke:** per ogni nuovo `guard_*.sh`, eseguire con
  fixtures positive (devono passare) e negative (devono fallire).
- **Invariants:** i due SQL sopra passati come test integration in
  golden-e2e.
- **Doku cross-link:** link-checker che fallisce se un anchor del
  audit punta a file inesistenti.

## CI guards introdotti

Questa PR **è** la PR di introduzione dei guard.

## Rischi

- Alcuni guard statici possono produrre falsi positivi se applicati a
  test fixtures. Gestione via diff-scope.sh (esclude `docs/**` e
  alcuni file di test marcati `// guard-ok`).
- Golden E2E ciclica lentamente per via dei due invariant SQL.
  Cache-friendly se eseguiti solo in master e su PR labelled
  `cutover/*`.

## Out of scope

- Rimozione totale dei file di test marcati `// guard-ok`
  (cleanup post cutover, oltre DoD).
- Conversione dei guard in Go AST invece di regex (refactor post
  cutover).

---

> [!NOTE]
> Questa PR chiude la **Definition of Done §10** di `LEGACY_SSOT_AUDIT.md`.
> Dopo il merge, aggiornare in audit:
> - §11: "Cutover status" → "Taglia cutover completato",
> - Patchare il *header*: `Stato generale: cutover completato il <data>`.
