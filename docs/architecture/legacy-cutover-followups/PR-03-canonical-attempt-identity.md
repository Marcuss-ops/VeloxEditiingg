# PR-03 — Canonical Attempt identity

> ⚠️ **STATUS (PR-11, 22 giu 2026): CLOSED — no-op di verifica (doc-only closure).**
>
> L'analisi empirica dei file reali (vedi **Appendice A** in
> [PR-11 — Pre-flight empirical reconciliation](./PR-11-pre-flight-empirical-reconciliation.md))
> ha **confutato** la claim §P0.3 dell'audit: nel codice attuale il
> `task_attempts.id` è già generato master-side PRIMA dell'offerta
> e NON è mai derivato dal `lease_id`. Prove in linea:
>
> - `RemoteCodex/native/worker-agent-go/internal/worker/task_result_builder.go:49`
>   (`submitTaskResult` consuma un `AttemptID` distinto dal `LeaseID`).
> - `DataServer/internal/store/sqlite_task_atomic.go:128-181`
>   (`ClaimNextWithAttemptAtomic` genera l'identità e persiste il
>   `TaskAttempt PENDING` prima dell'offerta).
> - `DataServer/internal/store/sqlite_task_lease_claim.go`
>   (`ClaimTaskForWorkerAtomic` applica lo stesso contratto al placement
>   specifico).
>
> **Nessuna modifica di codice è richiesta per chiudere §P0.3.**
> La presente PR è esclusivamente documentale: il design sotto è
> preservato come record storico della claim originale (formulazione
> pre-analisi-empirica), ma la tabella di marcia è aggiorata a
> *no-op*. Per la matrice completa di copertura audit → codice,
> riferimento obbligatorio all'Appendice A in PR-11.
>
> **Convenzione di chiusura:** questa è una PR puramente
> documentale (`docs-only`). Non richiede code-review né guarda CI
> perché non tocca codice, sql, proto, o scripts: ha il solo scopo
> di lasciare in `git log` il tracciato della claim originale così
> che chi legge `git blame` trovi immediatamente l'evidenza della
> confutazione. Il commit message porterà il prefisso
> `docs(cutover):` per distinguerlo dai commit di codice delle
> altre PR del cutover.

> **Audit anchor:** [§P0.3](../LEGACY_SSOT_AUDIT.md#p03--attempt-id-doppio)
> **Target milestone:** cutover P0
> **Branch:** `cutover/pr-03-canonical-attempt-identity`
> **Dipendenze:** nessuna (chiude P0.3 prima di PR-04 atomic acceptance).

## Contesto storico

Il flusso pre-PR-2 poteva confondere `lease_id` e `attempt_id` oppure creare
l'Attempt solo dopo `TaskAccepted`. Il percorso corrente su `main` non lo fa:
`attempt_id` è generato dal Master durante il claim, viene scritto su `tasks`,
viene usato per il `TaskAttempt PENDING` e viene inviato nel `TaskOffer`. Il campo
canonico di assegnazione è `tasks.worker_id`/`task_attempts.worker_id`;
`assigned_worker_id` non è una colonna o un modello persistente corrente.

Il worker conserva e riusa quell'identità; il Master valida la tupla completa
prima di accettare TaskAccepted e TaskResult.

## Scope

- Generare `Attempt ID` una sola volta, lato master, **prima** dell'invio
  del `TaskOffer`.
- Persistere `TaskAttempt PENDING` con quell'ID prima dell'offerta.
- Inviare `TaskOffer.attempt_id` uguale al DB.
- Validare ogni `TaskResult` richiedendo la coincidenza esatta di
  `task_id`, `attempt_id`, `job_id`, `worker_id`, `lease_id`.

## Files and symbols verified on `main`

Il percorso corrente è già implementato in questi punti:

```text
DataServer/internal/store/sqlite_task_atomic.go
  ClaimNextWithAttemptAtomic

DataServer/internal/store/sqlite_task_lease_claim.go
  ClaimTaskForWorkerAtomic

DataServer/internal/grpcserver/handler_placement.go:157
  sendClaimedTaskOffer

DataServer/internal/grpcserver/handler_accept.go:39
  handleTaskAccepted

DataServer/internal/grpcserver/handler_result.go:42
  handleTaskResult

RemoteCodex/native/worker-agent-go/internal/worker/task_result_builder.go:49
  submitTaskResult
```

Il protobuf non richiede una modifica per questo contratto: `TaskOffer` e
`TaskResult` trasportano già `attempt_id`; il codice generato in
`shared/controltransport/pb/worker_control.pb.go` non va modificato
manualmente. Se in futuro si modifica lo schema wire, la sorgente è
`shared/controltransport/proto/velox/control/worker_control.proto` e la
rigenerazione segue `scripts/gen-proto.sh`.

## Sequenza operativa

```text
1. Claim Task:
     - master genera attempt_id = uuid.NewString() (o formato canonico),
     - master persiste TaskAttempt con status=PENDING + attempt_id,
     - master invia TaskOffer{ attempt_id, task_id, worker_id, lease_id,
       attempt_number }.
2. Worker riceve. Memorizza attempt_id identico al DB.
3. Worker invia TaskAccepted.
4. Master transizione (PR-04): Task LEASED → RUNNING + TaskAttempt PENDING → RUNNING
   in un'unica transazione.
5. Master invia TaskLeaseGranted{ attempt_id, lease_id }.
6. Worker avvia l'esecuzione. Tutti i successivi report portano
   attempt_id coerente con DB.
7. Validazione TaskResult: deve combaciare (task_id, attempt_id, job_id,
   worker_id, lease_id). Se uno qualunque non coincide, SCARTARE.
```

## Acceptance criteria

- [x] `attempt_id` è generato master-side una sola volta per offerta.
- [x] `TaskAttempt` esiste con `status=PENDING` **prima** dell'invio del
      `TaskOffer`.
- [x] Nessun percorso canonico calcola `AttemptID = LeaseID`.
- [x] Validazione `TaskResult` rifiuta report con attempt_id non
      corrispondente al DB.
- [x] Worker non avvia l'esecuzione prima di `TaskLeaseGranted`.

## Test

- **Unit:**
  - test del repository `ClaimNextWithAttemptAtomic` /
    `ClaimTaskForWorkerAtomic`: il primo `attempt_id` viene persistito prima
    dell'invio;
  - `handler_task_identity_test.go` e i test di report identity: un report
    con attempt_id errato viene rifiutato.
- **Integration:**
  - end-to-end: claim → TaskOffer → Accepted → LeaseGranted → TaskResult
    valido → SUCCEEDED.
  - report con `(attempt_id, task_id, worker_id, lease_id)` non matching
    deve essere respinto; nessuna transizione di stato.
- **Race:** due report identici arrivano in concorrenza → atteso un solo
  successo, uno scarto idempotente.

## CI guards introdotti

```bash
# scripts/ci/check-no-legacy.sh — piena alberatura
# Vietato: AttemptID\s*[:=]\s*leaseID  (case-insensitive)
# Vietato: attempt_id\s*[:=]\s*lease_id
```

Il guard deve continuare a vietare la derivazione di `AttemptID` da
`LeaseID` e deve mantenere `ClaimNextWithAttemptAtomic` /
`ClaimTaskForWorkerAtomic` come punti di generazione master-side. Non esiste
un requisito di introdurre una factory nominale non presente nel percorso
corrente.

## Rischi

- Report o offerte legacy ancora in volo al momento del deploy: la validazione
  può scartarli; è voluto (meglio un retry che un dato inconsistente).
- `task_attempts.started_at` deve essere valorizzato nel percorso
  `AcceptTaskAtomic`; il gap è tracciato in PR-04.
- Report in volo al momento del deploy: la validazione li può scartare;
  è voluto (meglio un retry che un dato inconsistente).

## Out of scope

- Verifica documentale e test aggiuntivi su `task_attempts.started_at` nel
  percorso `AcceptTaskAtomic` (tracciati in PR-04).
- Ingestione completa del TaskResult (PR-06).
- Reaper lease scadute (PR-05).
