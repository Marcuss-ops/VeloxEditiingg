# PR-03 — Canonical Attempt identity

> ⚠️ **STATUS (PR-11, 22 giu 2026): CLOSED — no-op di verifica (doc-only closure).**
>
> L'analisi empirica dei file reali (vedi **Appendice A** in
> [PR-11 — Pre-flight empirical reconciliation](./PR-11-pre-flight-empirical-reconciliation.md))
> ha **confutato** la claim §P0.3 dell'audit: nel codice attuale il
> `task_attempts.id` è già generato master-side PRIMA dell'offerta
> e NON è mai derivato dal `lease_id`. Prove in linea:
>
> - `RemoteCodex/native/worker-agent-go/internal/worker/job_executor.go:159`
>   (`submitTaskResult` consuma un `AttemptID` distinto dal `LeaseID`).
> - `DataServer/internal/grpcserver/handler_jobs.go:118` (crea
>   `taskattempts.TaskAttempt.ID = uuid.NewString()` lato master prima
>   di inviare il grant).
> - `DataServer/internal/store/sqlite_task_attempt_repository.go` (la
>   `id` è UUID primario, completamente disaccoppiato da `lease_id`).
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

## Contesto

Nel flusso attuale il `TaskOffer` usa il **lease_id come attempt_id**;
solo successivamente, dopo `TaskAccepted`, il master crea un
`TaskAttempt` con UUID nuovo. Il worker conserva l'Attempt ID
iniziale, mentre il DB ne contiene uno diverso. Conseguenza:

- report non correlabile in modo forte,
- `attempt_id` non è una chiave autorevole,
- validazione report debole (possibile accettazione di report stale).

## Scope

- Generare `Attempt ID` una sola volta, lato master, **prima** dell'invio
  del `TaskOffer`.
- Persistere `TaskAttempt PENDING` con quell'ID prima dell'offerta.
- Inviare `TaskOffer.attempt_id` uguale al DB.
- Validare ogni `TaskResult` richiedendo la coincidenza esatta di
  `task_id`, `attempt_id`, `job_id`, `worker_id`, `lease_id`.

## Files to touch

```text
proto/velox/control/worker_control.proto                  # regen descriptor
velox-server/internal/taskgraph/lifecycle.go              # generazione Attempt ID
velox-server/internal/taskattempts/repository*.go         # persistence PENDING
velox-server/internal/taskgraph/dispatch.go (o simile)    # invio TaskOffer
velox-server/internal/grpcserver/handler_workers.go       # TaskResult validation
RemoteCodex/native/worker-agent-go/internal/worker/worker.go    # consuma Attempt ID
RemoteCodex/native/worker-agent-go/internal/worker/job_executor.go
RemoteCodex/native/worker-agent-go/internal/transport/grpc_stream.go
scripts/gen-proto.sh                                       # rigenerare pb.go
```

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

- [ ] `attempt_id` è generato master-side una sola volta per offerta.
- [ ] `TaskAttempt` esiste con `status=PENDING` **prima** dell'invio del
      `TaskOffer`.
- [ ] Nessun path che calcola `AttemptID = LeaseID`.
- [ ] Validazione `TaskResult` rifiuta report con attempt_id non
      corrispondente al DB.
- [ ] Worker non avvia l'esecuzione prima di `TaskLeaseGranted` (in
      cooperazione con PR-04).

## Test

- **Unit:**
  - `lifecycle_test.go`: il primo `attempt_id` viene persistito prima
    dell'invio.
  - `grpc_stream_test.go` lato worker: report con attempt_id errato
    viene rifiutato.
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

In `check-single-writer.sh` aggiungere la regola che
`AttemptID` istanziato solo da `taskgraph.identity.NewAttemptID()` (o
factory equivalente).

## Rischi

- Worker che aveva historical pending job con vecchio Attempt ID = Lease:
  cleanup migration datata prima del PR (vedi PR-09 per `legacy
  reader only`).
- Report in volo al momento del deploy: la validazione li può scartare;
  è voluto (meglio un retry che un dato inconsistente).

## Out of scope

- Transazione `LEASED → RUNNING + Attempt PENDING → RUNNING` (PR-04).
- Ingestione completa del TaskResult (PR-06).
- Reaper lease scadute (PR-05).
