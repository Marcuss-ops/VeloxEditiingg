# PR-12 — Expand single-writer CI guard (already implemented as scan_test.go)

> **Audit anchor:** [§P0.2](../LEGACY_SSOT_AUDIT.md#p02--due-writer-di-jobsstatus--succeeded) + §9.1
> **Target milestone:** cutover P0 (parallelo a PR-02).
> **Branch:** `cutover/pr-12-expand-single-writer-guard`
> **Dipendenze:** PR-11 (per la matrice). PR-02 benefica ma non bloccante.

## Contesto

L'audit §9.1 proponeva di creare una CI guard basata su regex `bash`
per vietare l'occorrenza di `StatusSucceeded` / `'SUCCEEDED'` fuori
dal package artifacts. Tuttavia `DataServer/internal/artifacts/scan_test.go`
ha GIÀ implementato il guard come **test Go nativo** che scansiona
l'albero e rileva `SET status='SUCCEEDED'` con allowlist esplicita
(`sqlite_finalization_repository.go`).

Andare a riscrivere questo in una nuova regex bash significherebbe
**reinventare ciò che esiste** e perdere la compliance semantic-aware
del parser SQL-like.

PR-12 eleva `scan_test.go` a guard canonica ed estende la copertura:

- a tutte le mutazioni su `jobs.status` (non solo `SET status='SUCCEEDED'`),
- a transizioni equivalenti (`jobs.SetStatus(SUCCEEDED)`, `repo.MarkSucceeded`, ecc.),
- all'inserimento in `outbox_events` di tipo `JOB_SUCCEEDED`,
- ai writer su `result_json` che farebbero sembrare un Job SUCCEEDED.

## Scope

- Ampliare il perimetro di `DataServer/internal/artifacts/scan_test.go`
  includendo le transizioni autorizzate di `jobs.status` da parte di:
  - `sqlite_finalization_repository.go` (canonical writer di `SUCCEEDED`),
  - handler di comando `CancelJob` (transizione a `CANCELLED`),
  - lifecycle service per FAILED/RETRY_WAIT.
- Negare tutto il resto.
- Sostituire la sovrapposizione con `scripts/ci/check-single-writer.sh`
  per la parte SUCCEEDED, lasciando bash solo per i pattern non SQL
  (es. import workflow, EXPORT *).
- Aggiungere un test `go test ./internal/artifacts/ -run Scan` che
  fallisce esplicitamente in CI se la regola è violata.

## Files to touch

```text
DataServer/internal/artifacts/scan_test.go
DataServer/internal/jobs/lifecycle_service.go           # transizioni documentate
DataServer/internal/jobs/transitions.go                 # tabella allowed
DataServer/internal/jobs/repository.go                  # firme business
DataServer/internal/grpcserver/handler_jobs.go         # nessun write diretto
DataServer/internal/store/sqlite_jobs_writer.go         # nessun write diretto SUCCEEDED
scripts/ci/check-single-writer.sh                       # delega al Go test per SQL
scripts/ci/lib/diff-scope.sh                            // esclusione mirata
```

## Sequenza operativa

```text
1. CENSIRE tutti i siti esistenti che chiamano jobs.status = SUCCEEDED.
   Atteso: solo DataServer/internal/artifacts/sqlite_finalization_repository.go.
2. Estendere lo scanner in scan_test.go per coprire:
     - UPDATE jobs SET status='SUCCEEDED' (esistente),
     - INSERT INTO jobs (... 'SUCCEEDED' ...)  (casi di bootstrap test only),
     - SetStatus(jobs.StatusSucceeded) al di fuori di authorized packages,
     - OUTBOX events type='JOB_SUCCEEDED' al di fuori di authorized packages,
     - (consentito in sqlite_finalization_repository.go).
3. Aggiungere un checkSetStatusAllowed() che parsa i sorgenti Go e
   segnala chiamate SetStatus(SUCCEEDED) esterne.
4. Cablare il test nella pipeline (`go test ./internal/artifacts/...`)
   come parte del golden E2E.
5. Aggiornare check-single-writer.sh per rimanere SOLO sui pattern
   non-SQL (import workflow, EXPORT, signature lock).
```

## Acceptance criteria

- [ ] `go test ./internal/artifacts/...` esegue lo scanner e fallisce
      su fixtures fasulle.
- [ ] Lo script CI `scripts/ci/check-single-writer.sh` non duplica più
      la parte SQL (delega al Go test).
- [ ] I allowlist sono dichiarati in una sola posizione (top dello
      `scan_test.go`) con motivazione inline.
- [ ] golden E2E rimane verde dopo l'ampliamento.
- [ ] Inserimento di un `UPDATE jobs SET status='SUCCEEDED'` in un
      sito non autorizzato → test fallisce.

## Test

- **Unit (nuovo):** `TestSingleSucceededWriter_AllowsArtifactsRepository`
  + `TestSingleSucceededWriter_RejectsCalendarHandlerLike` (fixture).
- **Integration:** golden E2E con 1 worker e 1 Job che produce una
  finalizzazione artifact; asserisce che il numero di `UPDATE jobs SET
  status='SUCCEEDED'` eseguiti è esattamente 1.
- **Regression:** esecuzione su commit pulito + diff con `git grep` per
  confermare nessun falso positivo.

## CI guards introdotti

Questa PR È l'introduzione/rafforzamento del guard §9.1, ma in forma
Go-native più robusta:

```bash
# go test-based guard eseguito in CI:
go test ./internal/artifacts -run ScanArchitectural -count=1
```

`scripts/ci/check-single-writer.sh` mantiene solo i check non-SQL
(import boundary, lock di simboli).

## Rischi

- Falsi positivi da parte di test fixtures che usano shell statements
  realistiche (es. `calendar_test.go:396`). Lo scanner deve avere
  esclusioni dichiarate (annotazione `// guard-ok` con motivazione).
- Prestazioni: file scan su 100+ sorgenti Go al test time. Stimato:
  < 1 second; confrontabile con golden E2E.

## Out of scope

- Riscrivere `check-single-writer.sh` da zero: solo ridurre duplicazione.
- Ampliare lo scanner a tutti gli `Status`: scope è SOLO
  transizione terminale `SUCCEEDED`. FAILED/CANCELLED hanno regole
  diverse (es. in transazioni) e vanno trattate separatamente in
  una successiva PR-17 (fuori cutover).
