# PR-15 — Payload "parameters" canonicalization (V2 envelope formalization)

> **Audit anchor:** [§P1.3](../LEGACY_SSOT_AUDIT.md#p13--payload-duplicato-top-level-e-parameters) — claim INVERTITA: `parameters` È il canonical, non un mirror.
> **Target milestone:** post-cutover P1.
> **Branch:** `cutover/pr-15-parameters-canonicalization`
> **Dipendenze:** PR-11 (matrice che dichiara P1.3 invertita). **Sostituisce** PR-09.

## Contesto

L'audit §P1.3 afferma che la normalizzazione "continua a scrivere due
copie" dei campi, implicando un dual-write top-level + `parameters`.

L'analisi empirica mostra il contrario:

```text
DataServer/internal/jobs/enqueue/enqueue.go:281
  → normalized["parameters"] = map[string]interface{}{...}

DataServer/internal/handlers/server/calendar/calendar_payload.go:79
  → "parameters": parameters

DataServer/internal/handlers/server/smoke/smoke_clip_stock.go:123
  → normalized["parameters"] = map[string]interface{}{...}

DataServer/internal/assets/asset_service.go:365,387,431,471
  → if params, ok := payload["parameters"].(map[string]interface{})

RemoteCodex/native/worker-agent-go/pkg/api/renderplan/renderplan.go:59
  → m["parameters"] = rp.Parameters
```

Non esiste **un altro** sito che scriva gli stessi campi a top-level.
Il pattern attuale è un envelope singolo: `parameters` È il
canonical, NON un mirror.

PR-09 (la design-doc che avevo originariamente preparato) affronta
il problema come se fosse dual-write. La sua sostanza va rifatta:
invece di **eliminare** `parameters`, va **formalizzato** come
Contract Pattern V2, sanendone l'idiosincrasia invece di negarla.

## Scope

- Definire un DTO tipizzato `JobParameters` in `shared/contract/` o
  in `DataServer/internal/jobs/parameters.go` che sostituisca le
  mappe libere `map[string]interface{}` nei path di scrittura.
- Aggiornare i writer:
  - `enqueue.go` (entry point normalizzazione),
  - `calendar_payload.go` (payload calendar handler),
  - `smoke_clip_stock.go` (smoke test generator),
  - `renderplan.go` (worker-side render plan).
- Aggiornare i reader coerentemente:
  - `asset_service.go` (asset upload/resolution),
  - `worker/job_upload.go` (multipart upload),
  - `worker/job_executor.go` (execution parameter read).
- Aggiungere contract test che serializza un `JobParameters`
  tipizzato e lo parsa, verificando la simmetria.

## Files to touch

```text
shared/contract/contract.go                              # JobParameters DTO (V2)
shared/contract/contract_test.go
DataServer/internal/jobs/parameters.go                   # nuovo (se non internalizzato)
DataServer/internal/jobs/enqueue/enqueue.go              # writer tipizzato
DataServer/internal/handlers/server/calendar/calendar_payload.go
DataServer/internal/handlers/server/smoke/smoke_clip_stock.go
DataServer/internal/assets/asset_service.go              # reader tipizzato
RemoteCodex/native/worker-agent-go/internal/worker/job_upload.go
RemoteCodex/native/worker-agent-go/internal/worker/job_executor.go
RemoteCodex/native/worker-agent-go/pkg/api/renderplan/renderplan.go
shared/validation/validation.go                          # validator
```

## Sequenza operativa

```text
1. Definire JobParameters struct in shared/contract:
     type JobParameters struct {
       Version           int                       `json:"version"`
       AudioLanguageForSrt string                   `json:"audio_language_for_srt,omitempty"`
       OutputPath        string                    `json:"output_path,omitempty"`
       SceneImagePaths   []string                  `json:"scene_image_paths,omitempty"`
       ... // altri campi effettivamente in uso censiti
     }
2. Aggiungere Serializer() / Parse() methods.
3. Sostituire le mappe libere nei 4 writer uno a uno.
4. Sostituire i lettori che fanno `payload["parameters"].(map[string]interface{})`
   con `JobParameters.Parse(rawJSON)`.
5. Contract test:
     - Round-trip Serialize→Parse deve essere idempotente.
     - Schema JSON pubblicato (contract_version=2).
6. Documentare il pattern in OWNERSHIP.md come canonical V2.
```

## Acceptance criteria

- [ ] Nessun path di scrittura usa `map[string]interface{}` per i
      parametri business.
- [ ] Nessun reader fa type-assertion `.(map[string]interface{})`
      sul campo `parameters`.
- [ ] `contract_version=2` diventa il default canonico; `contract_version=1`
      è solo read-only legacy.
- [ ] Contract test round-trip verde.
- [ ] Golden E2E verde: nessun Job viene creato con parametri vuoti
      a causa di parse failure.

## Test

- **Unit:**
  - `parameters_test.go`: Serialize/Parse idempotente.
  - `enqueue_test.go`: writer tipizzato produce `parameters` semanticamente
    identico al precedente ma con DTO.
- **Integration:** end-to-end enqueue → store → read con V2 typed payload.
- **Contract:** `contract_test.go`: schema JSON pubblicato versionato.
- **Regression:** golden E2E con workload reale, atteso stesso
  comportamento del branch corrente.

## CI guards introdotti

In `check-no-legacy.sh`:

```bash
# Vietato l'uso di:
#   normalized["parameters"] = map[string]interface{}{
#   "parameters": map[string]interface{}{...}
# I write devono passare per body tipizzato.
```

(in pratica: questo pattern regex è fragile. Valutare una regola AST.

## Rischi

- Worker datati che leggono V1 mappe libere: vanno disattivati o
  conversion-tolleranti. PR-07 + bump protocollo gestisce questo.
- Refactor di superficie ampia (5+ file). Stile simile a PR-08.

## Out of scope

- Definire schemi specifici per ogni job type (rinviato a oltre DoD).
- Crittografia selettiva dei parametri.
- Versioning automatico del contratto.

---

> [!NOTE]
> PR-15 è il redesign fedele al codice reale di quello che l'audit
> chiamava "PR 9 — Payload V2 single shape". Se PR-09 è già stata
> aperta con scope sbagliato, PR-15 la assorbe e ridenomina.
