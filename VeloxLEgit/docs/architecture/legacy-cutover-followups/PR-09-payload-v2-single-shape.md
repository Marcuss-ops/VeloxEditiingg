# PR-09 — Payload V2 single shape

> **Audit anchor:** [§P1.3](../LEGACY_SSOT_AUDIT.md#p13--payload-duplicato-top-level-e-parameters)
> **Target milestone:** post-cutover P1.
> **Branch:** `cutover/pr-09-payload-v2`
> **Dipendenze:** PR-02 (single SUCCEEDED writer — la pulizia del
> payload non deve mai reintrodurre doppi writer).

## Contesto

La normalizzazione continua a scrivere due copie di questi campi:

```text
job_id, job_run_id, correlation_id, job_type,
video_name, script_text, scenes, voiceover_paths,
priority, timeout_secs
```

Una a top-level del payload, una dentro `parameters`. Conseguenza:
lettori e validatori divergono, normalization crea superfici di errore.

## Scope

- Introdurre envelope V2:

```json
{
  "contract_version": 2,
  "payload": {
    "video_name": "...",
    "script_text": "...",
    "scenes": [],
    "voiceover_paths": []
  }
}
```

- I seguenti dati **NON** devono stare nel payload:
  `job_id, task_id, attempt_id, executor_id, executor_version,
  worker_id, lease_id, priority, requirements, status`.
- Eliminare il mirror `parameters`. Ogni campo business è in
  `payload` una sola volta.
- Lettore legacy (per dati storici) resta solo in modalità
  **read-only**.
- Aggiungere test che dimostrino l'assenza di dual-write.

## Files to touch

```text
velox-server/internal/payload/payload.go
shared/payload/payload.go
velox-server/internal/store/mapper_jobs.go (o equivalente)
velox-server/internal/jobs/normalization.go (se esiste)
velox-server/internal/grpcserver/handler_artifacts.go
velox-server/internal/grpcserver/handler_jobs.go
velox-server/internal/handlers/server/orchestratorv1/*.go
shared/contract/contract.go
shared/contract/contract_test.go
shared/validation/validation.go
```

## Sequenza operativa

```text
1. Definire envelope PayloadV2 in shared/payload/payload.go:
     type PayloadV2 struct {
       ContractVersion int       `json:"contract_version"`
       Payload         SubPayload `json:"payload"`
     }
2. Aggiungere SubPayload con i soli campi business.
3. Migration: nei nuovi record job.request_json = envelope V2.
4. Lettore legacy: normalisation di request_json con version ==
   "v1" o assente → NON supportato in scrittura, solo in lettura.
   Tutti i path let-only sono coperti da test.
5. Verifica assenza di dual-write: integration test che crea un Job
   e legge request_json asserendo che non esistono chiavi top-level
   `parameters.X` né `parameters` intero con sottochiavi duplicate.
```

## Acceptance criteria

- [ ] Nessun path di scrittura popola `payload["parameters"]["X"]`
      mentre popola anche `payload["X"]` per uno qualunque dei campi
      elencati.
- [ ] Lettori accettano `contract_version=2` (o assente, trattato
      legacy). Writer producono solo V2.
- [ ] Test `payload_v2_no_dual_write_test` verde.
- [ ] Documentazione API `docs/api/workers.md` aggiornata.
- [ ] CI guard §9.3 dell'audit passa.

## Test

- **Unit:**
  - `payload_test.go`: serializzazione/deserializzazione V2.
  - `normalization_test.go`: nessun dual-write.
- **Integration:** end-to-end enqueue → store → read con V2.
- **Architectural:** static scan via `check-no-legacy.sh` e
  `check-single-writer.sh` esteso.

## CI guards introdotti

In `check-no-legacy.sh` (full-tree):

```text
# Vietata la creazione di chiavi top-level:
#   payload["parameters"] insieme a payload[stesso campo].
# Pattern da intercettare:
#   parameters.video_name, parameters.script_text, ...,
#   video_name: ..., parameters: { video_name: ... } nella stessa unit.
```

In pratica: rileva coppie di assegnazioni che avvengono sullo stesso
record (mock static scan; il robust pattern richiede un parser
contestuale, implementato in check-single-writer.sh come regola
dedicata).

## Rischi

- Worker datati che leggono V1: sono già disattivati da PR-07
  (protocollo Job rimosso). Resta il rischio di un reader legacy
  per ID, gestito in read-only mode.
- Performance: una normalizzazione più ricca può essere più lenta.
  Misurare nel golden E2E.

## Out of scope

- Crittografia selettiva del payload.
- Compressione del blob (deferred).
- Refactor del contratto Estimator / costmodel (PR oltre DoD).
