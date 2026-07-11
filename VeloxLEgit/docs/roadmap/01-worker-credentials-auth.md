# 01 — Persistent Worker Credentials in Auth Flow

## Stato attuale

La tabella `worker_credentials` esiste in SQLite (migration `020_worker_control_plane.sql`) e il package `store` espone
`SetWorkerCredential()` e `ValidateWorkerCredential()` in `store_worker_control.go:331-355`.

**Tuttavia, queste funzioni non vengono mai chiamate.** Il flusso di registrazione attuale:

- **Legacy `RegisterHandler`** (`lifecycle/registration.go:168-169`): genera un session token temporaneo
  (`h.tokenMgr.GenerateToken()`), revoca sessioni precedenti, ma **non chiama mai `SetWorkerCredential`**.
- **V2 `RegisterV2Handler`** (`lifecycle/registration.go:16-107`): non genera nemmeno il token, men che meno
  persiste una credential.

Il risultato: l'identità del worker non ha una radice persistente in SQLite. Un restart del master perde
la capacità di riconoscere il worker oltre la validità del session token (1 ora).

## Stato target

1. Durante la registrazione (sia legacy che V2), il master **persiste un credential hash** del worker
   tramite `SetWorkerCredential(workerID, hash)`.
2. Il credential hash è derivato da un **secret condiviso** (es. `worker_id + pre-shared key` inviato
   dal worker durante la registrazione).
3. Il worker invia il credential in ogni richiesta di registrazione (campo `credential` o simile).
4. Il master valida la credential contro `worker_credentials` prima di generare un session token.
5. Se la credential non esiste (prima registrazione), viene creata.
6. Se esiste e matcha, il worker è riconosciuto.
7. Se esiste e NON matcha, la registrazione è rifiutata (possibile impersonificazione).

## File coinvolti

| File | Azione |
|---|---|
| `DataServer/internal/handlers/remote/workers/lifecycle/registration.go` | Modificare: aggiungere credential handling in `RegisterHandler` e `RegisterV2Handler` |
| `DataServer/internal/store/store_worker_control.go` | Nessuna modifica (già pronto) |
| `RemoteCodex/native/worker-agent-go/pkg/api/api_types.go` | Modificare: aggiungere campo `Credential` a `WorkerInfo` |
| `RemoteCodex/native/worker-agent-go/internal/worker/worker_comms.go` | Modificare: `register()` invia la credential |
| `RemoteCodex/native/worker-agent-go/pkg/config/config.go` | Modificare: aggiungere `WorkerSecret` alla config |
| `RemoteCodex/native/worker-agent-go/cmd/velox-worker-agent/main.go` | Modificare: leggere `VELOX_WORKER_SECRET` da env |

## Definition of Done

- [ ] `WorkerInfo` nel worker include campo `Credential string` (hash del secret)
- [ ] `WorkerConfig` ha campo `WorkerSecret` popolato da env `VELOX_WORKER_SECRET`
- [ ] `register()` nel worker calcola `credential = SHA256(workerID + ":" + workerSecret)` e lo invia
- [ ] `RegisterHandler` (legacy) chiama `SetWorkerCredential(workerID, credentialHash)` dopo `RegisterWorker`
- [ ] `RegisterV2Handler` chiama `SetWorkerCredential(workerID, credentialHash)` dopo `RegisterWorker`
- [ ] `RegisterHandler` (legacy) valida credential esistente prima di generare token:
  - Se credential esiste e matcha → procedi con token
  - Se credential esiste e NON matcha → **rifiuta** (401)
  - Se credential non esiste (prima registrazione) → creala e procedi
- [ ] `RegisterV2Handler` stesso comportamento di validazione
- [ ] Test: worker con credential corretta → registrato e token generato
- [ ] Test: worker con credential errata → 401 Unauthorized
- [ ] Test: nuovo worker (nessuna credential preesistente) → credential creata e token generato

## Criteri di test

```bash
# Master side
cd refactored/DataServer && go test ./internal/handlers/remote/workers/lifecycle/... -v -run TestCredential

# Worker side
cd refactored/RemoteCodex/native/worker-agent-go && go test ./internal/worker/... -v -run TestCredential
```

## Dipendenze

- Nessuna — indipendente da tutti gli altri task.
- È un **prerequisito** per 13 (mTLS), dove la credential diventa il client certificate.
