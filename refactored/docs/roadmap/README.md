# Velox Control Plane — Roadmap verso lo stato target

Questo documento indicizza le Definition of Done per ogni componente mancante
necessario a raggiungere l'architettura target descritta nel documento
`velox-communication-analysis.md`.

## Stato attuale vs target

```
Oggi:                          Target:
  HTTP polling indipendenti    →  gRPC bidirectional stream
  Control plane non persistente → SQLite-backed control plane
  Token/session in memoria     →  Persistent credentials + sessions
  ACK per tipo comando         →  ACK per command_id con sequence
  Re-registration manuale      →  Re-registration automatica con backoff
  Single-job worker            →  Multi-job con activeJobs map
```

## Fasi

### Phase 1 — Completamento controllo persistente (prima del gRPC)

| # | Documento | Cosa manca | Priorità |
|---|---|---|---|
| 01 | [worker-credentials-auth](01-worker-credentials-auth.md) | Integrare `worker_credentials` in SQLite nel flusso di auth | 🔴 Critica |
| 02 | [command-ack-by-id](02-command-ack-by-id.md) | Worker: inviare ACK per `command_id`, non per tipo | 🔴 Critica |
| 03 | [mark-command-delivered](03-mark-command-delivered.md) | Chiamare `MarkCommandDelivered` quando i comandi sono inviati | 🟡 Alta |
| 04 | [re-registration-loop](04-re-registration-loop.md) | Loop automatico di re-registration con backoff | 🔴 Critica |
| 05 | [multi-job-active-jobs](05-multi-job-active-jobs.md) | `currentJob` → `activeJobs` map per supporto multi-job | 🟡 Alta |

### Phase 2-3 — ControlTransport + gRPC opzionale

| # | Documento | Cosa manca | Priorità |
|---|---|---|---|
| 06 | [control-transport-interface](06-control-transport-interface.md) | Interfaccia `ControlTransport` comune | 🔴 Critica |
| 07 | [grpc-protobuf](07-grpc-protobuf.md) | Definizioni `.proto` per il control plane | 🔴 Critica |
| 08 | [grpc-stream-transport](08-grpc-stream-transport.md) | Implementazione `GRPCStreamTransport` | 🟡 Alta |
| 09 | [polling-http-transport](09-polling-http-transport.md) | Wrap del polling HTTP in `PollingHTTPTransport` | 🟡 Alta |
| 10 | [worker-control-transport-refactor](10-worker-control-transport-refactor.md) | Refactor worker per usare `ControlTransport` | 🟡 Alta |

### Phase 4-6 — Shadow mode, push, mTLS, eliminazione polling

| # | Documento | Cosa manca | Priorità |
|---|---|---|---|
| 11 | [shadow-mode](11-shadow-mode.md) | Shadow mode: stream + polling paralleli | 🟢 Media |
| 12 | [job-push](12-job-push.md) | Push reale dei job via stream (`JobOffer`) | 🟢 Media |
| 13 | [mtls](13-mtls.md) | mTLS per autenticazione del control plane | 🟢 Media |
| 14 | [polling-removal](14-polling-removal.md) | Rimozione graduale degli endpoint HTTP legacy | 🟢 Bassa |

## Dipendenze

```
Phase 1 (indipendenti tra loro, tutte prerequisite per Phase 2)
  ├── 01 · 02 · 03 · 04 · 05
  │
Phase 2-3
  ├── 06 (dipende da: nessuna — è pura interfaccia)
  ├── 07 (dipende da: nessuna — è pura definizione)
  ├── 08 (dipende da: 06, 07)
  ├── 09 (dipende da: 06)
  └── 10 (dipende da: 06, 08, 09)

Phase 4-6
  ├── 11 (dipende da: 06, 08, 09, 10)
  ├── 12 (dipende da: 11)
  ├── 13 (dipende da: 08)
  └── 14 (dipende da: 12)
```

## Convenzioni

Ogni documento MD segue questa struttura:

1. **Stato attuale** — cosa c'è oggi nel codice
2. **Stato target** — cosa vogliamo ottenere
3. **File coinvolti** — percorso dei file da modificare/creare
4. **Definition of Done** — checklist verificabile
5. **Criteri di test** — come verificare che sia fatto
6. **Dipendenze** — cosa deve esistere prima

## Riferimenti

- Documento originale: `velox-communication-analysis.md`
- Codice master: `refactored/DataServer/`
- Codice worker: `refactored/RemoteCodex/native/worker-agent-go/`
- Store SQLite: `refactored/DataServer/internal/store/`
- Migrations: `refactored/DataServer/internal/store/migrations/`
