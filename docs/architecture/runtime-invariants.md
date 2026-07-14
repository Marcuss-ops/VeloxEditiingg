# Runtime invariants — Principi architetturali non negoziabili

**Capitolo del perimetro architetturale Velox** — corrisponde alla **sezione 4** del documento indice [`CURRENT-TO-TARGET-ARCHITECTURE.md`](./CURRENT-TO-TARGET-ARCHITECTURE.md).  
**Stato:** invarianti di runtime. Violazioni di una di queste regole sono bloccanti per il merge.

---

## 4.1 Single source of truth

| Tipo di dato | Fonte autoritativa |
|---|---|
| Job, Task, TaskAttempt, lease, forwarding, upload, artifact, outbox e delivery | SQLite tramite repository |
| Video, audio, immagini e blob pesanti | BlobStore/filesystem |
| Configurazione e secret | config validata, env e secret store approvati |
| Cache worker | persistente ma ricostruibile |
| Stato in memoria | proiezione o cache, mai autorità business |
| Versione prodotto | `VERSION.txt` |

Sono vietati:

- JSON locali usati come stato business;
- mappe globali usate come coda autoritativa;
- dual-write tra colonne canoniche e copie JSON mutabili;
- fallback silenziosi verso storage alternativi;
- più percorsi impliciti per il database;
- blob pesanti salvati dentro SQLite.

---

## 4.2 Single writer

Ogni stato importante deve avere:

- un owner;
- un writer;
- una API di mutazione;
- una tabella di transizioni;
- test di invariante full-tree.

Forma obbligatoria:

```text
HTTP/gRPC
    ↓
Application Service
    ↓
Repository / Unit of Work
    ↓
SQLite / BlobStore
```

Forma vietata:

```text
Handler ───────────────► SQL
Background runner ─────► SQL non posseduto
Service A ─────────────► JSON
Service B ─────────────► la stessa riga
Worker ────────────────► reinventa lo stato master
```

---

## 4.3 Registry-first

Nuove capacità devono entrare in un registry, resolver, compiler, estimator o sampler comune.

Sono vietati switch paralleli quando esiste già un registry:

```go
switch videoMode { ... }
switch executorID { ... }
if provider == "x" { ... }
```

---

## 4.4 Fail-closed

Una dipendenza obbligatoria mancante deve:

- impedire il bootstrap; oppure
- rendere readiness falsa; oppure
- produrre un errore tipizzato.

Non deve produrre:

- successo apparente;
- `nil` interpretato come successo per un loop permanente;
- registry vuoto;
- probe placeholder verde;
- downgrade automatico a un percorso legacy;
- log di successo senza commit persistente.

> I gap specifici di supervisor e readiness (che sono il dominio principale del fail-closed operativo) sono trattati in [`failure-recovery.md`](./failure-recovery.md).

---

## 4.5 Idempotenza e fencing

Ogni operazione ripetibile deve restare corretta dopo:

- retry di rete;
- crash del master;
- crash del worker;
- replay gRPC;
- lease scaduta;
- esecuzione concorrente;
- riordino dei messaggi.

Identità minima di un'esecuzione Task:

```text
task_id
attempt_id
worker_id
lease_id
revision
attempt_number, dove previsto
```

Un report non può modificare lo stato se la tupla non corrisponde al tentativo vincente corrente.
