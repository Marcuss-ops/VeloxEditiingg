# Failure & recovery — Supervisor, recovery, progress e conflict budget

**Capitolo del perimetro architetturale Velox** — raccoglie dal documento indice [`CURRENT-TO-TARGET-ARCHITECTURE.md`](./CURRENT-TO-TARGET-ARCHITECTURE.md) la sezione **14** (Supervisor e readiness), le voci **P0-03** (§25), **P0-04** (§26) e **P1-03** (§31) del piano di intervento.  
**Stato:** gap ancora aperti e target.

> Le garanzie di recovery che la suite P1-03 deve dimostrare sono definite come **Recovery target** in [`target-architecture.md §21`](./target-architecture.md#21-recovery-target).

---

## 14. Supervisor e readiness

Classi:

- `ClassOneShot`;
- `ClassRestartable`;
- `ClassCritical`.

Stati:

```text
STARTING
RUNNING
BACKING_OFF
STOPPED
FAILED
```

### Gap: uscita nil

Un runner permanente che ritorna `nil` con context attivo viene considerato clean exit.

Target:

```text
err == nil
AND ctx non cancellato
AND class != OneShot
    ↓
ErrUnexpectedExit
    ↓
restart o fail-loud
```

### Gap: supervisorDone

La chiusura inattesa dell'intero supervisor deve terminare `runServer` con errore.

### Gap: MaxRetries

Semantica target:

- Restartable + 0 = nessun retry;
- Critical + 0 = infinito;
- OneShot = nessun retry.

### Gap: transport probe

Il probe `transport` placeholder restituisce sempre `nil`.

Deve essere sostituito da un check reale del TransportRegistry.

### Gap: registration warning

Un errore nel registrare una capability obbligatoria deve fallire il bootstrap, non produrre soltanto warning.

---

## 25. P0-03 — Correggere supervisor e readiness

### Azioni

1. introdurre `ErrUnexpectedExit`;
2. `nil` da runner permanente diventa errore;
3. test Restartable/Critical;
4. correggere `MaxRetries=0`;
5. trattare `supervisorDone` inatteso come fatal;
6. sostituire transport placeholder;
7. registration error diventa bootstrap error;
8. collegare `CapabilityRegistry.Readyz` alla health readiness;
9. timeout breve per probe;
10. testare runner death e recovery.

### Accettazione

- runner permanente non sparisce silenziosamente;
- readiness rossa in backoff/failure;
- transport mancante non risulta ready;
- master non serve ready con supervisor morto.

---

## 26. P0-04 — Correggere progress e conflict budget

### Azioni

1. rendere `uploaded_bytes` monotono;
2. testare sequenza `1000 → 800 → 1200`;
3. allineare soglia documentata e codice;
4. rendere budget keyed per operation+commit o contare solo contention infrastrutturale;
5. evitare streak condivise tra commit indipendenti;
6. reset sulla stessa chiave;
7. metriche per conflict, escalation e reset;
8. oldest unresolved commit.

### Accettazione

- progress non regredisce;
- conflitti indipendenti non si sommano;
- soglia implementata uguale alla documentazione;
- escalation osservabile e supervisionata.

---

## 31. P1-03 — Suite recovery

Automatizzare:

- master crash con READY Task;
- master crash durante finalization;
- worker crash prima/dopo accept;
- crash durante render;
- crash durante upload;
- partition corta/lunga rispetto alla lease;
- duplicate TaskResult;
- stale result;
- due worker in race;
- outbox failure dopo business commit;
- forwarding DB failure;
- SIGTERM e drain.

Ogni test verifica SQLite, BlobStore, metriche e artifact.

### Garanzie di recovery che la suite deve dimostrare

Le garanzie dichiarate in [`target-architecture.md §21`](./target-architecture.md#21-recovery-target) — master restart, worker crash, network partition — sono il contratto di accettazione della suite P1-03. Ogni test della suite deve poter essere associato a una di queste garanzie.
