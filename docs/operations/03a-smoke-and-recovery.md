# Runbook operativo 03a — Smoke test e recovery

Status: operativo

Data snapshot: 2026-06-21

Document set: [03 — Build e CI](03-build-deploy-and-ci-hardening.md) · **03a — Smoke e recovery** · [03b — Governance, release e deploy](03b-governance-release-and-operations.md) · [03c — Rollout e accettazione](03c-rollout-and-acceptance.md).

## 7. Smoke test master-worker end-to-end

Il release gate deve verificare il percorso reale, non soltanto che i container si avviino.

### 7.1 Ambiente smoke

Avviare:

- master con SQLite temporaneo;
- blobstore filesystem temporaneo;
- un worker CPU;
- un asset di test piccolo;
- un Job/Task deterministico con durata breve.

Usare Docker Compose dedicato, per esempio:

```text
deploy/smoke/compose.yml
scripts/ci/smoke-master-worker.sh
```

Il compose non deve contenere segreti reali.

### 7.2 Sequenza test

1. Avviare master.
2. Attendere readiness.
3. Avviare worker.
4. Verificare registrazione e heartbeat.
5. Sottomettere un Job di test.
6. Verificare creazione TaskGraph.
7. Verificare claim Task.
8. Verificare TaskAttempt RUNNING.
9. Attendere artifact output.
10. Verificare hash e finalizzazione.
11. Verificare Task e Job SUCCEEDED.
12. Riavviare master e confermare che lo stato resti leggibile.
13. Spegnere con SIGTERM e verificare uscita pulita.

### 7.3 Assert minimi

- un solo worker riceve la Task;
- nessuna Task resta RUNNING dopo completamento;
- artifact esiste nel BlobStore;
- outbox non contiene eventi bloccati non giustificati;
- nessun panic nei log;
- readiness diventa verde;
- teardown non supera il timeout previsto.

Conservare in CI:

- log master;
- log worker;
- report smoke JSON;
- eventuale DB SQLite come artifact solo su fallimento, dopo rimozione di dati sensibili;
- checksum degli output.

## 8. Test di recovery

Aggiungere scenari separati:

### 8.1 Master restart

- Task READY prima del restart;
- master riavviato;
- scheduler ricostruisce stato da repository;
- Task viene assegnata una sola volta.

### 8.2 Worker disconnect

- worker riceve Task;
- processo worker viene terminato;
- lease scade;
- Task torna recuperabile;
- nuovo attempt viene creato;
- report tardivo del vecchio attempt viene rifiutato o registrato come stale.

### 8.3 Upload interrotto

- upload chunked incompleto;
- master restart;
- sessione upload recuperata o marcata fallita secondo contratto;
- nessun Job viene finalizzato senza artifact verificato.

### 8.4 Database lock/concorrenza

Con SQLite:

- più writer concorrenti controllati;
- nessun `database is locked` non gestito nel workload supportato;
- busy timeout e pool misurati, non soltanto aumentati;
- test sotto race detector.

---

Continue with [03b — Governance, release e deploy](03b-governance-release-and-operations.md).
