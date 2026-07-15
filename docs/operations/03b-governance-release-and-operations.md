# Runbook operativo 03b — Governance, release e deploy

Status: operativo

Data snapshot: 2026-06-21

Document set: [03 — Build e CI](03-build-deploy-and-ci-hardening.md) · [03a — Smoke e recovery](03a-smoke-and-recovery.md) · **03b — Governance, release e deploy** · [03c — Rollout e accettazione](03c-rollout-and-acceptance.md).

## 9. Branch protection e policy GitHub

Configurare `main` con:

- pull request obbligatoria;
- required status check `make verify`;
- branch aggiornato prima del merge;
- conversazioni review risolte;
- force push disabilitato;
- branch deletion disabilitata;
- amministratori inclusi nelle regole, salvo procedura break-glass documentata;
- linear history o merge strategy scelta e coerente.

Il repository supporta merge commit, squash e rebase: sceglierne una strategia primaria. Raccomandazione:

- squash per PR piccole;
- titolo Conventional Commit;
- nessun merge di checklist vuota;
- nessun push diretto su `main` dopo l'attivazione della protezione.

Il break-glass deve richiedere:

- incidente reale;
- issue associata;
- motivo nel commit;
- verifica post-merge;
- review retrospettiva.

## 10. Release pipeline

### 10.1 Versione

`VERSION.txt` è la fonte di verità. Il tag deve corrispondere esattamente:

```bash
VERSION="$(tr -d '[:space:]' < VERSION.txt)"
test "$GITHUB_REF_NAME" = "v$VERSION"
```

Non pubblicare se tag e file divergono.

### 10.2 Artifact

Pubblicare:

- immagine master;
- immagine worker;
- digest SHA256;
- SBOM;
- changelog;
- commit SHA;
- eventuali binari standalone solo se realmente supportati.

Tag immagini:

```text
velox-master:<version>
velox-master:sha-<shortsha>
velox-worker:<version>
velox-worker:sha-<shortsha>
```

`latest` può essere aggiornato soltanto dopo smoke test riuscito.

### 10.3 Promotion

Separare build da promotion:

1. build una volta;
2. testare gli stessi digest;
3. promuovere i digest verificati;
4. non ricostruire tra staging e production.

## 11. Deploy operativo

### 11.1 Preflight

Prima del deploy:

```bash
make verify
```

Verificare:

- backup DB completato;
- migrazioni previste e compatibili;
- spazio disco;
- directory e ownership;
- worker allowlist;
- certificati gRPC;
- BlobStore raggiungibile;
- digest immagini.

### 11.2 Ordine deploy

Per cambi protocol-compatible:

1. master compatibile con worker vecchi e nuovi;
2. aggiornamento worker graduale;
3. verifica flotta;
4. rimozione compatibilità in una release successiva.

Per cambi protocol-incompatible:

- introdurre prima una versione di transizione;
- negoziare protocol version esplicita;
- rifiutare versioni incompatibili con errore chiaro;
- non usare fallback silenziosi.

### 11.3 Canary worker

Aggiornare inizialmente un worker:

- verificare registrazione;
- eseguire Job smoke;
- osservare errori e performance;
- poi espandere alla flotta.

Il canary non deve ricevere lavori di produzione incompatibili durante il test.

## 12. Rollback

Preparare prima del deploy:

- digest immagini precedenti;
- backup database;
- matrice compatibilità schema/app;
- comandi systemd/compose per tornare indietro;
- responsabile decisione rollback.

Regole:

1. le migrazioni sono forward-only;
2. non fare downgrade applicativo se il vecchio binario non comprende il nuovo schema;
3. in quel caso usare una release correttiva compatibile, non rollback cieco;
4. non cancellare artifact prodotti durante il deploy fallito prima dell'audit;
5. fermare nuovi claim prima di spegnere worker o master.

## 13. Osservabilità minima per produzione

Registrare metriche e log strutturati per:

- worker connessi/offline/draining;
- Task READY/RUNNING/FAILED;
- lease scadute;
- durata claim;
- TaskAttempt per executor/versione;
- errori upload;
- outbox backlog;
- database busy/latency;
- cache hit bytes;
- render/encode/upload duration;
- graceful shutdown timeout.

Ogni log deve includere gli ID disponibili:

```text
job_id
task_id
attempt_id
worker_id
session_id
executor_id
artifact_id
```

Non loggare token, header Authorization o payload sensibili.

---

Continue with [03c — Rollout e accettazione](03c-rollout-and-acceptance.md).
