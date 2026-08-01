# Velox SLO e runbook operativi

## SLO

Il catalogo sorgente è `DataServer/internal/slo`. Gli alert devono usare la
finestra di 30 giorni e il relativo error budget, senza sostituire le metriche
con controlli manuali.

## Runbook rapido

### Worker offline

Verificare heartbeat, sessione e ultimo `worker_runtime_identity`; drenare il
worker, spostare i task recuperabili in coda e confermare che non esistano
lease attive non rinnovate.

### Disco pieno

Bloccare admission per nuovi render, controllare `gc_candidate_bytes` e gli
artifact `UPLOAD_PENDING`, liberare solo candidati verificati e poi riaprire la
coda. Non cancellare file con riferimenti DB.

### Database bloccato

Controllare lock e connessioni, sospendere admission, non eliminare il file DB;
eseguire il backup consistente e seguire il restore test prima del failover.

### Job zombie o lease scaduta

Controllare timeline e worker snapshot, lasciare agire il reaper, poi eseguire
un retry controllato con nuovo attempt. L'orizzonte storico non va modificato.

### Upload parziale / provider rate-limited

Riprendere dalla fase pubblicazione persistita, rispettare `Retry-After` e non
ricaricare il video se `VIDEO_CREATED` è già presente.

### Token revocato

Mettere la publication in `BLOCKED_AUTH`/`RETRY_WAIT`, revocare la lease,
ruotare la credenziale nel vault e verificare l'evento di utilizzo audit.

### Artifact corrotto o rollback motore

Quarantinare l'artifact, bloccare la pubblicazione, confrontare ffprobe/hash e
promuovere il worker solo dopo golden e canary verdi. In caso contrario il
rollout passa a `quarantine`.
