# State machines and invariant audit

Velox mantiene una sola matrice eseguibile in
`DataServer/internal/statemachine`. Ogni `TransitionRule` descrive:

- dominio e stato sorgente/destinazione;
- attore canonico autorizzato;
- invarianti richieste;
- eventi emessi;
- idempotenza della ripetizione.

I repository SQL restano gli unici writer. I CAS di jobs, tasks, artifacts,
uploads e deliveries validano la transizione contro il registry prima di
eseguire la loro scrittura atomica.

## Audit read-only

```bash
velox-admin audit-invariants --db /path/to/velox.db
```

Il comando apre SQLite con `mode=ro`, esegue soltanto `SELECT` e stampa JSON
con registry, invarianti e findings. Non applica correzioni e non scrive audit
eventi. Un report senza violazioni contiene `"ok": true`; con violazioni il
comando restituisce exit code non-zero dopo aver stampato il report.

Le verifiche includono:

- stati non presenti nel registry;
- convergenza task/attempt;
- convergenza job/task;
- artifact `READY` senza blob referenziabile;
- delivery `SUCCEEDED` senza `remote_id`;
- più sessioni worker attive per tipo;
- upload `COMPLETED` senza `completed_at`.

L’auditor osserva soltanto. La riconciliazione applicativa rimane separata e
può essere eseguita esclusivamente dai percorsi operatori già esistenti.
