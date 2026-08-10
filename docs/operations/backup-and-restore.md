# Backup e disaster recovery

## Stato dell'implementazione

L'audit dei call site del 2026-08-10 ha confermato che il package
`DataServer/internal/backup` era scaffolding irraggiungibile: non aveva import,
caller, wiring di bootstrap, scheduler, CLI o job operativo. La decisione di rimuovere il package e i relativi test è stata approvata;
la rimozione effettiva avverrà nel commit atomico successivo. Questo documento
descrive quindi il requisito operativo e non dichiara un'implementazione
automatica disponibile nel server.

Fino all'introduzione di un componente posseduto e collegato al bootstrap, ogni
backup pre-deploy o di emergenza deve essere eseguito con una procedura
operativa approvata dal proprietario dell'ambiente. Non usare nomi di funzioni
rimossi come se fossero comandi disponibili.

## Criteri per il futuro componente

La procedura target deve produrre uno snapshot SQLite consistente, cifrarlo con
chiavi gestite dal percorso credentials approvato, conservarlo su storage
off-site con versioning e retention e verificare il restore in una directory
isolata. L'evidenza deve includere:

- `PRAGMA integrity_check` riuscito;
- migration ledger presente;
- conteggi di `jobs`, `tasks`, `artifacts` e `job_deliveries`;
- apertura di un job e recupero dell'artifact associato;
- SHA-256 dell'artefatto e timestamp/operator owner.

RPO 5 minuti e RTO 30 minuti restano obiettivi da approvare e misurare; non
sono garanzie già fornite dal runtime corrente.

## Ownership

Owner del requisito: platform/operations. Un'eventuale reintroduzione deve
avere un entrypoint operativo reale, test di restore eseguiti in CI o nel
runbook, retention documentata e un nuovo call-site proof prima di essere
considerata produzione.
