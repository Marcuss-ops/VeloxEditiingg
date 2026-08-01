# Backup e disaster recovery

Velox usa `VACUUM INTO` per snapshot consistenti. In produzione il file deve
essere cifrato con `BackupEncryptedSQLite`, copiato su storage off-site con
versioning e retention, e sottoposto a `RestoreTest` in una directory isolata.

Baseline operativa: RPO 5 minuti, RTO 30 minuti. Il test di restore deve
verificare integrità SQLite, migration ledger, conteggi di `jobs`, `tasks`,
`artifacts` e `job_deliveries`, quindi aprire un job e recuperare l'artifact
associato prima di essere marcato PASS.
