# SQLite retention and disk cleanup

## Database snapshots

The operator script only considers `velox.db.bak-*`, `velox.db.bak.*`,
`velox.db.pre-*`, and `velox.db.backup-*`. It keeps snapshots newer than 14
days and always keeps the three newest snapshots. It never matches the live
database, WAL, or SHM files.

Preview, then apply:

```bash
sudo scripts/ops/prune-db-backups.sh /opt/velox/current/.velox/data
sudo scripts/ops/prune-db-backups.sh /opt/velox/current/.velox/data --apply
```

The policy can be tuned with `VELOX_DB_BACKUP_KEEP_LAST` and
`VELOX_DB_BACKUP_RETENTION_DAYS`. Review the preview before applying it.

## Reclaiming space from `job_events`

The Master prunes terminal/orphan `job_events` older than
`VELOX_RETENTION_JOB_EVENTS_DAYS` (default 30) in 5,000-row transactions.
Active-job history is retained. The first maintenance pass drains the backlog;
subsequent passes keep it bounded. SQLite does not return deleted pages to the
filesystem until a VACUUM (unless incremental auto-vacuum was enabled when the
database was created).

Run VACUUM only in a maintenance window after confirming retention has run and
the service is stopped, with a verified recovery snapshot and enough free
space for a rebuilt database. Then run:

```bash
sqlite3 /opt/velox/current/.velox/data/velox.db \
  'PRAGMA wal_checkpoint(TRUNCATE); VACUUM;'
```

Do not run VACUUM from the normal reconciler or heartbeat path: it needs a
long exclusive maintenance window and can temporarily require disk space close
to the database size.
