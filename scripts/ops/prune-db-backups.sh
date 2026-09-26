#!/usr/bin/env bash
set -euo pipefail

# Retain all SQLite snapshots newer than RETENTION_DAYS and at least the
# newest KEEP_LAST snapshots. Only known backup filename families are touched.
DATA_DIR="${1:-/opt/velox/current/.velox/data}"
APPLY=0
if [[ "${2:-}" == "--apply" ]]; then APPLY=1; fi
if [[ "${2:-}" != "" && "${2:-}" != "--apply" ]]; then
  echo "usage: $0 [data-dir] [--apply]" >&2
  exit 2
fi
KEEP_LAST="${VELOX_DB_BACKUP_KEEP_LAST:-3}"
RETENTION_DAYS="${VELOX_DB_BACKUP_RETENTION_DAYS:-14}"
[[ "$KEEP_LAST" =~ ^[1-9][0-9]*$ && "$RETENTION_DAYS" =~ ^[1-9][0-9]*$ ]] || {
  echo "KEEP_LAST and RETENTION_DAYS must be positive integers" >&2; exit 2;
}
[[ -d "$DATA_DIR" ]] || { echo "data directory not found: $DATA_DIR" >&2; exit 2; }

python3 - "$DATA_DIR" "$KEEP_LAST" "$RETENTION_DAYS" "$APPLY" <<'PY'
import pathlib, sys, time

root = pathlib.Path(sys.argv[1])
keep_last, retention_days, apply = int(sys.argv[2]), int(sys.argv[3]), sys.argv[4] == "1"
prefixes = ("velox.db.bak-", "velox.db.bak.", "velox.db.pre-", "velox.db.backup-")
files = [p for p in root.iterdir() if p.is_file() and not p.is_symlink() and p.name.startswith(prefixes)]
files.sort(key=lambda p: (p.stat().st_mtime_ns, p.name), reverse=True)
cutoff = time.time() - retention_days * 86400
keep = {p for p in files[:keep_last]}
keep.update(p for p in files if p.stat().st_mtime >= cutoff)
total = removed = 0
for p in files:
    size = p.stat().st_size
    if p in keep:
        print(f"KEEP   {size:>12} {p}")
        continue
    total += size
    print(f"{'DELETE' if apply else 'WOULD_DELETE'} {size:>12} {p}")
    if apply:
        p.unlink()
        removed += size
print(f"snapshots={len(files)} retained={len(keep)} reclaimable_bytes={total} removed_bytes={removed} mode={'apply' if apply else 'dry-run'}")
PY
