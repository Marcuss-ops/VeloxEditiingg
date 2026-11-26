#!/usr/bin/env python3
"""Read-only inventory of persisted legacy video task and recovery references."""

import argparse
import json
import os
import sqlite3
import sys
from collections import Counter
from pathlib import Path

LEGACY = ("video.prepare.window.v1", "video.assemble.slice.v1")


def table_columns(db, table):
    return [row[1] for row in db.execute(f'PRAGMA table_info("{table}")')]


def walk_strings(value, path="$", found=None):
    if found is None:
        found = []
    if isinstance(value, dict):
        for key, child in value.items():
            walk_strings(child, f"{path}.{key}", found)
    elif isinstance(value, list):
        for index, child in enumerate(value):
            walk_strings(child, f"{path}[{index}]", found)
    elif isinstance(value, str):
        found.append((path, value))
    return found


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--db",
        default=os.environ.get("VELOX_DB_PATH", "/opt/velox/current/.velox/data/velox.db"),
        help="Master SQLite database path (read-only; defaults to VELOX_DB_PATH)",
    )
    parser.add_argument("--json", action="store_true", help="emit the inventory as JSON")
    args = parser.parse_args()
    db_path = Path(args.db).expanduser().resolve()
    if not db_path.is_file():
        parser.error(f"database not found: {db_path}")

    try:
        db = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=10)
        db.execute("PRAGMA query_only=ON")
        db.execute("PRAGMA busy_timeout=10000")
        tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if not {"tasks", "task_specs", "task_attempts"}.issubset(tables):
            raise RuntimeError("database is missing tasks, task_specs, or task_attempts")

        task_rows = db.execute(
            """SELECT t.task_id, t.job_id, t.executor_id, s.executor_id,
                      t.status, t.revision, t.attempt_count, t.created_at, t.updated_at
                 FROM tasks t LEFT JOIN task_specs s ON s.task_id=t.task_id
                WHERE t.executor_id IN (?, ?) OR s.executor_id IN (?, ?)
                ORDER BY t.created_at, t.task_id""",
            (*LEGACY, *LEGACY),
        ).fetchall()
        legacy_ids = {row[0] for row in task_rows}
        attempt_rows = []
        if legacy_ids:
            placeholders = ",".join("?" for _ in legacy_ids)
            attempt_rows = db.execute(
                f"SELECT id, task_id, attempt_number, status, error_code, created_at, updated_at "
                f"FROM task_attempts WHERE task_id IN ({placeholders}) "
                "ORDER BY task_id, attempt_number",
                tuple(sorted(legacy_ids)),
            ).fetchall()

        references = []
        for task_id, executor_id, payload in db.execute(
            "SELECT task_id, executor_id, payload_json FROM task_specs"
        ):
            try:
                document = json.loads(payload or "{}")
            except (TypeError, json.JSONDecodeError):
                references.append({"task_id": task_id, "executor_id": executor_id, "invalid_json": True})
                continue
            for json_path, value in walk_strings(document):
                if value in legacy_ids or value in LEGACY:
                    references.append({
                        "task_id": task_id,
                        "executor_id": executor_id,
                        "json_path": json_path,
                        "value": value,
                    })

        recovery_refs = []
        for table in sorted(tables - {"tasks", "task_attempts", "task_specs"}):
            columns = table_columns(db, table)
            ref_columns = [name for name in columns if "task_id" in name.lower()]
            for column in ref_columns:
                if not legacy_ids:
                    continue
                quoted_table = '"' + table.replace('"', '""') + '"'
                quoted_column = '"' + column.replace('"', '""') + '"'
                placeholders = ",".join("?" for _ in legacy_ids)
                query = f"SELECT {quoted_column} FROM {quoted_table} WHERE {quoted_column} IN ({placeholders})"
                for row in db.execute(query, tuple(sorted(legacy_ids))):
                    recovery_refs.append({"table": table, "column": column, "task_id": row[0]})

        retry_tables = []
        for table in sorted(tables):
            lowered = table.lower()
            if "retry" in lowered or "recover" in lowered:
                count = db.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
                retry_tables.append({"table": table, "rows": count})
        statuses = Counter(row[4] for row in task_rows)
        attempt_statuses = Counter(row[3] for row in attempt_rows)
        report = {
            "database": str(db_path),
            "legacy_task_count": len(task_rows),
            "task_status_counts": dict(sorted(statuses.items())),
            "attempt_count": len(attempt_rows),
            "attempt_status_counts": dict(sorted(attempt_statuses.items())),
            "tasks": [dict(zip(("task_id", "job_id", "task_executor_id", "spec_executor_id", "status", "revision", "attempt_count", "created_at", "updated_at"), row)) for row in task_rows],
            "attempts": [dict(zip(("attempt_id", "task_id", "attempt_number", "status", "error_code", "created_at", "updated_at"), row)) for row in attempt_rows],
            "task_spec_references": references,
            "other_task_id_references": recovery_refs,
            "retry_recovery_tables": retry_tables,
        }
        if args.json:
            print(json.dumps(report, indent=2, sort_keys=True))
        else:
            print(f"database: {db_path}")
            print(f"legacy tasks: {len(task_rows)} {dict(sorted(statuses.items()))}")
            print(f"attempts: {len(attempt_rows)} {dict(sorted(attempt_statuses.items()))}")
            print(f"task-spec references: {len(references)}")
            print(f"other task-id references: {len(recovery_refs)}")
            print(f"retry/recovery tables: {retry_tables}")
            for row in report["tasks"]:
                print("task:", json.dumps(row, sort_keys=True))
            for row in references:
                print("spec-reference:", json.dumps(row, sort_keys=True))
            for row in recovery_refs:
                print("task-id-reference:", json.dumps(row, sort_keys=True))
        db.close()
    except (sqlite3.Error, RuntimeError) as error:
        print(f"legacy video task audit failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
