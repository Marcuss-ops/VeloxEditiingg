#!/usr/bin/env python3
"""Read-only validator for the uniform worker identity migration plan.

The validator checks repository mapping and, when --db is supplied, safe
SQLite metadata only. It never renames identities or calls SSH, Ansible,
Docker, OpenBao, or the Master API. It never prints credential/hash values,
raw JSON, tokens, private keys, certificate bodies, or connection data.
"""
from __future__ import annotations

import argparse
import re
import sqlite3
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PLAN = ROOT / "docs/operations/identity-migration-plan.md"

MAPPING = {
    "host_57_129_132_133": "velox-worker-57-129-132-133",
    "host_57_131_20_173": "velox-worker-57-131-20-173",
    "velox-worker-13197": "velox-worker-149-56-131-97",
    "velox-worker-523925eb": "velox-worker-51-222-204-158",
}
ALIASES = {
    "host_57_129_132_133": "worker_57_129",
    "host_57_131_20_173": "worker_57_131",
    "velox-worker-13197": "worker_13197",
    "velox-worker-523925eb": "worker_523925",
}
REQUIRED_TABLE_COLUMNS = {
    "workers": {"worker_id"},
    "worker_credentials": {"worker_id", "credential_hash"},
    "ansible_hosts": {"worker_id"},
    "worker_runtime_snapshots": {"worker_id"},
}


def fail(message: str) -> None:
    raise RuntimeError(message)


def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table,),
    ).fetchone()
    return row is not None


def table_count(conn: sqlite3.Connection, table: str) -> int:
    return int(conn.execute(f'SELECT count(*) FROM "{table}"').fetchone()[0])


def table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {
        str(row[1])
        for row in conn.execute(f'PRAGMA table_info("{table}")')
    }


def worker_ids(conn: sqlite3.Connection, table: str) -> set[str]:
    return {
        str(row[0])
        for row in conn.execute(f'SELECT worker_id FROM "{table}"')
        if row[0] is not None
    }


def validate_plan() -> None:
    if not PLAN.is_file():
        fail(f"missing plan: {PLAN}")

    target_ids = list(MAPPING.values())
    if len(MAPPING) != 4 or len(set(MAPPING)) != 4 or len(set(target_ids)) != 4:
        fail("mapping must contain exactly four unique current and target IDs")

    for current, target in MAPPING.items():
        if current == target:
            fail(f"mapping is not a rename: {current}")
        if not target.startswith("velox-worker-") or "_" in target:
            fail(f"target does not use uniform naming: {target}")
        if not ALIASES.get(current):
            fail(f"inventory alias missing for current ID: {current}")

    plan = PLAN.read_text(encoding="utf-8")
    mapping_rows = {
        (current, target)
        for current, target in re.findall(
            r"^\| `([^`]+)` \| `[^`]+` \| `([^`]+)` \|$",
            plan,
            flags=re.MULTILINE,
        )
    }
    expected_rows = set(MAPPING.items())
    if mapping_rows != expected_rows:
        fail(
            "mapping table differs from validator mapping: "
            f"expected={sorted(expected_rows)!r}, found={sorted(mapping_rows)!r}"
        )

    for required in (
        "dual-identity",
        "forward-only",
        "worker_runtime_snapshots",
        "OpenBao",
        "Rollback plan",
        "This preparation does **not**",
        "OPENBAO_CERTIFICATE_INVENTORY_AUDIT=NOT_VERIFIED_BY_THIS_TOOL",
        "--db",
    ):
        if required not in plan:
            fail(f"plan missing required contract: {required}")


def validate_db(path: Path) -> None:
    if not path.is_file():
        fail(f"database does not exist: {path}")

    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        conn.execute("PRAGMA query_only=ON")
        integrity = str(conn.execute("PRAGMA integrity_check").fetchone()[0])
        if integrity != "ok":
            fail(f"integrity_check={integrity!r}")
        print(f"DB_INTEGRITY={integrity}")

        for table, required_columns in REQUIRED_TABLE_COLUMNS.items():
            if not table_exists(conn, table):
                fail(f"required table missing: {table}")
            missing = sorted(required_columns - table_columns(conn, table))
            if missing:
                fail(f"{table}: missing expected columns {missing}")
            print(f"TABLE_{table}_COUNT={table_count(conn, table)}")

        current_ids = set(MAPPING)
        target_ids = set(MAPPING.values())
        for table in ("workers", "worker_credentials", "ansible_hosts"):
            present = worker_ids(conn, table)
            current_count = len(present & current_ids)
            target_count = len(present & target_ids)
            print(f"{table.upper()}_CURRENT_IDS_PRESENT={current_count}/4")
            print(f"{table.upper()}_TARGET_IDS_PRESENT={target_count}/4")
            if current_count != 4:
                fail(f"{table}: current identity coverage is {current_count}/4")

        placeholders = ",".join("?" for _ in MAPPING)
        rows = conn.execute(
            """SELECT worker_id,
                      count(*) AS row_count,
                      sum(CASE WHEN credential_hash IS NOT NULL
                                 AND length(credential_hash) > 0
                               THEN 1 ELSE 0 END) AS hash_present
                 FROM worker_credentials
                WHERE worker_id IN (%s)
                GROUP BY worker_id
                ORDER BY worker_id""" % placeholders,
            tuple(MAPPING),
        )
        credential_rows = {str(row[0]): (int(row[1]), int(row[2])) for row in rows}
        if set(credential_rows) != current_ids:
            fail("worker_credentials: current identity coverage is not exactly 4/4")
        for worker_id in sorted(current_ids):
            row_count, hash_present = credential_rows[worker_id]
            print(
                f"CREDENTIAL_META={worker_id}:rows={row_count},"
                f"hash_present={hash_present}"
            )
            if row_count != 1 or hash_present != 1:
                fail(
                    "worker_credentials: expected exactly one non-empty hash "
                    f"row for {worker_id}, got rows={row_count}, "
                    f"hash_present={hash_present}"
                )

        target_tuple = tuple(MAPPING.values())
        for table in REQUIRED_TABLE_COLUMNS:
            placeholders = ",".join("?" for _ in target_tuple)
            collision_count = int(
                conn.execute(
                    f"SELECT count(*) FROM \"{table}\" "
                    f"WHERE worker_id IN ({placeholders})",
                    target_tuple,
                ).fetchone()[0]
            )
            print(f"{table.upper()}_TARGET_COLLISIONS={collision_count}")
            if collision_count != 0:
                fail(f"target IDs already exist in {table}: {collision_count}")
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, help="optional local SQLite DB to inspect read-only")
    args = parser.parse_args()

    validate_plan()
    print("MAPPING_VALIDATION=PASS")
    for current, target in MAPPING.items():
        print(f"MAPPING={current}->{target}")
    if args.db is not None:
        validate_db(args.db)
    print("OPENBAO_CERTIFICATE_INVENTORY_AUDIT=NOT_VERIFIED_BY_THIS_TOOL")
    print("IDENTITY_MIGRATION_PLAN_VALIDATION=PASS")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, RuntimeError, sqlite3.Error) as error:
        print(f"IDENTITY_MIGRATION_PLAN_VALIDATION=FAIL: {error}")
        raise SystemExit(1)
