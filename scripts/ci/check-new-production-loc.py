#!/usr/bin/env python3
"""Report oversized newly-added production files.

This is intentionally complementary to check-loc-thresholds.sh: it only scans
Git-added files between BASE_REF and HEAD, so existing long files are not
retroactively reported by this guardrail.
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
from pathlib import PurePosixPath

DEFAULT_THRESHOLD = 600
EMPTY_TREE = "4b825dc642cb6eb9a060e54bf8d69288fbee4904"

# Production source/config formats used in this repository. Documentation,
# fixtures, snapshots, generated output, and test files are filtered below.
PRODUCTION_SUFFIXES = {
    ".bash",
    ".c",
    ".cc",
    ".cpp",
    ".cxx",
    ".h",
    ".hh",
    ".hpp",
    ".java",
    ".js",
    ".jsx",
    ".json",
    ".kt",
    ".kts",
    ".php",
    ".py",
    ".rb",
    ".rs",
    ".sh",
    ".swift",
    ".ts",
    ".tsx",
    ".yaml",
    ".yml",
    ".go",
}

DOCUMENTATION_SUFFIXES = {".adoc", ".md", ".rst", ".txt"}
EXCLUDED_DIRECTORY_NAMES = {
    ".git",
    ".github",
    "archive",
    "docs",
    "documentation",
    "archives",
    "fixture",
    "fixtures",
    "generated",
    "gen",
    "golden",
    "node_modules",
    "snapshot",
    "snapshots",
    "test",
    "testdata",
    "tests",
}
GENERATED_MARKER = re.compile(
    rb"(?:code )?generated(?: by| file)?|do not edit|@generated",
    re.IGNORECASE,
)


def fail(message: str) -> int:
    print(f"new-production-loc: ERROR: {message}", file=sys.stderr)
    return 2


def read_ref(name: str, default: str) -> str:
    value = os.environ.get(name, default).strip()
    return value or default


def positive_threshold() -> int:
    raw = os.environ.get(
        "NEW_PRODUCTION_FILE_LOC_THRESHOLD", str(DEFAULT_THRESHOLD)
    ).strip()
    if not raw.isdigit() or int(raw) <= 0:
        raise ValueError(
            "NEW_PRODUCTION_FILE_LOC_THRESHOLD must be a positive integer"
        )
    return int(raw)


def git(*args: str) -> bytes:
    return subprocess.check_output(["git", *args], stderr=subprocess.PIPE)


def resolve_base(base: str, head: str) -> str:
    # A branch-creation push can expose an all-zero event.before. There is no
    # prior ref in that event, so compare with the empty tree rather than
    # origin/main (which may already point at HEAD after checkout).
    # HEAD is always explicit; an invalid head is a configuration error, not
    # evidence of an initial repository.
    git("rev-parse", "--verify", head)
    if not base or set(base) == {"0"}:
        return EMPTY_TREE

    # The CI fallback for schedule/manual runs is HEAD^. A root commit has
    # no parent, so that sentinel intentionally maps to the empty tree.
    if base == "HEAD^":
        try:
            git("rev-parse", "--verify", base)
        except subprocess.CalledProcessError:
            return EMPTY_TREE
        return base

    # An explicitly supplied but unreachable base is a CI configuration
    # error. Do not silently fall back to the empty tree and scan the world.
    git("rev-parse", "--verify", base)
    git("merge-base", base, head)
    return base


def added_paths(base: str, head: str) -> list[str]:
    # Use the exact event base. `resolve_base` already verifies that normal
    # histories share an ancestor; using merge-base here could include files
    # from commits outside the push/PR range after a history divergence.
    output = git("diff", "--name-only", "-z", "--diff-filter=A", base, head)
    return [path for path in output.decode(errors="surrogateescape").split("\0") if path]


def blob(path: str, head: str) -> bytes:
    return git("show", f"{head}:{path}")


def is_documentation(path: PurePosixPath) -> bool:
    name = path.name.lower()
    return path.suffix.lower() in DOCUMENTATION_SUFFIXES or name in {
        "changelog",
        "license",
        "copying",
        "readme",
    }


def is_binary(path: str, base: str, head: str) -> bool:
    try:
        numstat = git("diff", "--numstat", base, head, "--", path).decode(errors="replace")
    except subprocess.CalledProcessError:
        return False
    first = numstat.splitlines()[0] if numstat.splitlines() else ""
    return first.startswith("-\t-\t")


def is_excluded(path: PurePosixPath, data: bytes) -> bool:
    parts = {part.lower() for part in path.parts}
    name = path.name.lower()
    stem = path.stem.lower()

    if is_documentation(path):
        return True
    if name.startswith(("openapi", "swagger")) or name.endswith(
        (".schema.json", ".schema.yaml", ".schema.yml")
    ):
        return True
    if parts & EXCLUDED_DIRECTORY_NAMES:
        return True
    if ".github" in parts and "workflows" in parts:
        return True
    if name.endswith("_test.go") or name.endswith("_test.py"):
        return True
    if any(token in stem for token in ("snapshot", "fixture", "golden")):
        return True
    if (
        stem.endswith(("_gen", "_generated"))
        or ".generated." in name
        or name.endswith(("_gen.go", "_generated.go"))
    ):
        return True
    if ".pb." in name or name.endswith(".pb.go") or name.endswith("_grpc.pb.go"):
        return True
    if path.suffix.lower() not in PRODUCTION_SUFFIXES:
        return True
    # NUL bytes are a conservative binary signal. Empty files are text and
    # harmless: their newline count is zero and they cannot breach the gate.
    if b"\0" in data[:8192]:
        return True
    if GENERATED_MARKER.search(data):
        return True
    return False


def loc(data: bytes) -> int:
    # Match `wc -l`: only newline-terminated lines count.
    return data.count(b"\n")


def main() -> int:
    base = read_ref("BASE_REF", "origin/main")
    head = read_ref("HEAD_REF", "HEAD")
    try:
        threshold = positive_threshold()
        base = resolve_base(base, head)
        paths = added_paths(base, head)
    except (ValueError, subprocess.CalledProcessError) as exc:
        detail = getattr(exc, "stderr", b"")
        if isinstance(detail, bytes):
            detail = detail.decode(errors="replace").strip()
        return fail(str(exc) + (f": {detail}" if detail else ""))

    candidates: list[tuple[str, int]] = []
    excluded = 0
    for raw_path in paths:
        path = PurePosixPath(raw_path)
        try:
            data = blob(raw_path, head)
        except subprocess.CalledProcessError as exc:
            return fail(f"cannot read added file {raw_path}: {exc}")
        if is_binary(raw_path, base, head) or is_excluded(path, data):
            excluded += 1
            continue
        candidates.append((raw_path, loc(data)))

    print(
        "new-production-loc: scanned "
        f"{len(candidates)} new production file(s), excluded {excluded}; "
        f"threshold={threshold} LOC; base={base}; head={head}"
    )
    violations = 0
    for path, lines in candidates:
        if lines > threshold:
            print(
                f"::error file={path}::new production file has {lines} LOC "
                f"(threshold {threshold})"
            )
            violations += 1
            continue
        print(f"new-production-loc: PASS {path} ({lines} LOC)")

    if violations:
        print(
            f"new-production-loc: FAIL: {violations} new production file(s) "
            f"exceed {threshold} LOC"
        )
        return 1
    print("new-production-loc: PASS: no new production file exceeds the threshold")
    return 0


if __name__ == "__main__":
    sys.exit(main())
