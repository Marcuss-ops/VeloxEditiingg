#!/usr/bin/env python3
"""Reproducible refactoring metrics for two Git refs.

The analyzer intentionally uses only the Python standard library.  Its output
is a versioned JSON document plus a Markdown summary so CI artifacts from
separate refactors can be compared without depending on a local tool install.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import tarfile
import tempfile
from collections import Counter, defaultdict
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Iterable

SCHEMA_VERSION = 1
WINDOW_SIZE = 6
SOURCE_EXTENSIONS = {
    ".go": "go",
    ".c": "c",
    ".cc": "cpp",
    ".cpp": "cpp",
    ".cxx": "cpp",
    ".h": "cpp",
    ".hh": "cpp",
    ".hpp": "cpp",
    ".hxx": "cpp",
    ".py": "python",
    ".sh": "shell",
    ".bash": "shell",
}
EXCLUDED_DIRS = {".git", "build", "dist", "node_modules", "vendor", "__pycache__", ".pb-cache"}
GENERATED_SUFFIXES = (".pb.go", "_generated.go", "_generated.hpp", "_generated.h")

# Domain categories are deliberately broad and stable.  They describe the
# responsibilities visible in a file, not a claim about architectural purity.
RESPONSIBILITY_RULES = {
    "artifact": (r"artifact|upload|spool|output|manifest",),
    "configuration": (r"config|configuration|environment|env",),
    "conversion": (r"marshal|unmarshal|json|protobuf|proto|normalize|projection|mapping|convert",),
    "media": (r"ffmpeg|avformat|avcodec|render|video|audio|scene|clip|frame|media",),
    "orchestration": (r"orchestrat|execute|dispatch|pipeline|workflow|lifecycle|coordinator|runner",),
    "persistence": (r"database/sql|sqlite|sql|repository|store|persist|transaction|migration",),
    "security": (r"auth|token|secret|crypto|tls|certificate|permission|credential",),
    "telemetry": (r"metric|telemetry|observability|prometheus|trace|logging|logger|report",),
    "transport": (r"grpc|http|websocket|transport|rpc|socket|client",),
    "validation": (r"validat|parse|schema|contract|check|verify|assert",),
}


def run_git(root: Path, *args: str) -> bytes:
    return subprocess.check_output(["git", "-C", str(root), *args], stderr=subprocess.STDOUT)


def extract_ref(root: Path, ref: str, destination: Path) -> None:
    try:
        archive = subprocess.Popen(
            ["git", "-C", str(root), "archive", "--format=tar", ref],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        assert archive.stdout is not None
        with tarfile.open(fileobj=archive.stdout, mode="r|") as tar:
            tar.extractall(destination)
        stderr = archive.stderr.read().decode("utf-8", "replace") if archive.stderr else ""
        code = archive.wait()
    except OSError as exc:
        raise RuntimeError(f"cannot create archive for {ref}: {exc}") from exc
    if code != 0:
        raise RuntimeError(f"cannot read Git ref {ref}: {stderr.strip()}")


def is_generated(path: Path) -> bool:
    return path.name.endswith(GENERATED_SUFFIXES) or "/vendor/" in path.as_posix()


def iter_source_files(root: Path) -> Iterable[tuple[str, Path, str]]:
    for current, dirs, files in os.walk(root):
        dirs[:] = sorted(d for d in dirs if d not in EXCLUDED_DIRS)
        for name in sorted(files):
            path = Path(current) / name
            language = SOURCE_EXTENSIONS.get(path.suffix.lower())
            if language is None or is_generated(path):
                continue
            yield path.relative_to(root).as_posix(), path, language


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="replace")


def strip_comments_and_strings(text: str) -> str:
    """Keep line structure while removing strings and comments for token counts."""
    out: list[str] = []
    i = 0
    block = False
    quote = ""
    escaped = False
    line: list[str] = []
    while i < len(text):
        char = text[i]
        nxt = text[i + 1] if i + 1 < len(text) else ""
        if block:
            if char == "*" and nxt == "/":
                block = False
                line.extend("  ")
                i += 2
            else:
                line.append("\n" if char == "\n" else " ")
                i += 1
            continue
        if quote:
            if char == "\n":
                line.append(" ")
                quote = ""
                escaped = False
            elif escaped:
                line.append(" ")
                escaped = False
            elif char == "\\":
                line.append(" ")
                escaped = True
            elif char == quote:
                line.append(" ")
                quote = ""
            else:
                line.append(" ")
            i += 1
            continue
        if char in ('"', "'", "`"):
            quote = char
            line.append(" ")
            i += 1
            continue
        if char == "/" and nxt == "*":
            block = True
            line.extend("  ")
            i += 2
            continue
        if char == "/" and nxt == "/":
            while i < len(text) and text[i] != "\n":
                line.append(" ")
                i += 1
            continue
        if char == "#":
            while i < len(text) and text[i] != "\n":
                line.append(" ")
                i += 1
            continue
        line.append(char)
        if char == "\n":
            out.append("".join(line))
            line = []
        i += 1
    if line:
        out.append("".join(line))
    return "".join(out)


def line_stats(text: str) -> dict[str, int]:
    lines = text.splitlines()
    blank = 0
    comments = 0
    for line in lines:
        stripped = line.strip()
        if not stripped:
            blank += 1
        elif stripped.startswith(("//", "#", "/*", "*", "*/")):
            comments += 1
    return {
        "loc": len(lines),
        "blank_loc": blank,
        "comment_loc": comments,
        "code_loc": len(lines) - blank - comments,
    }


def complexity(text: str, language: str) -> tuple[int, int]:
    clean = strip_comments_and_strings(text)
    if language == "shell":
        decisions = len(re.findall(r"\b(if|elif|for|while|until|case)\b|&&|\|\|", clean))
    else:
        decisions = len(re.findall(r"\b(if|for|case|catch|switch)\b|&&|\|\||\?", clean))
    return 1 + decisions, decisions


def function_count(text: str, language: str) -> int:
    clean = strip_comments_and_strings(text)
    if language == "go":
        return len(re.findall(r"\bfunc\s+(?:\([^)]*\)\s*)?[A-Za-z_][A-Za-z0-9_]*\s*\(", clean))
    if language == "shell":
        return len(re.findall(r"\bfunction\s+[A-Za-z_][A-Za-z0-9_]*|\b[A-Za-z_][A-Za-z0-9_]*\s*\(\s*\)\s*\{", clean))
    if language == "python":
        return len(re.findall(r"\bdef\s+[A-Za-z_][A-Za-z0-9_]*\s*\(", clean))
    return len(re.findall(r"\b[A-Za-z_][A-Za-z0-9_:<>]*\s*\([^;{}]*\)\s*\{", clean))


def normalized_code_lines(text: str) -> list[tuple[int, str]]:
    clean = strip_comments_and_strings(text)
    rows: list[tuple[int, str]] = []
    for number, line in enumerate(clean.splitlines(), 1):
        normalized = re.sub(r"\s+", " ", line).strip()
        normalized = re.sub(r"\b\d+(?:\.\d+)?\b", "<num>", normalized)
        if normalized:
            rows.append((number, normalized))
    return rows


def responsibilities(path: str, text: str, language: str) -> list[str]:
    haystack = f"{path}\n{text[:100000]}".lower()
    result = []
    for category, patterns in RESPONSIBILITY_RULES.items():
        if any(re.search(pattern, haystack) for pattern in patterns):
            result.append(category)
    if path.endswith("_test.go") or "/tests/" in path or path.startswith("tests/"):
        result.append("testing")
    return sorted(set(result)) or ["general"]


@dataclass
class FileMetrics:
    path: str
    language: str
    package: str
    loc: int
    blank_loc: int
    comment_loc: int
    code_loc: int
    complexity: int
    decision_points: int
    function_count: int
    responsibilities: list[str]

    def as_dict(self) -> dict:
        return asdict(self)


def package_for(path: str) -> str:
    directory = Path(path).parent.as_posix()
    return "." if directory == "." else directory


def analyze_tree(root: Path) -> tuple[dict[str, FileMetrics], dict]:
    files: dict[str, FileMetrics] = {}
    duplicate_locations: defaultdict[str, list[tuple[str, int]]] = defaultdict(list)
    for relative, path, language in iter_source_files(root):
        text = read_text(path)
        stats = line_stats(text)
        complexity_value, decisions = complexity(text, language)
        metric = FileMetrics(
            path=relative,
            language=language,
            package=package_for(relative),
            **stats,
            complexity=complexity_value,
            decision_points=decisions,
            function_count=function_count(text, language),
            responsibilities=responsibilities(relative, text, language),
        )
        files[relative] = metric
        rows = normalized_code_lines(text)
        for index in range(0, max(0, len(rows) - WINDOW_SIZE + 1)):
            block = tuple(value for _, value in rows[index : index + WINDOW_SIZE])
            digest = hashlib.sha256("\n".join(block).encode("utf-8")).hexdigest()
            duplicate_locations[digest].append((relative, rows[index][0]))

    duplicate_blocks = []
    duplicate_lines = 0
    for digest, locations in sorted(duplicate_locations.items()):
        if len(locations) < 2:
            continue
        duplicate_lines += (len(locations) - 1) * WINDOW_SIZE
        duplicate_blocks.append({
            "hash": digest[:16],
            "occurrences": len(locations),
            "locations": [{"path": p, "line": line} for p, line in sorted(locations)],
        })
    duplicate_blocks.sort(key=lambda item: (-item["occurrences"], item["hash"]))

    by_language = Counter(metric.language for metric in files.values())
    by_responsibility = Counter(
        category for metric in files.values() for category in metric.responsibilities
    )
    mixed = [
        metric for metric in files.values()
        if len([r for r in metric.responsibilities if r != "testing"]) >= 3
    ]
    totals = {
        "files": len(files),
        "loc": sum(metric.loc for metric in files.values()),
        "blank_loc": sum(metric.blank_loc for metric in files.values()),
        "comment_loc": sum(metric.comment_loc for metric in files.values()),
        "code_loc": sum(metric.code_loc for metric in files.values()),
        "complexity": sum(metric.complexity for metric in files.values()),
        "max_file_complexity": max((metric.complexity for metric in files.values()), default=0),
        "function_count": sum(metric.function_count for metric in files.values()),
        "duplicate_blocks": len(duplicate_blocks),
        "duplicate_lines": duplicate_lines,
        "duplicate_ratio": round(duplicate_lines / max(1, sum(metric.code_loc for metric in files.values())), 6),
        "responsibility_count": sum(len(metric.responsibilities) for metric in files.values()),
        "avg_responsibilities_per_file": round(
            sum(len(metric.responsibilities) for metric in files.values()) / max(1, len(files)), 6
        ),
        "mixed_responsibility_files": len(mixed),
    }
    summary = {
        "totals": totals,
        "languages": dict(sorted(by_language.items())),
        "responsibilities": dict(sorted(by_responsibility.items())),
        "mixed_files": [metric.path for metric in sorted(mixed, key=lambda item: (-len(item.responsibilities), item.path))[:25]],
        "duplicate_samples": duplicate_blocks[:25],
        "files": {path: files[path].as_dict() for path in sorted(files)},
    }
    return files, summary


def delta(current: dict[str, FileMetrics], base: dict[str, FileMetrics]) -> dict:
    paths = sorted(set(current) | set(base))
    changed = []
    for path in paths:
        before = base.get(path)
        after = current.get(path)
        before_loc = before.loc if before else 0
        after_loc = after.loc if after else 0
        before_complexity = before.complexity if before else 0
        after_complexity = after.complexity if after else 0
        if before is None or after is None or before_loc != after_loc or before_complexity != after_complexity or (before and after and before.responsibilities != after.responsibilities):
            changed.append({
                "path": path,
                "status": "added" if before is None else "removed" if after is None else "changed",
                "loc_delta": after_loc - before_loc,
                "complexity_delta": after_complexity - before_complexity,
                "responsibility_delta": (len(after.responsibilities) if after else 0) - (len(before.responsibilities) if before else 0),
            })
    return {
        "files_added": sum(1 for path in paths if path not in base),
        "files_removed": sum(1 for path in paths if path not in current),
        "changed_files": len(changed),
        "top_loc_reductions": sorted(changed, key=lambda row: (row["loc_delta"], row["path"]))[:20],
        "top_loc_increases": sorted(changed, key=lambda row: (-row["loc_delta"], row["path"]))[:20],
        "top_complexity_reductions": sorted(changed, key=lambda row: (row["complexity_delta"], row["path"]))[:20],
        "top_complexity_increases": sorted(changed, key=lambda row: (-row["complexity_delta"], row["path"]))[:20],
        "changed": changed,
    }


def build_report(base_ref: str, head_ref: str, base_summary: dict, head_summary: dict, base_files: dict, head_files: dict) -> dict:
    base_totals = base_summary["totals"]
    head_totals = head_summary["totals"]
    totals_delta = {key: head_totals.get(key, 0) - base_totals.get(key, 0) for key in head_totals}
    return {
        "schema_version": SCHEMA_VERSION,
        "metric_definition": {
            "scope": "tracked source files in DataServer, RemoteCodex, shared, scripts, tests, cmd, internal, and deploy",
            "excluded": ["generated protobuf files", "vendor", "node_modules", "build", "dist", "__pycache__", ".pb-cache"],
            "loc": "physical lines, with blank_loc/comment_loc/code_loc classified deterministically",
            "complexity": "decision complexity = 1 + if/for/case/catch/switch plus &&, ||, ?: decision tokens; strings/comments removed",
            "duplication": f"duplicate normalized code windows of {WINDOW_SIZE} lines; duplicate_lines counts occurrences after the first",
            "responsibility": "stable category matches over repository-relative path and first 100000 bytes of source; mixed file means at least 3 non-testing categories",
        },
        "base_ref": base_ref,
        "head_ref": head_ref,
        "base": base_summary,
        "head": head_summary,
        "delta": {"totals": totals_delta, **delta(head_files, base_files)},
    }


def markdown(report: dict) -> str:
    base = report["base"]["totals"]
    head = report["head"]["totals"]
    change = report["delta"]["totals"]
    def signed(value):
        return f"{value:+d}" if isinstance(value, int) else f"{value:+.6f}"
    rows = [
        "# Refactoring Metrics",
        "",
        f"Schema: `{report['schema_version']}`  ",
        f"Base: `{report['base_ref']}`  ",
        f"Head: `{report['head_ref']}`",
        "",
        "| Metric | Base | Head | Delta |",
        "| --- | ---: | ---: | ---: |",
    ]
    for key in ("files", "loc", "code_loc", "complexity", "max_file_complexity", "duplicate_blocks", "duplicate_lines", "mixed_responsibility_files"):
        rows.append(f"| `{key}` | {base[key]} | {head[key]} | {signed(change[key])} |")
    rows += [
        f"| `duplicate_ratio` | {base['duplicate_ratio']:.6f} | {head['duplicate_ratio']:.6f} | {change['duplicate_ratio']:+.6f} |",
        f"| `avg_responsibilities_per_file` | {base['avg_responsibilities_per_file']:.6f} | {head['avg_responsibilities_per_file']:.6f} | {change['avg_responsibilities_per_file']:+.6f} |",
        "",
        "## Largest Changes",
        "",
        "### LOC reductions",
        "",
    ]
    for row in report["delta"]["top_loc_reductions"][:10]:
        rows.append(f"- `{row['path']}`: LOC {row['loc_delta']:+d}, complexity {row['complexity_delta']:+d}")
    rows += ["", "### LOC increases", ""]
    for row in report["delta"]["top_loc_increases"][:10]:
        rows.append(f"- `{row['path']}`: LOC {row['loc_delta']:+d}, complexity {row['complexity_delta']:+d}")
    rows += ["", "## Responsibility Mix", "", "| Category | Base | Head |", "| --- | ---: | ---: |"]
    categories = sorted(set(report["base"]["responsibilities"]) | set(report["head"]["responsibilities"]))
    for category in categories:
        rows.append(f"| `{category}` | {report['base']['responsibilities'].get(category, 0)} | {report['head']['responsibilities'].get(category, 0)} |")
    rows += ["", "## Definitions", "", "```text", json.dumps(report["metric_definition"], sort_keys=True, indent=2), "```", ""]
    return "\n".join(rows)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parents[2])
    parser.add_argument("--base-ref", required=True)
    parser.add_argument("--head-ref", default="HEAD")
    parser.add_argument("--out-dir", type=Path, default=Path("metrics-out"))
    args = parser.parse_args()
    repo_root = args.repo_root.resolve()
    out_dir = args.out_dir if args.out_dir.is_absolute() else repo_root / args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="velox-refactor-metrics-") as temporary:
        temp = Path(temporary)
        base_root = temp / "base"
        head_root = temp / "head"
        base_root.mkdir()
        head_root.mkdir()
        extract_ref(repo_root, args.base_ref, base_root)
        extract_ref(repo_root, args.head_ref, head_root)
        base_files, base_summary = analyze_tree(base_root)
        head_files, head_summary = analyze_tree(head_root)
    report = build_report(args.base_ref, args.head_ref, base_summary, head_summary, base_files, head_files)
    (out_dir / "refactor-metrics.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    (out_dir / "refactor-metrics.md").write_text(markdown(report), encoding="utf-8")
    print(markdown(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
