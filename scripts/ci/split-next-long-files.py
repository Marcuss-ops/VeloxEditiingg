#!/usr/bin/env python3
"""One-shot, idempotent splitter for the next requested long files."""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def write(path: str, text: str) -> None:
    target = ROOT / path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(text.rstrip() + "\n", encoding="utf-8")


def with_entry_nav(text: str, nav: str) -> str:
    lines = text.splitlines(keepends=True)
    if not lines:
        raise ValueError("empty markdown document")
    return lines[0].rstrip("\n") + "\n\n" + nav.rstrip() + "\n\n" + "".join(lines[1:]).lstrip("\n")


def validate_markdown(path: str) -> None:
    text = read(path)
    if text.count("```") % 2:
        raise RuntimeError(f"unbalanced Markdown fences: {path}")


def split_loc_baseline() -> None:
    source_path = "docs/metrics/loc-baseline.md"
    policy_path = "docs/metrics/loc-baseline-policy.md"
    history_path = "docs/metrics/loc-refactor-history.md"
    text = read(source_path)

    if "loc-baseline-policy.md" in text and (ROOT / policy_path).exists() and (ROOT / history_path).exists():
        return

    hotspot = re.search(r"(?m)^## 10\. Hotspot files", text)
    history = re.search(r"(?m)^## 15\.", text)
    if not hotspot or not history or hotspot.start() >= history.start():
        raise RuntimeError("LOC baseline split markers not found")

    part1 = text[: hotspot.start()].rstrip()
    part2 = text[hotspot.start() : history.start()].strip()
    part3 = text[history.start() :].strip()

    part2 = part2.replace(
        "Re-run them after each refactor round and append a `## Round N — Delta vs. baseline` section below §14.",
        "Re-run them after each refactor round and append the delta to `loc-refactor-history.md`.",
    )
    part3 = part3.replace(
        "This document is the single source of truth for LOC policy; the bash script enforces it.",
        "`loc-baseline-policy.md` is the source of truth for LOC policy; this document records the refactor rounds.",
    )

    write(
        source_path,
        with_entry_nav(
            part1,
            "> Document set: **Part 1 — Baseline maps** · [Part 2 — Hotspots, policy and methodology](loc-baseline-policy.md) · [Part 3 — Refactor history](loc-refactor-history.md)",
        ),
    )
    write(
        policy_path,
        "# LOC Baseline — Hotspots, policy and methodology\n\n"
        "> Document set: [Part 1 — Baseline maps](loc-baseline.md) · **Part 2 — Hotspots, policy and methodology** · [Part 3 — Refactor history](loc-refactor-history.md)\n\n"
        + part2,
    )
    write(
        history_path,
        "# LOC Refactor History\n\n"
        "> Document set: [Part 1 — Baseline maps](loc-baseline.md) · [Part 2 — Hotspots, policy and methodology](loc-baseline-policy.md) · **Part 3 — Refactor history**\n\n"
        + part3,
    )

    for path in (source_path, policy_path, history_path):
        validate_markdown(path)



def split_architecture_archive() -> None:
    source_path = "docs/archive/architecture-pre-grpc.md"
    runtime_path = "docs/archive/architecture-pre-grpc-contract-and-runtime.md"
    artifacts_path = "docs/archive/architecture-pre-grpc-artifacts-and-assets.md"
    text = read(source_path)

    if runtime_path.split("/")[-1] in text and (ROOT / runtime_path).exists() and (ROOT / artifacts_path).exists():
        return

    contract = re.search(r"(?m)^## 5\. Contratto Go", text)
    artifacts = re.search(r"(?m)^## 10\. Artifact Pipeline", text)
    if not contract or not artifacts or contract.start() >= artifacts.start():
        raise RuntimeError("architecture archive split markers not found")

    part1 = text[: contract.start()].rstrip()
    part2 = text[contract.start() : artifacts.start()].strip()
    part3 = text[artifacts.start() :].strip()

    write(
        source_path,
        with_entry_nav(
            part1,
            "> Archived document set: **Part 1 — Deploy, workers and job pipeline** · [Part 2 — Contracts and runtime](architecture-pre-grpc-contract-and-runtime.md) · [Part 3 — Artifacts, delivery and assets](architecture-pre-grpc-artifacts-and-assets.md)",
        ),
    )
    write(
        runtime_path,
        "# Velox Architecture pre-gRPC — Contracts and runtime\n\n"
        "> Archived document set: [Part 1 — Deploy, workers and job pipeline](architecture-pre-grpc.md) · **Part 2 — Contracts and runtime** · [Part 3 — Artifacts, delivery and assets](architecture-pre-grpc-artifacts-and-assets.md)\n\n"
        + part2,
    )
    write(
        artifacts_path,
        "# Velox Architecture pre-gRPC — Artifacts, delivery and assets\n\n"
        "> Archived document set: [Part 1 — Deploy, workers and job pipeline](architecture-pre-grpc.md) · [Part 2 — Contracts and runtime](architecture-pre-grpc-contract-and-runtime.md) · **Part 3 — Artifacts, delivery and assets**\n\n"
        + part3,
    )

    for path in (source_path, runtime_path, artifacts_path):
        validate_markdown(path)


REGION_RE = re.compile(
    r"(?m)^// =+\n// (?P<label>(?:#region|EXTRA)[^\n]*)\n// =+\n"
)
TEST_RE = re.compile(r"(?m)^func (Test\w+)\(")


def service_bucket(label: str) -> str:
    match = re.match(r"#region\s+(\d+)", label)
    if match:
        region = int(match.group(1))
        if region in {1, 2, 3, 4, 11, 12, 16}:
            return "begin"
        if region in {5, 6, 7}:
            return "receive"
        if region in {8, 9, 10, 13, 14, 15}:
            return "finalize"
    lowered = label.lower()
    if "empty input" in lowered:
        return "begin"
    if "receive out-of-order" in lowered:
        return "receive"
    raise RuntimeError(f"unassigned service_test block: {label}")


def split_artifact_service_tests() -> None:
    source_path = "DataServer/internal/artifacts/service_test.go"
    outputs = {
        "begin": "DataServer/internal/artifacts/service_begin_upload_test.go",
        "receive": "DataServer/internal/artifacts/service_receive_test.go",
        "finalize": "DataServer/internal/artifacts/service_finalize_test.go",
    }
    source = read(source_path)

    if "service_begin_upload_test.go" in source and all((ROOT / p).exists() for p in outputs.values()):
        return

    markers = list(REGION_RE.finditer(source))
    if not markers:
        raise RuntimeError("service_test region markers not found")

    package_match = re.search(r"(?m)^package artifacts\s*$", source)
    import_match = re.search(r"(?ms)^import \(.*?^\)\s*$", source)
    if not package_match or not import_match:
        raise RuntimeError("service_test package/import block not found")

    blocks: dict[str, list[str]] = {key: [] for key in outputs}
    for index, marker in enumerate(markers):
        end = markers[index + 1].start() if index + 1 < len(markers) else len(source)
        blocks[service_bucket(marker.group("label"))].append(source[marker.start() : end].rstrip())

    before_tests = set(TEST_RE.findall(source))
    fixture = source[: markers[0].start()].rstrip()
    fixture += (
        "\n\n// Test cases are split by lifecycle concern into "
        "service_begin_upload_test.go, service_receive_test.go and "
        "service_finalize_test.go.\n"
    )
    write(source_path, fixture)

    import_block = import_match.group(0)
    for bucket, path in outputs.items():
        body = "\n\n".join(blocks[bucket])
        write(path, f"package artifacts\n\n{import_block}\n\n{body}")

    after_text = read(source_path) + "\n" + "\n".join(read(path) for path in outputs.values())
    after_tests = set(TEST_RE.findall(after_text))
    if before_tests != after_tests:
        missing = sorted(before_tests - after_tests)
        added = sorted(after_tests - before_tests)
        raise RuntimeError(f"service test set changed; missing={missing}, added={added}")



def assert_line_limits() -> None:
    limits = {
        "docs/metrics/loc-baseline.md": 600,
        "docs/metrics/loc-baseline-policy.md": 600,
        "docs/metrics/loc-refactor-history.md": 600,
        "docs/archive/architecture-pre-grpc.md": 500,
        "docs/archive/architecture-pre-grpc-contract-and-runtime.md": 500,
        "docs/archive/architecture-pre-grpc-artifacts-and-assets.md": 500,
        "DataServer/internal/artifacts/service_test.go": 400,
        "DataServer/internal/artifacts/service_begin_upload_test.go": 400,
        "DataServer/internal/artifacts/service_receive_test.go": 400,
        "DataServer/internal/artifacts/service_finalize_test.go": 400,
    }
    failures = []
    for path, limit in limits.items():
        lines = len(read(path).splitlines())
        print(f"{lines:4d} {path}")
        if lines > limit:
            failures.append(f"{path}: {lines} > {limit}")
    if failures:
        raise RuntimeError("line limits exceeded: " + "; ".join(failures))


def main() -> None:
    split_loc_baseline()
    split_architecture_archive()
    split_artifact_service_tests()
    assert_line_limits()


if __name__ == "__main__":
    main()
