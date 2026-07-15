#!/usr/bin/env python3
"""Split the four user-requested long files without changing test bodies."""

from __future__ import annotations

import math
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class TestDecl:
    name: str
    start: int
    end: int
    text: str


def go_header(text: str) -> tuple[str, str]:
    package_match = re.search(r"(?m)^package\s+([A-Za-z_][A-Za-z0-9_]*)\s*$", text)
    if package_match is None:
        raise RuntimeError("package declaration not found")
    import_match = re.search(
        r'(?ms)^import\s*\(.*?^\)\s*|(?m)^import\s+"[^"]+"\s*$',
        text,
    )
    return package_match.group(1), import_match.group(0).strip() if import_match else ""


def matching_brace(text: str, open_pos: int) -> int:
    depth = 0
    state = "normal"
    index = open_pos
    while index < len(text):
        char = text[index]
        nxt = text[index + 1] if index + 1 < len(text) else ""
        if state == "normal":
            if char == "/" and nxt == "/":
                state = "line_comment"
                index += 2
                continue
            if char == "/" and nxt == "*":
                state = "block_comment"
                index += 2
                continue
            if char == '"':
                state = "string"
            elif char == "`":
                state = "raw_string"
            elif char == "'":
                state = "rune"
            elif char == "{":
                depth += 1
            elif char == "}":
                depth -= 1
                if depth == 0:
                    return index + 1
            index += 1
            continue
        if state == "line_comment":
            if char == "\n":
                state = "normal"
            index += 1
            continue
        if state == "block_comment":
            if char == "*" and nxt == "/":
                state = "normal"
                index += 2
            else:
                index += 1
            continue
        if state == "raw_string":
            if char == "`":
                state = "normal"
            index += 1
            continue
        if state in {"string", "rune"}:
            if char == "\\":
                index += 2
                continue
            if (state == "string" and char == '"') or (state == "rune" and char == "'"):
                state = "normal"
            index += 1
    raise RuntimeError("unbalanced Go function braces")


def include_leading_comments(text: str, func_start: int, lower_bound: int) -> int:
    line_start = text.rfind("\n", lower_bound, func_start) + 1
    start = line_start
    cursor = line_start
    while cursor > lower_bound:
        previous_end = cursor - 1
        previous_start = text.rfind("\n", lower_bound, previous_end) + 1
        line = text[previous_start:previous_end].strip()
        if line == "" or line.startswith("//"):
            start = previous_start
            cursor = previous_start
            continue
        break
    return max(start, lower_bound)


def extract_tests(text: str) -> list[TestDecl]:
    matches = list(re.finditer(r"(?m)^func\s+(Test[A-Za-z0-9_]+)\s*\(", text))
    declarations: list[TestDecl] = []
    lower_bound = 0
    for match in matches:
        open_pos = text.find("{", match.end())
        if open_pos < 0:
            raise RuntimeError(f"opening brace not found for {match.group(1)}")
        end = matching_brace(text, open_pos)
        if end < len(text) and text[end] == "\r":
            end += 1
        if end < len(text) and text[end] == "\n":
            end += 1
        start = include_leading_comments(text, match.start(), lower_bound)
        declarations.append(TestDecl(match.group(1), start, end, text[start:end].strip()))
        lower_bound = end
    return declarations


def remove_tests(text: str, declarations: list[TestDecl]) -> str:
    result = text
    for declaration in reversed(declarations):
        result = result[: declaration.start] + result[declaration.end :]
    return re.sub(r"\n{4,}", "\n\n\n", result).rstrip() + "\n"


def write_test_file(path: Path, package: str, imports: str, declarations: list[TestDecl]) -> None:
    if not declarations:
        raise RuntimeError(f"empty split group for {path}")
    header = f"package {package}\n\n"
    if imports:
        header += imports + "\n\n"
    body = "\n\n".join(declaration.text for declaration in declarations)
    path.write_text(header + body.rstrip() + "\n", encoding="utf-8")


def balanced_groups(tests: list[TestDecl], count: int) -> list[list[TestDecl]]:
    groups: list[list[TestDecl]] = []
    remaining = list(tests)
    for index in range(count):
        take = math.ceil(len(remaining) / (count - index))
        groups.append(remaining[:take])
        remaining = remaining[take:]
    return groups


def split_go_tests(
    source: str,
    destinations: list[str],
    grouper: Callable[[list[TestDecl]], list[list[TestDecl]]],
) -> list[Path]:
    source_path = ROOT / source
    if not source_path.exists():
        print(f"{source}: source absent; assuming already split")
        return [ROOT / destination for destination in destinations if (ROOT / destination).exists()]

    text = source_path.read_text(encoding="utf-8")
    package, imports = go_header(text)
    tests = extract_tests(text)
    if not tests:
        print(f"{source}: no tests remain; preserving existing split")
        return [source_path, *(ROOT / destination for destination in destinations if (ROOT / destination).exists())]

    groups = grouper(tests)
    if len(groups) != len(destinations) or any(not group for group in groups):
        groups = balanced_groups(tests, len(destinations))
    if any(not group for group in groups):
        raise RuntimeError(f"{source}: invalid grouping {[len(group) for group in groups]}")

    source_path.write_text(remove_tests(text, tests), encoding="utf-8")
    generated = []
    for destination, group in zip(destinations, groups, strict=True):
        destination_path = ROOT / destination
        write_test_file(destination_path, package, imports, group)
        generated.append(destination_path)

    print(f"{source}: moved {len(tests)} tests -> {[len(group) for group in groups]}")
    return [source_path, *generated]


def ingest_groups(tests: list[TestDecl]) -> list[list[TestDecl]]:
    identity: list[TestDecl] = []
    result: list[TestDecl] = []
    rollup: list[TestDecl] = []
    identity_words = ("Identity", "Spoof", "Imperson", "LeaseMismatch", "AttemptNumber")
    rollup_words = ("TransitionJob", "Rollup", "AwaitingArtifact", "AllTasks", "CommitGate", "JobStatus")
    for test in tests:
        if any(word in test.name for word in identity_words):
            identity.append(test)
        elif any(word in test.name for word in rollup_words):
            rollup.append(test)
        else:
            result.append(test)
    return [identity, result, rollup]


def artifact_groups(tests: list[TestDecl]) -> list[list[TestDecl]]:
    begin = [test for test in tests if test.name.startswith("TestBeginUpload")]
    receive = [test for test in tests if test.name.startswith("TestReceive")]
    finalize = [test for test in tests if test.name.startswith("TestFinalize")]
    assigned = {test.name for test in [*begin, *receive, *finalize]}
    finalize.extend(test for test in tests if test.name not in assigned)
    return [begin, receive, finalize]


def forwarding_groups(tests: list[TestDecl]) -> list[list[TestDecl]]:
    persistence: list[TestDecl] = []
    claims: list[TestDecl] = []
    lifecycle: list[TestDecl] = []
    persistence_words = ("Insert", "Get", "Payload", "Source")
    claim_words = ("Claim", "Renew", "Release", "Lease")
    for test in tests:
        if any(word in test.name for word in claim_words):
            claims.append(test)
        elif any(word in test.name for word in persistence_words):
            persistence.append(test)
        else:
            lifecycle.append(test)
    return [persistence, claims, lifecycle]


def split_archived_document() -> list[Path]:
    source = ROOT / "docs/archive/architecture-pre-grpc.md"
    if not source.exists():
        return []
    text = source.read_text(encoding="utf-8")
    markers = (
        "## 1. Deploy del Master Server",
        "## 4. Ciclo di Vita Completo di un Job",
        "## 8. Progress Streaming (FFmpeg → Dashboard)",
    )
    if not all(marker in text for marker in markers):
        print("architecture-pre-grpc.md: already split or historical markers changed")
        return [source, *ROOT.glob("docs/archive/architecture-pre-grpc-*.md")]

    section_1, section_4, section_8 = (text.index(marker) for marker in markers)
    note = "> Snapshot storico mantenuto come riferimento; non descrive l'architettura canonica corrente."
    parts = [
        (
            ROOT / "docs/archive/architecture-pre-grpc-deploy-workers.md",
            "# Velox Architecture Pre-gRPC — Deploy e Worker",
            text[section_1:section_4],
        ),
        (
            ROOT / "docs/archive/architecture-pre-grpc-job-pipeline.md",
            "# Velox Architecture Pre-gRPC — Job e Video Pipeline",
            text[section_4:section_8],
        ),
        (
            ROOT / "docs/archive/architecture-pre-grpc-runtime-artifacts.md",
            "# Velox Architecture Pre-gRPC — Runtime, Artifact e Delivery",
            text[section_8:],
        ),
    ]
    for path, title, body in parts:
        path.write_text(f"{title}\n\n{note}\n\n{body.strip()}\n", encoding="utf-8")

    source.write_text(
        "# Velox Architecture — Pre-gRPC Archive\n\n"
        f"{note}\n\n"
        "Il documento originale è stato separato per responsabilità:\n\n"
        "- [Deploy, worker remoti e comunicazione con il master](architecture-pre-grpc-deploy-workers.md)\n"
        "- [Ciclo di vita dei job, pipeline video nativa e contratto Go/C++](architecture-pre-grpc-job-pipeline.md)\n"
        "- [Runtime, concorrenza, artifact pipeline, delivery e struttura storica](architecture-pre-grpc-runtime-artifacts.md)\n",
        encoding="utf-8",
    )
    print("architecture-pre-grpc.md: split into three focused historical chapters")
    return [source, *(part[0] for part in parts)]


def validate_line_counts(paths: list[Path]) -> None:
    for path in sorted(set(paths)):
        if not path.exists():
            continue
        lines = len(path.read_text(encoding="utf-8").splitlines())
        print(f"{lines:4d} {path.relative_to(ROOT)}")
        if lines > 500:
            raise RuntimeError(f"{path.relative_to(ROOT)} remains too large: {lines} lines")


def main() -> None:
    affected: list[Path] = []
    affected.extend(
        split_go_tests(
            "DataServer/internal/ingest/service_test.go",
            [
                "DataServer/internal/ingest/service_identity_test.go",
                "DataServer/internal/ingest/service_result_test.go",
                "DataServer/internal/ingest/service_rollup_test.go",
            ],
            ingest_groups,
        )
    )
    affected.extend(
        split_go_tests(
            "DataServer/internal/artifacts/service_test.go",
            [
                "DataServer/internal/artifacts/service_begin_upload_test.go",
                "DataServer/internal/artifacts/service_receive_test.go",
                "DataServer/internal/artifacts/service_finalize_test.go",
            ],
            artifact_groups,
        )
    )
    affected.extend(
        split_go_tests(
            "DataServer/internal/store/store_creator_forwardings_test.go",
            [
                "DataServer/internal/store/store_creator_forwardings_persistence_test.go",
                "DataServer/internal/store/store_creator_forwardings_claim_test.go",
                "DataServer/internal/store/store_creator_forwardings_lifecycle_test.go",
            ],
            forwarding_groups,
        )
    )
    affected.extend(split_archived_document())
    validate_line_counts(affected)


if __name__ == "__main__":
    main()
