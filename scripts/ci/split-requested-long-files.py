#!/usr/bin/env python3
"""Split the four requested long files without rewriting test bodies."""

from __future__ import annotations

import math
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

ROOT = Path(__file__).resolve().parents[2]


@dataclass
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
    imports = import_match.group(0).strip() if import_match else ""
    return package_match.group(1), imports


def matching_brace(text: str, open_pos: int) -> int:
    depth = 0
    state = "normal"
    i = open_pos

    while i < len(text):
        char = text[i]
        nxt = text[i + 1] if i + 1 < len(text) else ""

        if state == "normal":
            if char == "/" and nxt == "/":
                state = "line_comment"
                i += 2
                continue
            if char == "/" and nxt == "*":
                state = "block_comment"
                i += 2
                continue
            if char == '"':
                state = "string"
                i += 1
                continue
            if char == "`":
                state = "raw_string"
                i += 1
                continue
            if char == "'":
                state = "rune"
                i += 1
                continue
            if char == "{":
                depth += 1
            elif char == "}":
                depth -= 1
                if depth == 0:
                    return i + 1
            i += 1
            continue

        if state == "line_comment":
            if char == "\n":
                state = "normal"
            i += 1
            continue

        if state == "block_comment":
            if char == "*" and nxt == "/":
                state = "normal"
                i += 2
            else:
                i += 1
            continue

        if state == "raw_string":
            if char == "`":
                state = "normal"
            i += 1
            continue

        if state in {"string", "rune"}:
            if char == "\\":
                i += 2
                continue
            if (state == "string" and char == '"') or (state == "rune" and char == "'"):
                state = "normal"
            i += 1
            continue

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


def split_go_tests(
    source: str,
    destinations: list[str],
    grouper: Callable[[list[TestDecl]], list[list[TestDecl]]],
) -> None:
    source_path = ROOT / source
    text = source_path.read_text(encoding="utf-8")
    package, imports = go_header(text)
    tests = extract_tests(text)
    groups = grouper(tests)

    if len(groups) != len(destinations) or any(not group for group in groups):
        raise RuntimeError(f"{source}: invalid grouping {[len(group) for group in groups]}")

    source_path.write_text(remove_tests(text, tests), encoding="utf-8")
    for destination, group in zip(destinations, groups, strict=True):
        write_test_file(ROOT / destination, package, imports, group)

    print(f"{source}: moved {len(tests)} tests -> {[len(group) for group in groups]}")


def balanced_groups(tests: list[TestDecl], count: int) -> list[list[TestDecl]]:
    groups: list[list[TestDecl]] = []
    remaining = list(tests)
    for index in range(count):
        take = math.ceil(len(remaining) / (count - index))
        groups.append(remaining[:take])
        remaining = remaining[take:]
    return groups


def handler_groups(tests: list[TestDecl]) -> list[list[TestDecl]]:
    identity: list[TestDecl] = []
    metrics: list[TestDecl] = []
    acknowledgements: list[TestDecl] = []

    for test in tests:
        if "Identity" in test.name or "Spoof" in test.name:
            identity.append(test)
        elif "Ack" in test.name or "ReportConflict" in test.name:
            acknowledgements.append(test)
        else:
            metrics.append(test)

    return [identity, metrics, acknowledgements]


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

    groups = [identity, result, rollup]
    return balanced_groups(tests, 3) if any(not group for group in groups) else groups


def split_documentation() -> None:
    baseline_path = ROOT / "docs/metrics/loc-baseline.md"
    text = baseline_path.read_text(encoding="utf-8")
    marker = re.search(r"(?m)^##\s+15(?:\.|\s)", text)

    if marker is None:
        later_headings = [
            heading
            for heading in re.finditer(r"(?m)^##\s+", text)
            if heading.start() > len(text) // 2
        ]
        if not later_headings:
            raise RuntimeError("loc-baseline.md: no suitable history split marker")
        marker = later_headings[0]

    baseline = text[: marker.start()].rstrip()
    history = text[marker.start() :].lstrip()

    (ROOT / "docs/metrics/loc-refactor-history.md").write_text(
        "# LOC Refactor History — VeloxEditingg\n\n"
        "> Extracted from `loc-baseline.md` to keep the baseline focused and maintainable.\n"
        "> Historical section numbering is intentionally preserved for existing references.\n\n"
        + history.rstrip()
        + "\n",
        encoding="utf-8",
    )

    baseline_path.write_text(
        baseline
        + "\n\n---\n\n"
        + "## 15. Refactor history\n\n"
        + "The detailed round-by-round LOC reduction record now lives in "
        + "[`loc-refactor-history.md`](./loc-refactor-history.md).\n",
        encoding="utf-8",
    )


def main() -> None:
    split_go_tests(
        "DataServer/internal/grpcserver/handler_reports_test.go",
        [
            "DataServer/internal/grpcserver/handler_reports_identity_test.go",
            "DataServer/internal/grpcserver/handler_reports_metrics_test.go",
            "DataServer/internal/grpcserver/handler_reports_ack_test.go",
        ],
        handler_groups,
    )
    split_go_tests(
        "DataServer/internal/artifacts/retry_budget_propagation_test.go",
        [
            "DataServer/internal/artifacts/retry_budget_plan_test.go",
            "DataServer/internal/artifacts/retry_budget_limits_test.go",
        ],
        lambda tests: balanced_groups(tests, 2),
    )
    split_go_tests(
        "DataServer/internal/ingest/service_test.go",
        [
            "DataServer/internal/ingest/service_identity_test.go",
            "DataServer/internal/ingest/service_result_test.go",
            "DataServer/internal/ingest/service_rollup_test.go",
        ],
        ingest_groups,
    )
    split_documentation()


if __name__ == "__main__":
    main()
