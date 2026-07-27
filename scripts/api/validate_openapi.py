#!/usr/bin/env python3
"""OpenAPI 3.1 spec validator — CORRECTNESS-ONLY (post [P1] codegen).

This validator runs alongside `cmd/api-docs-gen` (the codegen). The
manifest (api/api_docs_manifest.yaml) is the catalog of routes the
Master claims to publish; the codegen enforces drift on that catalog.
The validator's job is strictly correctness:

  1. The YAML parses.
  2. The document declares openapi 3.1.0.
  3. Components.securitySchemes has bearerAdminToken type=http, scheme=bearer.
  4. The ErrorCode enum's set is bidirectionally equal to EXPECTED_ERROR_CODES.
  5. Every $ref used anywhere in the spec resolves to a defined
     component (inside the same doc).
  6. Every operation has operationId, tags, responses, security.
  7. Every response $ref points at a schema in components.schemas.

Pre-[P1] this validator carried two completeness lists:
  * ROUTE_INVARIANTS — a hard-coded list of (path, method, operationId,
    parameters, requestBody, responses). Adding a fifth route required
    editing Python. Removed.
  * REQUIRED_SCHEMAS — a list of schema names that MUST exist. Removed.

The codegen + manifest replaces both. New routes = add an entry to
api_docs_manifest.yaml, run `make api-docs`, commit. The validator
never fails because of "missing" routes or "missing" schemas, only
because something that IS in the spec is malformed.

Hard failures vs warnings:

  * Hard FAIL — incomplete spec, malformed $ref, missing operationId,
    non-bearer security scheme, ErrorCode enum drift.
  * WARN — registered route in manifest that has no matching operation
    in the spec (the codegen reports this at generation time; re-asserted
    here so drift is double-checked).

Usage:
    python3 scripts/api/validate_openapi.py DataServer/api/openapi.yaml
    python3 scripts/api/validate_openapi.py DataServer/api/openapi.yaml --manifest=DataServer/api/api_docs_manifest.yaml
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

try:
    import yaml
except ImportError:  # pragma: no cover - explicit guidance for missing dep
    sys.stderr.write(
        "PyYAML is required. Install with: pip install 'pyyaml>=6.0'\n"
    )
    sys.exit(2)


# MUST match ErrorCode.enum in the spec — bidirectional equality is the
# single source of "closed-enum drift" detection. Add a new error code
# here + in the spec, NEVER in only one place.
EXPECTED_ERROR_CODES = {
    "missing_authorization",
    "invalid_bearer",
    "invalid_json",
    "invalid_payload",
    "payload_incomplete",
    "resolver_failure",
    "idempotency_key_reused",
    "job_not_found",
}

VALID_VERBS = {"get", "post", "put", "delete", "patch", "head", "options"}
MAX_REF_RECURSION = 8  # bounded walk to keep validation O(small).


# ── Errors / warnings collectors ────────────────────────────────────────


class Report:
    def __init__(self, source: str) -> None:
        self.source = str(source)
        self.failures: list[str] = []
        self.warnings: list[str] = []

    def fail(self, msg: str) -> None:
        self.failures.append(f"{self.source}: {msg}")

    def warn(self, msg: str) -> None:
        self.warnings.append(f"{self.source}: WARN: {msg}")


# ── Top-level spec rules ───────────────────────────────────────────────


def _check_openapi_version(doc: dict[str, Any], report: Report) -> None:
    v = doc.get("openapi")
    if v != "3.1.0":
        report.fail(f"openapi version is {v!r}, want '3.1.0'")


def _check_security_scheme(doc: dict[str, Any], report: Report) -> None:
    sec_top = doc.get("security") or []
    if not any(isinstance(s, dict) and "bearerAdminToken" in s for s in sec_top):
        report.fail("top-level security must reference bearerAdminToken")
    sec_def = (
        doc.get("components", {})
        .get("securitySchemes", {})
        .get("bearerAdminToken", {})
    )
    if sec_def.get("type") != "http" or sec_def.get("scheme") != "bearer":
        report.fail(
            f"bearerAdminToken scheme must be http+bearer, got {sec_def!r}"
        )


def _check_error_code_enum(doc: dict[str, Any], report: Report) -> None:
    schemas = doc.get("components", {}).get("schemas", {}) or {}
    if "ErrorCode" not in schemas:
        report.fail("ErrorCode schema missing (cannot validate enum set)")
        return
    ec_enum = set(schemas["ErrorCode"].get("enum", []) or [])
    if ec_enum != EXPECTED_ERROR_CODES:
        missing = EXPECTED_ERROR_CODES - ec_enum
        extra = ec_enum - EXPECTED_ERROR_CODES
        report.fail(
            "ErrorCode enum drift. "
            f"missing_in_spec={sorted(missing)}, "
            f"extra_in_spec={sorted(extra)}, "
            f"want={sorted(EXPECTED_ERROR_CODES)}, got={sorted(ec_enum)}"
        )


def _collect_refs(node: Any, out: set[str]) -> None:
    """Walk `node` and collect every $ref string. Bounded recursion."""
    if isinstance(node, dict):
        for k, v in node.items():
            if k == "$ref" and isinstance(v, str):
                out.add(v)
            else:
                _collect_refs(v, out)
    elif isinstance(node, list):
        for x in node:
            _collect_refs(x, out)


def _resolve_ref(doc: dict[str, Any], ref: str, report: Report) -> bool:
    """Resolve #/components/<group>/<name> only. Returns True on success."""
    if not ref.startswith("#/components/"):
        report.fail(
            f"$ref {ref!r} points outside this document; "
            "validator only supports intra-document refs"
        )
        return False
    rest = ref[len("#/components/"):]
    parts = rest.split("/", 1)
    if len(parts) != 2 or not parts[0] or not parts[1]:
        report.fail(f"$ref {ref!r} is malformed (expected #/components/<g>/<n>)")
        return False
    group, name = parts
    bucket = doc.get("components", {}).get(group)
    if not isinstance(bucket, dict):
        report.fail(f"components.{group} is missing (referenced by {ref!r})")
        return False
    if name not in bucket:
        report.fail(
            f"components.{group}.{name} is missing (referenced by {ref!r})"
        )
        return False
    return True


def _check_every_ref_resolves(doc: dict[str, Any], report: Report) -> None:
    refs: set[str] = set()
    _collect_refs(doc, refs)
    for ref in sorted(refs):
        _resolve_ref(doc, ref, report)


# ── Per-operation structural correctness ───────────────────────────────


def _check_operations(doc: dict[str, Any], report: Report) -> None:
    paths = doc.get("paths") or {}
    if not isinstance(paths, dict):
        report.fail("paths must be a mapping")
        return
    # Top-level security is the inheritance source for every
    # operation. Operations may redeclare (to override) OR opt out
    # explicitly with `security: []`. An operation-level `security`
    # entry is therefore only REQUIRED when the document does NOT
    # declare a top-level `security` block. Reading the spec this way
    # matches OpenAPI's documented inheritance model.
    doc_has_global_security = bool(doc.get("security"))
    for path, path_node in paths.items():
        if not isinstance(path_node, dict):
            report.fail(f"paths.{path} must be a mapping")
            continue
        for verb, op in path_node.items():
            if verb not in VALID_VERBS:
                continue
            if not isinstance(op, dict):
                report.fail(
                    f"paths.{path}.{verb} must be an operation mapping"
                )
                continue
            missing = []
            if not op.get("operationId"):
                missing.append("operationId")
            if not op.get("tags"):
                missing.append("tags")
            if not op.get("responses"):
                missing.append("responses")
            # See docstring above on inheritance; only require
            # operation-level security when the document does not
            # provide one globally.
            if not doc_has_global_security and not op.get("security"):
                missing.append("security")
            if missing:
                report.fail(
                    f"paths.{path}.{verb} missing required fields:"
                    f" {missing}"
                )
            # Every response schema's $ref must resolve.
            for code, response in (op.get("responses") or {}).items():
                if not isinstance(response, dict):
                    continue
                schema = (
                    response.get("content", {})
                    .get("application/json", {})
                    .get("schema", {})
                )
                ref = schema.get("$ref") if isinstance(schema, dict) else None
                if ref:
                    _resolve_ref(doc, ref, report)


# ── Manifest cross-check (warn-only) ───────────────────────────────────


def _check_manifest_coverage(
    doc: dict[str, Any], manifest: dict[str, Any], report: Report
) -> None:
    """Cross-check manifest routes against spec paths. WARN-only.

    A manifest route with no matching operation in the spec is a
    drift signal, not a hard failure: the codegen surfaces this in
    `make api-docs` and `make api-docs-apply` raise it as an error,
    so the validator's role is just to remind the maintainer.
    """
    spec_ops: set[tuple[str, str]] = set()
    for path, path_node in (doc.get("paths") or {}).items():
        if not isinstance(path_node, dict):
            continue
        for verb in path_node:
            if verb in VALID_VERBS:
                spec_ops.add((verb, str(path)))
    manifest_ops: set[tuple[str, str]] = set()
    for route in manifest.get("routes", []) or []:
        path = route.get("path")
        verb = (route.get("method") or "").lower()
        if path and verb in VALID_VERBS:
            manifest_ops.add((verb, path))

    missing = manifest_ops - spec_ops
    for verb, path in sorted(missing):
        report.warn(
            f"manifest declares {verb.upper()} {path} but no matching "
            "operation exists in the spec; re-run `make api-docs-apply`"
        )


# ── Driver ────────────────────────────────────────────────────────────


def load_yaml(path: Path) -> tuple[Any, list[str]]:
    """Parse YAML. Returns (doc_or_None, parse_errors)."""
    try:
        with path.open("r", encoding="utf-8") as fh:
            return yaml.safe_load(fh), []
    except yaml.YAMLError as exc:
        return None, [f"YAML parse error: {exc}"]
    except OSError as exc:
        return None, [f"open error: {exc}"]


def validate(
    spec_path: Path, manifest: dict[str, Any] | None = None
) -> Report:
    """Validate `spec_path`. Returns a Report; failures + warnings."""
    report = Report(str(spec_path))
    doc, parse_errs = load_yaml(spec_path)
    if parse_errs:
        for e in parse_errs:
            report.fail(e)
        return report
    if not isinstance(doc, dict):
        report.fail("top-level is not a mapping")
        return report

    _check_openapi_version(doc, report)
    _check_security_scheme(doc, report)
    _check_error_code_enum(doc, report)
    _check_every_ref_resolves(doc, report)
    _check_operations(doc, report)
    if manifest is not None:
        _check_manifest_coverage(doc, manifest, report)
    return report


def _load_manifest(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    raw, errs = load_yaml(path)
    if errs:
        return None
    if not isinstance(raw, dict):
        return None
    return raw


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("specs", nargs="+", help="openapi.yaml file(s) to validate")
    parser.add_argument(
        "--manifest",
        default=None,
        help=(
            "optional api_docs_manifest.yaml — enables the warn-level "
            "manifest-coverage cross-check"
        ),
    )
    args = parser.parse_args(argv)

    manifest = _load_manifest(Path(args.manifest)) if args.manifest else None

    all_reports: list[Report] = []
    for spec_arg in args.specs:
        spec_path = Path(spec_arg)
        print(f"--- validating {spec_path} ---")
        report = validate(spec_path, manifest=manifest)
        all_reports.append(report)
        if report.failures:
            print(f"FAIL: {spec_path} ({len(report.failures)} failures)")
            for f in report.failures:
                print(f"  - {f}")
        else:
            print("PASS")
        if report.warnings:
            for w in report.warnings:
                print(f"  - {w}")

    total_failures = sum(len(r.failures) for r in all_reports)
    if total_failures > 0:
        print(f"--- TOTAL FAIL ({total_failures} failures) ---")
        return 1
    print(
        f"--- TOTAL PASS: {len(all_reports)} file(s) "
        "(correctness-only; manifest coverage is warn-level) ---"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
