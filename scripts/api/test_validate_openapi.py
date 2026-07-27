# test_validate_openapi.py — pytest suite for scripts/api/validate_openapi.py.
#
# Goal: lock the validator's behaviour so a refactor to the drift-
# detector (IN_VARIANTS) or the correctness-ONLY checks cannot quietly
# pass / fail differently.
#
# Run: python3 -m pytest scripts/api/test_validate_openapi.py -v
#
# Each test builds a small in-memory spec, writes it to a tmp_path
# file via yaml.safe_dump, runs validate(spec_path), and asserts on
# the Report's failures/warnings lists. The minimal valid spec already
# contains the three IN_VARIANTS routes, so individual tests mutate one
# piece at a time to assert which check fired.
from __future__ import annotations

import sys
from pathlib import Path

import pytest
import yaml

# Make scripts/api/ importable as a top-level package.
sys.path.insert(0, str(Path(__file__).resolve().parent))

import validate_openapi as v  # noqa: E402


# Path to the real single-source-of-truth spec the rest of the
# project's CI validates against. Pinned here so the test suite
# doubles as a regression net for the live spec.
REAL_SPEC = (
    Path(__file__).resolve().parents[2]
    / "DataServer"
    / "api"
    / "openapi.yaml"
)


@pytest.fixture()
def tmp_yaml(tmp_path: Path):
    """Factory: write `data` (dict) to tmp_path/x.yaml and return Path."""

    def _write(data: dict, name: str = "spec.yaml") -> Path:
        p = tmp_path / name
        with p.open("w", encoding="utf-8") as fh:
            yaml.safe_dump(data, fh, sort_keys=False)
        return p

    return _write


def _minimal_valid_spec() -> dict:
    """A spec that satisfies every correctness check AND contains all
    three IN_VARIANTS routes. Use this as the base; mutate one thing
    per test to assert which check fired.
    """
    envelope = {
        "type": "object",
        "properties": {"error_code": {"type": "string"}},
    }
    err_codes = sorted(v.EXPECTED_ERROR_CODES)
    return {
        "openapi": "3.1.0",
        "security": [{"bearerAdminToken": []}],
        "components": {
            "securitySchemes": {
                "bearerAdminToken": {"type": "http", "scheme": "bearer"},
            },
            "schemas": {
                "ErrorCode": {"type": "string", "enum": err_codes},
                "ErrorEnvelope": envelope,
            },
        },
        "paths": {
            "/api/v1/creator/jobs": {
                "post": {
                    "operationId": "pushCreatorJob",
                    "tags": ["creator"],
                    "responses": {
                        "202": {
                            "description": "Accepted.",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "$ref": "#/components/schemas/ErrorEnvelope",
                                    },
                                },
                            },
                        },
                    },
                },
            },
            "/api/v1/jobs": {
                "post": {
                    "operationId": "submitJob",
                    "tags": ["jobs"],
                    "responses": {
                        "202": {
                            "description": "Accepted.",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "$ref": "#/components/schemas/ErrorEnvelope",
                                    },
                                },
                            },
                        },
                    },
                },
            },
            "/api/v1/jobs/{job_id}": {
                "get": {
                    "operationId": "getSubmittedJob",
                    "tags": ["jobs"],
                    "responses": {
                        "200": {
                            "description": "OK.",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "$ref": "#/components/schemas/ErrorEnvelope",
                                    },
                                },
                            },
                        },
                    },
                },
            },
        },
    }


# ── 1. Real spec round-trip ───────────────────────────────────────────


def test_real_openapi_passes() -> None:
    """The shipped DataServer/api/openapi.yaml must validate clean.

    Asserts on the live canonical spec (which the merge-mode codegen
    keeps in sync with apiwire + manifest) so a Codegen regression
    surfaces here, not just in cmd/api-docs-gen -ci.
    """
    if not REAL_SPEC.exists():
        pytest.skip(f"real spec not present at {REAL_SPEC}")
    report = v.validate(REAL_SPEC)
    assert report.failures == [], (
        f"expected real spec to pass, got failures: {report.failures}"
    )


# ── 2. Happy minimal spec ─────────────────────────────────────────────


def test_minimal_valid_spec_passes(tmp_yaml) -> None:
    spec = _minimal_valid_spec()
    p = tmp_yaml(spec)
    report = v.validate(p)
    assert report.failures == [], (
        f"expected minimal valid spec to pass, "
        f"got failures: {report.failures}"
    )


# ── 3. IN_VARIANTS drift detector ─────────────────────────────────────


def test_missing_invariant_route_fails(tmp_yaml) -> None:
    spec = _minimal_valid_spec()
    del spec["paths"]["/api/v1/jobs"]
    p = tmp_yaml(spec)
    report = v.validate(p)
    assert any(
        "IN_VARIANT MISSING" in f and "/api/v1/jobs" in f
        for f in report.failures
    ), f"expected /api/v1/jobs IN_VARIANT MISSING; got {report.failures}"


def test_missing_invariant_method_fails(tmp_yaml) -> None:
    """The path exists but the required method does NOT.

    Same code path as a fully-missing route — the IN_VARIANTS check
    explicitly requires (method, path).
    """
    spec = _minimal_valid_spec()
    # Replace POST /api/v1/jobs with DELETE on the same path.
    spec["paths"]["/api/v1/jobs"] = {
        "delete": {
            "operationId": "deleteSpec",
            "tags": ["jobs"],
            "responses": {"204": {"description": "No Content."}},
        },
    }
    p = tmp_yaml(spec)
    report = v.validate(p)
    assert any(
        "IN_VARIANT MISSING" in f
        and "/api/v1/jobs" in f
        and "POST" in f
        for f in report.failures
    ), f"expected POST /api/v1/jobs IN_VARIANT MISSING; got {report.failures}"


def test_extra_route_outside_invariants_passes(tmp_yaml) -> None:
    """Extra routes outside the IN_VARIANTS list must NOT fail.

    The whole point of IN_VARIANTS-as-guard-rail (not boundary) is that
    adding a new endpoint never trips this validator.
    """
    spec = _minimal_valid_spec()
    spec["paths"]["/api/v1/youtube/editor-sessions/abc"] = {
        "get": {
            "operationId": "editorSessionGet",
            "tags": ["youtube"],
            "responses": {
                "200": {
                    "description": "OK.",
                    "content": {
                        "application/json": {
                            "schema": {
                                "$ref": "#/components/schemas/ErrorEnvelope",
                            },
                        },
                    },
                },
            },
        },
    }
    p = tmp_yaml(spec)
    report = v.validate(p)
    assert report.failures == [], (
        f"expected extras to pass; got failures: {report.failures}"
    )


# ── 4. Correctness checks ─────────────────────────────────────────────


def test_broken_ref_fails(tmp_yaml) -> None:
    spec = _minimal_valid_spec()
    # 202 response on /api/v1/jobs points at a missing schema.
    spec["paths"]["/api/v1/jobs"]["post"]["responses"]["202"]["content"][
        "application/json"
    ]["schema"]["$ref"] = "#/components/schemas/DoesNotExist"
    p = tmp_yaml(spec)
    report = v.validate(p)
    assert any(
        "DoesNotExist" in f for f in report.failures
    ), f"expected missing-ref failure; got {report.failures}"


def test_errorcode_enum_drift_fails(tmp_yaml) -> None:
    """Bidirectional equality of ErrorCode enum hard-fails on drift."""
    spec = _minimal_valid_spec()
    spec["components"]["schemas"]["ErrorCode"]["enum"] = sorted(
        v.EXPECTED_ERROR_CODES - {"job_not_found"}
    )
    p = tmp_yaml(spec)
    report = v.validate(p)
    assert any(
        "ErrorCode" in f and ("drift" in f or "missing_in_spec" in f)
        for f in report.failures
    ), f"expected enum-drift failure; got {report.failures}"


def test_non_bearer_security_fails(tmp_yaml) -> None:
    spec = _minimal_valid_spec()
    spec["components"]["securitySchemes"]["bearerAdminToken"] = {
        "type": "apiKey",
        "in": "header",
        "name": "X-API-Key",
    }
    p = tmp_yaml(spec)
    report = v.validate(p)
    assert any(
        "bearerAdminToken" in f and ("http" in f or "bearer" in f)
        for f in report.failures
    ), f"expected non-bearer security failure; got {report.failures}"


def test_bad_openapi_version_fails(tmp_yaml) -> None:
    spec = _minimal_valid_spec()
    spec["openapi"] = "3.0.0"
    p = tmp_yaml(spec)
    report = v.validate(p)
    assert any(
        "3.1.0" in f for f in report.failures
    ), f"expected version failure; got {report.failures}"


def test_paths_must_be_mapping(tmp_yaml) -> None:
    spec = _minimal_valid_spec()
    spec["paths"] = ["not", "a", "mapping"]
    p = tmp_yaml(spec)
    report = v.validate(p)
    assert any(
        "mapping" in f.lower() for f in report.failures
    ), f"expected paths-mapping failure; got {report.failures}"


# ── 5. Manifest cross-check is warn-only ──────────────────────────────


def test_manifest_warn_only(tmp_yaml) -> None:
    spec = _minimal_valid_spec()
    # Manifest claims a route that the spec does NOT publish.
    manifest = {
        "version": 1,
        "routes": [
            {
                "method": "post",
                "path": "/api/v1/jobs",
                "operationId": "submitJob",
                "tag": "jobs",
                "parameters": [],
                "requestBody": None,
                "responses": {"202": {"$ref": "#/components/schemas/ErrorEnvelope"}},
            },
            {
                "method": "post",
                "path": "/api/v1/route-not-in-spec",
                "operationId": "phantom",
                "tag": "phantom",
                "parameters": [],
                "requestBody": None,
                "responses": {"200": None},
            },
        ],
    }
    p_spec = tmp_yaml(spec, "spec.yaml")
    p_man = tmp_yaml(manifest, "manifest.yaml")
    # refresh: validate expects manifest as dict
    raw_man, _ = v.load_yaml(p_man)
    report = v.validate(p_spec, manifest=raw_man)
    assert (
        report.failures == []
    ), f"manifest drift must be warn-level; got failures: {report.failures}"
    assert any(
        "/api/v1/route-not-in-spec" in w for w in report.warnings
    ), f"expected drift warning for phantom route; got {report.warnings}"
