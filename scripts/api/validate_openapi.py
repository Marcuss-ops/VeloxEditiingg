#!/usr/bin/env python3
"""Standalone validator for DataServer/api/openapi.yaml.

Takes file paths from argv (robust against shell-quoting issues) and
prints a structured PASS/FAIL report. Exits 0 only if every assertion
holds. Used as the canonical companion to the spec for both local
review and CI gating.

Usage:
    python3 scripts/api/validate_openapi.py DataServer/api/openapi.yaml
    # or, from the repo root:
    python3 -c "import runpy; runpy.run_path('scripts/api/validate_openapi.py', run_name='__main__')"

Hardened invariants (post v4 review):

  - Per-route invariants are data-driven via ``ROUTE_INVARIANTS``.
    Adding a new endpoint means adding one entry there, NEVER editing
    compare logic. Required routes MUST be present; additional routes
    are silently allowed (we explicitly do NOT enforce exclusivity —
    that was the v3 fragility that broke every time a new endpoint
    landed).
  - Per-route response schemas: 4xx (and 5xx where declared) MUST be
    ``#/components/schemas/ErrorEnvelope``; 202 MUST be the operation's
    declared accepted-response schema. Top-level security must
    reference ``bearerAdminToken`` of type http+bearer.
  - Bidirectional equality on ``ErrorCode.enum`` vs the canonical
    ``EXPECTED_ERROR_CODES`` set — silent drift (new code added to
    the spec without updating the validator, or vice versa) is a
    hard FAIL.
"""
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


# Canonical error-code list. MUST match `ErrorCode.enum` in the spec.
# Update both sides in lockstep, or the validator will FAIL loudly.
# If `ErrorCode` is renamed in the spec (e.g. to `ErrorCodeV1`), update
# both `EXPECTED_ERROR_CODES` (this list) AND the line below
# referencing `ErrorCode` schema in lockstep — the validator's FAIL
# messages will otherwise be misleading.
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

# Schema $ref that MUST be the target of every error-style response on
# authenticated POST endpoints (4xx/5xx).
ERROR_RESPONSE_SCHEMA_REF = "#/components/schemas/ErrorEnvelope"


# ── Schemas that MUST exist in components.schemas ───────────────────────────
# Adding a new schema anywhere in the spec that the rest of the contract
# depends on = add the name here. The validator does NOT recurse into
# $ref resolution by design (it is a lightweight invariant checker,
# not a full OpenAPI linter).
REQUIRED_SCHEMAS = {
    # Universal error envelope — used by every authenticated endpoint.
    "ErrorEnvelope",
    "ErrorCode",
    # Creator intake (POST /api/v1/creator/jobs).
    "CreatorPushRequest",
    "CreatorPushPayload",
    "CreatorPushAcceptedResponse",
    "RemotePipelineResult",
    "CreatorScene",
    "DeliveryPlanEntry",
    "CreatorMetadata",
    "CreatorAsset",
    "CreatorScript",
    # Simplified job submission (POST /api/v1/jobs).
    "SubmitJobRequest",
    "SubmitScene",
    "SubmitDeliveryPlanEntry",
    "SubmitJobAcceptedResponse",
    # Polling endpoint (GET /api/v1/jobs/{job_id}) — 4-field envelope.
    "SubmitJobStatusResponse",
    "SubmitLayer",
    "SubmitSubtitleTrack",
}


# ── Per-route invariants (data-driven; an endpoint = one entry) ─────────────
# Each entry declares required structure on a single path+method:
#
#   path:        exact path string (must exist under paths:<path>)
#   method:      HTTP verb (must exist under paths:<path>:<method>)
#   operationId: must equal this string
#   parameters:  list of parameter names that MUST be $ref'd from
#                components/parameters/ (e.g. ``XRequestIDHeader``)
#   responses:   {status_code: required $ref OR None to skip}
#                Missing routes FAIL loudly. Extra routes are allowed.
ROUTE_INVARIANTS = [
    {
        "path": "/api/v1/creator/assets",
        "method": "post",
        "operationId": "uploadCreatorAsset",
        # 201 has an inline content-addressed schema; 400/413/422 carry no
        # JSON body. Nothing to enforce beyond presence + operationId.
        "responses": {},
    },
    {
        "path": "/api/v1/creator/jobs",
        "method": "post",
        "operationId": "pushCreatorJob",
        "parameters": ["AuthorizationHeader", "XRequestIDHeader"],
        "requestBody": "#/components/schemas/CreatorPushRequest",
        "responses": {
            "202": "#/components/schemas/CreatorPushAcceptedResponse",
            "401": ERROR_RESPONSE_SCHEMA_REF,
            "422": ERROR_RESPONSE_SCHEMA_REF,
            "500": ERROR_RESPONSE_SCHEMA_REF,
        },
    },
    {
        "path": "/api/v1/jobs",
        "method": "post",
        "operationId": "submitJob",
        "parameters": ["AuthorizationHeader", "XRequestIDHeader"],
        "requestBody": "#/components/schemas/SubmitJobRequest",
        "responses": {
            "202": "#/components/schemas/SubmitJobAcceptedResponse",
            "400": ERROR_RESPONSE_SCHEMA_REF,
            "401": ERROR_RESPONSE_SCHEMA_REF,
            "409": ERROR_RESPONSE_SCHEMA_REF,
            "422": ERROR_RESPONSE_SCHEMA_REF,
            "500": ERROR_RESPONSE_SCHEMA_REF,
        },
    },
    {
        # Polling endpoint (P2): GET /api/v1/jobs/{job_id}. Walks the
        # same M2M bearer as POST /api/v1/jobs and emits the canonical
        # 4-field envelope via SubmitJobStatusResponse. The 404 path
        # carries the new `job_not_found` error code in the spec's
        # ErrorCode enum.
        "path": "/api/v1/jobs/{job_id}",
        "method": "get",
        "operationId": "getSubmittedJob",
        "parameters": ["AuthorizationHeader", "XRequestIDHeader"],
        "responses": {
            "200": "#/components/schemas/SubmitJobStatusResponse",
            "400": ERROR_RESPONSE_SCHEMA_REF,
            "401": ERROR_RESPONSE_SCHEMA_REF,
            "404": ERROR_RESPONSE_SCHEMA_REF,
            "500": ERROR_RESPONSE_SCHEMA_REF,
        },
    },
]


def _missing_required(value: Any, expected: Any, label: str, failures: list[str]) -> None:
    """Compare a YAML-parsed value against the expected one.

    ``expected`` may be a string, list, int, float, or None — whatever
    the YAML fragment at the call site is supposed to be.
    """
    if value != expected:
        failures.append(f"{label} must be {expected}, got {value!r}")


def _check_path_op(
    path_obj, path: str, method: str, inv: dict, failures: list[str], source: str
) -> None:
    """Validate the per-route invariants declared in ROUTE_INVARIANTS."""
    op = path_obj.get(method) or {}
    if not op:
        # Surface the structural root cause first, then skip per-field
        # assertions (which would otherwise emit "got None" noise).
        failures.append(f"{source}: {path}.{method} method is missing")
        return

    expected_op_id = inv["operationId"]
    _missing_required(
        op.get("operationId"),
        expected_op_id,
        f"{source}: {path}.{method}.operationId",
        failures,
    )

    for wanted_param in inv.get("parameters", []):
        ref = f"#/components/parameters/{wanted_param}"
        op_params = op.get("parameters", []) or []
        if not any(
            isinstance(p, dict) and p.get("$ref") == ref for p in op_params
        ):
            failures.append(
                f"{source}: {path}.{method}.parameters must reference {wanted_param}"
            )

    # Presence-based gate so an explicit `"requestBody": null` in
    # ROUTE_INVARIANTS still surfaces as a hard FAIL (otherwise dict
    # lookup would coerce None into the silent skip path).
    if "requestBody" in inv:
        actual_body = (
            (op.get("requestBody", {}) or {})
            .get("content", {})
            .get("application/json", {})
            .get("schema", {})
            .get("$ref")
        )
        _missing_required(
            actual_body,
            inv["requestBody"],
            f"{source}: {path}.{method}.requestBody.content."
            f"application/json.schema.$ref",
            failures,
        )

    responses = op.get("responses", {}) or {}
    for code, want_ref in inv.get("responses", {}).items():
        actual_ref = (
            (responses.get(code, {}) or {})
            .get("content", {})
            .get("application/json", {})
            .get("schema", {})
            .get("$ref")
        )
        _missing_required(
            actual_ref,
            want_ref,
            f"{source}: {path}.{method}.responses.{code}.content."
            f"application/json.schema.$ref",
            failures,
        )


def _check_required_schemas(doc, source: str, failures: list[str]) -> None:
    schemas = doc.get("components", {}).get("schemas", {}) or {}
    missing = [s for s in sorted(REQUIRED_SCHEMAS) if s not in schemas]
    if missing:
        failures.append(f"{source}: missing schemas {missing}")


def _check_creator_push_response(doc, source: str, failures: list[str]) -> None:
    """CreatorPushAcceptedResponse: all 8 fields required + enums."""
    schemas = doc.get("components", {}).get("schemas", {}) or {}
    if "CreatorPushAcceptedResponse" not in schemas:
        return
    resp_props = schemas["CreatorPushAcceptedResponse"].get("properties", {}) or {}
    for f in [
        "ok",
        "accepted_from",
        "source_provider",
        "source_job_id",
        "target_executor_id",
        "job_id",
        "status",
        "dispatch_status",
    ]:
        if f not in resp_props:
            failures.append(
                f"{source}: CreatorPushAcceptedResponse missing property '{f}'"
            )
    _missing_required(
        resp_props.get("accepted_from", {}).get("enum"),
        ["creator_push"],
        f"{source}: accepted_from enum",
        failures,
    )
    _missing_required(
        resp_props.get("dispatch_status", {}).get("enum"),
        ["queued_for_workers"],
        f"{source}: dispatch_status enum",
        failures,
    )


def _check_creator_push_request(doc, source: str, failures: list[str]) -> None:
    """CreatorPushRequest: required + payload $ref."""
    schemas = doc.get("components", {}).get("schemas", {}) or {}
    if "CreatorPushRequest" not in schemas:
        return
    required = schemas["CreatorPushRequest"].get("required", []) or []
    if "source_provider" not in required:
        failures.append(
            f"{source}: CreatorPushRequest.required missing source_provider"
        )
    if "payload" not in required:
        failures.append(
            f"{source}: CreatorPushRequest.required missing payload"
        )
    payload_ref = (
        schemas["CreatorPushRequest"]
        .get("properties", {})
        .get("payload", {})
        .get("$ref")
    )
    _missing_required(
        payload_ref,
        "#/components/schemas/CreatorPushPayload",
        f"{source}: CreatorPushRequest.payload $ref (flat wire)",
        failures,
    )


def _check_submit_job_request(doc, source: str, failures: list[str]) -> None:
    """SubmitJobRequest: idempotency_key + scenes required, scenes items $ref."""
    schemas = doc.get("components", {}).get("schemas", {}) or {}
    if "SubmitJobRequest" not in schemas:
        return
    required = schemas["SubmitJobRequest"].get("required", []) or []
    if "idempotency_key" not in required:
        failures.append(
            f"{source}: SubmitJobRequest.required missing idempotency_key"
        )
    if "scenes" not in required:
        failures.append(
            f"{source}: SubmitJobRequest.required missing scenes"
        )
    props = schemas["SubmitJobRequest"].get("properties", {}) or {}
    ikey_minlen = (props.get("idempotency_key", {}) or {}).get("minLength")
    _missing_required(
        ikey_minlen,
        1,
        f"{source}: SubmitJobRequest.idempotency_key.minLength",
        failures,
    )
    scenes_ref = (
        (props.get("scenes", {}) or {}).get("items", {}) or {}
    ).get("$ref")
    _missing_required(
        scenes_ref,
        "#/components/schemas/SubmitScene",
        f"{source}: SubmitJobRequest.scenes.items $ref",
        failures,
    )


def _check_submit_job_response(doc, source: str, failures: list[str]) -> None:
    """SubmitJobAcceptedResponse: required fields + enums."""
    schemas = doc.get("components", {}).get("schemas", {}) or {}
    if "SubmitJobAcceptedResponse" not in schemas:
        return
    required = schemas["SubmitJobAcceptedResponse"].get("required", []) or []
    for f in ["ok", "accepted_from", "idempotency_key", "job_id", "dispatch_status"]:
        if f not in required:
            failures.append(
                f"{source}: SubmitJobAcceptedResponse.required missing {f!r}"
            )
    props = schemas["SubmitJobAcceptedResponse"].get("properties", {}) or {}
    _missing_required(
        props.get("accepted_from", {}).get("enum"),
        ["api_v1_jobs"],
        f"{source}: SubmitJobAcceptedResponse.accepted_from enum",
        failures,
    )
    _missing_required(
        props.get("dispatch_status", {}).get("enum"),
        ["queued_for_workers"],
        f"{source}: SubmitJobAcceptedResponse.dispatch_status enum",
        failures,
    )


def _check_submit_scene(doc, source: str, failures: list[str]) -> None:
    """SubmitScene: text + duration_seconds required, duration.minimum=0.1."""
    schemas = doc.get("components", {}).get("schemas", {}) or {}
    if "SubmitScene" not in schemas:
        return
    required = schemas["SubmitScene"].get("required", []) or []
    for f in ["text", "duration_seconds"]:
        if f not in required:
            failures.append(
                f"{source}: SubmitScene.required missing {f!r}"
            )
    duration = (
        schemas["SubmitScene"].get("properties", {}).get("duration_seconds", {}) or {}
    )
    _missing_required(
        duration.get("minimum"),
        0.1,
        f"{source}: SubmitScene.duration_seconds.minimum",
        failures,
    )


def _check_submit_delivery_plan_entry(doc, source: str, failures: list[str]) -> None:
    """SubmitDeliveryPlanEntry: destination_id required (server rejects empty)."""
    schemas = doc.get("components", {}).get("schemas", {}) or {}
    if "SubmitDeliveryPlanEntry" not in schemas:
        return
    required = schemas["SubmitDeliveryPlanEntry"].get("required", []) or []
    if "destination_id" not in required:
        failures.append(
            f"{source}: SubmitDeliveryPlanEntry.required missing destination_id"
        )


def _check_error_code(doc, source: str, failures: list[str]) -> None:
    """ErrorCode enum MUST match EXPECTED_ERROR_CODES bidirectionally."""
    schemas = doc.get("components", {}).get("schemas", {}) or {}
    if "ErrorCode" not in schemas:
        failures.append(
            f"{source}: ErrorCode schema missing (cannot validate enum set)"
        )
        return
    ec_enum = set(schemas["ErrorCode"].get("enum", []) or [])
    if ec_enum != EXPECTED_ERROR_CODES:
        missing = EXPECTED_ERROR_CODES - ec_enum
        extra = ec_enum - EXPECTED_ERROR_CODES
        failures.append(
            f"{source}: ErrorCode enum drift. "
            f"missing_in_spec={sorted(missing)}, "
            f"extra_in_spec={sorted(extra)}, "
            f"want={sorted(EXPECTED_ERROR_CODES)}, got={sorted(ec_enum)}"
        )


def _check_security(doc, source: str, failures: list[str]) -> None:
    sec_top = doc.get("security", []) or []
    if not any(isinstance(s, dict) and "bearerAdminToken" in s for s in sec_top):
        failures.append(
            f"{source}: top-level security must reference bearerAdminToken"
        )
    sec_def = (
        doc.get("components", {})
        .get("securitySchemes", {})
        .get("bearerAdminToken", {})
    )
    if sec_def.get("type") != "http" or sec_def.get("scheme") != "bearer":
        failures.append(
            f"{source}: bearerAdminToken scheme must be http+bearer, got {sec_def}"
        )


def validate(path: Path) -> list[str]:
    """Return a list of failure strings. Empty list means PASS."""
    failures: list[str] = []
    try:
        with path.open("r", encoding="utf-8") as fh:
            doc = yaml.safe_load(fh)
    except yaml.YAMLError as exc:
        return [f"{path}: YAML parse error: {exc}"]
    except OSError as exc:
        return [f"{path}: open error: {exc}"]

    if not isinstance(doc, dict):
        return [f"{path}: top-level is not a mapping"]

    version = doc.get("openapi")
    if version != "3.1.0":
        failures.append(f"{path}: openapi version is {version!r}, want '3.1.0'")

    source = str(path)

    _check_security(doc, source, failures)

    paths = doc.get("paths", {}) or {}
    for inv in ROUTE_INVARIANTS:
        p = inv["path"]
        m = inv["method"]
        if p not in paths:
            failures.append(f"{source}: required path '{p}' is missing")
            continue
        _check_path_op(paths[p], p, m, inv, failures, source)

    _check_required_schemas(doc, source, failures)
    _check_creator_push_response(doc, source, failures)
    _check_creator_push_request(doc, source, failures)
    _check_submit_job_request(doc, source, failures)
    _check_submit_job_response(doc, source, failures)
    _check_submit_scene(doc, source, failures)
    _check_submit_delivery_plan_entry(doc, source, failures)
    _check_error_code(doc, source, failures)

    return failures


def main() -> int:
    if len(sys.argv) < 2:
        print(
            "usage: validate_openapi.py <path/to/openapi.yaml> [<path>...]",
            file=sys.stderr,
        )
        return 2

    all_failures: list[str] = []
    for arg in sys.argv[1:]:
        path = Path(arg)
        print(f"--- validating {path} ---")
        failures = validate(path)
        if failures:
            print(f"FAIL: {path} ({len(failures)} failures)")
            for failure in failures:
                print(f"  - {failure}")
            all_failures.extend(failures)
        else:
            print("PASS")

    if all_failures:
        print(
            f"--- TOTAL FAIL ({len(all_failures)} failures across {len(sys.argv) - 1} file(s)) ---"
        )
        return 1

    print(
        f"--- TOTAL PASS: {len(sys.argv) - 1} openapi file(s) meet all invariants ---"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
