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

Hardened invariants (post v3 review):

  - 401 / 422 / 500 response schemas MUST be `#/components/schemas/ErrorEnvelope`.
    202 is `CreatorPushAcceptedResponse`. Any copy-paste regression that
    drops the wrong schema $ref will be caught.
  - Bidirectional equality on `ErrorCode.enum` vs the canonical
    `EXPECTED_ERROR_CODES` set — silent drift (new code added to the
    spec without updating the validator, or vice versa) is a hard FAIL.
"""
import sys
from pathlib import Path

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
    "invalid_payload",
    "resolver_failure",
}

# Schemas that MUST be the $ref target of every non-202 response on
# POST /api/v1/creator/jobs (the master creator_push endpoint).
# If `ErrorEnvelope` is renamed (e.g. to `ErrorEnvelopeV1`), update BOTH
# this constant AND the `ErrorEnvelope` schema name simultaneously.
# If `CreatorPushAcceptedResponse` is renamed, update both this
# constant AND its schema name simultaneously. See validate() below
# for the assertion site.
ERROR_RESPONSE_SCHEMA_REF = "#/components/schemas/ErrorEnvelope"
ACCEPTED_RESPONSE_SCHEMA_REF = "#/components/schemas/CreatorPushAcceptedResponse"

# Optional: file path to the Go DTO that the spec's
# `x-flat-to-dto` mapping references. Disable by setting to None.
X_FLAT_TO_DTO_GO_FILE = (
    Path(__file__).resolve().parent.parent.parent
    / "DataServer"
    / "internal"
    / "remoteengine"
    / "dto.go"
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

    # OpenAPI version
    version = doc.get("openapi")
    if version != "3.1.0":
        failures.append(f"{path}: openapi version is {version!r}, want '3.1.0'")

    # Single path focused on creator_push (this revision's scope)
    paths = doc.get("paths", {})
    if list(paths.keys()) != ["/api/v1/creator/jobs"]:
        failures.append(
            f"{path}: paths expected ['/api/v1/creator/jobs'], got {list(paths.keys())}"
        )

    # Global security: bearerAdminToken must be the active security
    sec_top = doc.get("security", [])
    if not any(
        isinstance(s, dict) and "bearerAdminToken" in s for s in sec_top
    ):
        failures.append(f"{path}: top-level security must reference bearerAdminToken")

    sec_def = (
        doc.get("components", {})
        .get("securitySchemes", {})
        .get("bearerAdminToken", {})
    )
    if sec_def.get("type") != "http" or sec_def.get("scheme") != "bearer":
        failures.append(f"{path}: bearerAdminToken scheme must be http+bearer, got {sec_def}")

    # operationId + 202 + 401/422/500 response schemas
    op = paths.get("/api/v1/creator/jobs", {}).get("post", {})
    if op.get("operationId") != "pushCreatorJob":
        failures.append(
            f"{path}: operationId must be 'pushCreatorJob', got {op.get('operationId')!r}"
        )

    responses = op.get("responses", {})
    for code, want_ref in (
        ("202", ACCEPTED_RESPONSE_SCHEMA_REF),
        ("401", ERROR_RESPONSE_SCHEMA_REF),
        ("422", ERROR_RESPONSE_SCHEMA_REF),
        ("500", ERROR_RESPONSE_SCHEMA_REF),
    ):
        actual_ref = (
            responses.get(code, {})
            .get("content", {})
            .get("application/json", {})
            .get("schema", {})
            .get("$ref")
        )
        if actual_ref != want_ref:
            failures.append(
                f"{path}: POST /api/v1/creator/jobs.responses.{code}.content."
                f"application/json.schema.$ref must be {want_ref}, got {actual_ref!r}"
            )

    # X-Request-ID parameter is declared + used on the POST
    parameters = doc.get("components", {}).get("parameters", {})
    if "XRequestIDHeader" not in parameters:
        failures.append(f"{path}: components.parameters.XRequestIDHeader is missing")
    op_params = op.get("parameters", [])
    if not any(
        isinstance(p, dict)
        and p.get("$ref") == "#/components/parameters/XRequestIDHeader"
        for p in op_params
    ):
        failures.append(
            f"{path}: POST /api/v1/creator/jobs.parameters must reference XRequestIDHeader"
        )

    # Required schemas
    schemas = doc.get("components", {}).get("schemas", {})
    wanted = [
        "CreatorPushRequest",
        "CreatorPushPayload",
        "CreatorPushAcceptedResponse",
        "RemotePipelineResult",
        "CreatorScene",
        "DeliveryPlanEntry",
        "CreatorMetadata",
        "CreatorAsset",
        "CreatorScript",
        "ErrorEnvelope",
        "ErrorCode",
    ]
    missing = [s for s in wanted if s not in schemas]
    if missing:
        failures.append(f"{path}: missing schemas {missing}")

    # CreatorPushAcceptedResponse: all 8 fields required + enums
    if "CreatorPushAcceptedResponse" in schemas:
        resp_props = schemas["CreatorPushAcceptedResponse"].get("properties", {})
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
                    f"{path}: CreatorPushAcceptedResponse missing property '{f}'"
                )
        if resp_props.get("accepted_from", {}).get("enum") != ["creator_push"]:
            failures.append(
                f"{path}: accepted_from enum must be ['creator_push']"
            )
        if resp_props.get("dispatch_status", {}).get("enum") != [
            "queued_for_workers"
        ]:
            failures.append(
                f"{path}: dispatch_status enum must be ['queued_for_workers']"
            )

    # CreatorPushRequest: source_provider required + payload $refs CreatorPushPayload
    if "CreatorPushRequest" in schemas:
        required = schemas["CreatorPushRequest"].get("required", [])
        if "source_provider" not in required:
            failures.append(
                f"{path}: CreatorPushRequest.required missing source_provider"
            )
        if "payload" not in required:
            failures.append(
                f"{path}: CreatorPushRequest.required missing payload"
            )
        payload_ref = (
            schemas["CreatorPushRequest"]
            .get("properties", {})
            .get("payload", {})
            .get("$ref")
        )
        if payload_ref != "#/components/schemas/CreatorPushPayload":
            failures.append(
                f"{path}: CreatorPushRequest.payload $ref must be CreatorPushPayload (flat wire), got {payload_ref!r}"
            )

    # ErrorCode schema: bidirectional equality with EXPECTED_ERROR_CODES
    if "ErrorCode" in schemas:
        ec_enum = set(schemas["ErrorCode"].get("enum", []))
        if ec_enum != EXPECTED_ERROR_CODES:
            missing = EXPECTED_ERROR_CODES - ec_enum
            extra = ec_enum - EXPECTED_ERROR_CODES
            if missing or extra:
                failures.append(
                    f"{path}: ErrorCode enum drift. "
                    f"missing_in_spec={sorted(missing)}, "
                    f"extra_in_spec={sorted(extra)}, "
                    f"want={sorted(EXPECTED_ERROR_CODES)}, got={sorted(ec_enum)}"
                )
    else:
        failures.append(f"{path}: ErrorCode schema missing (cannot validate enum set)")

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
