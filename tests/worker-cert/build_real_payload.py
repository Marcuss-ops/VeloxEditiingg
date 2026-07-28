#!/usr/bin/env python3
# =============================================================================
# tests/worker-cert/build_real_payload.py — Real-asset SubmitJobRequest builder.
# =============================================================================
# Builds the JSON payload accepted by Velox Master's POST /api/v1/jobs
# (operationId `submitJob`, schema `SubmitJobRequest` in
# `DataServer/api/openapi.yaml`) using ONLY canonical `velox-asset://<asset_id>`
# references — never the path-like `velox-asset://voiceovers/<file>.mp3` shape
# that has caused application-level failures in past smokes
# (see docs/operations/04-veloxediting-final-smoke-checklist.md §1).
#
# Asset IDs come from `tests/worker-cert/fixtures/assets.json` (canonical,
# hand-curated from job_submit_e2e_happy_path_test.go, enqueue_normalization_
# test.go, creator_push_e2e_test.go, render_manifest_resolver_test.go).
#
# Outputs the JSON to stdout (default) or to a file via --output. Stdlib
# only (argparse + json + pathlib + re + sys) so it can run in any minimal
# CI / e2e / local shell without pip dependencies.
#
# Exit codes:
#   0  success — payload written/printed.
#   2  usage / env error (missing arg, invalid flag, fixtures unreadable).
#   3  fixtures file present but malformed (jq-parseable JSON expected).
#   4  forbidden pattern detected by --strict self-validation
#      (defensive; this script never emits those by construction).
# =============================================================================

import argparse
import json
import re
import sys
import time
from pathlib import Path

# --- Forbidden patterns (intentional anti-pattern from earlier failures) ----
# Matches any velox-asset://<scheme>/<file>.<ext> shape — i.e. references that
# try to back into a server-local file path. The canonical codec only accepts
# `velox-asset://<asset_id>` regardless of kind (voiceover/clip/subtitle/
# image) — see DataServer/internal/handlers/server/pipeline/intake_validation.go
# §manifestRefURLRegexp. These patterns are EXCLUDED from any payload this
# script produces; --strict runs the same regex against the OUTPUT to assert
# safety on every invocation.
FORBIDDEN_PATTERNS = [
    re.compile(r"velox-asset://voiceovers/[^/]+\.[a-zA-Z0-9]+"),
    re.compile(r"velox-asset://clips/[^/]+\.[a-zA-Z0-9]+"),
    re.compile(r"velox-asset://subtitles/[^/]+\.[a-zA-Z0-9]+"),
    re.compile(r"velox-asset://images/[^/]+\.[a-zA-Z0-9]+"),
    re.compile(r"file://"),
    re.compile(r"\bC:\\\\|\b/home/|\b/Users/|\b/var/|\b/tmp/"),
]

# Canonical kind taxonomy (kind → assets[].asset_id selector), matches
# fixtures/assets.json schema documented inline.
KINDS = ("voiceover", "clips", "subtitles", "images")


def load_fixtures(path: Path) -> dict:
    """Load fixtures JSON; abort with code 3 on parse error, code 2 on missing."""
    if not path.exists():
        print(f"fixtures not found: {path}", file=sys.stderr)
        sys.exit(2)
    try:
        with path.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
    except json.JSONDecodeError as exc:
        print(f"fixtures malformed JSON: {path}: {exc}", file=sys.stderr)
        sys.exit(3)
    for kind in KINDS:
        if not isinstance(data.get(kind), list) or not data[kind]:
            print(
                f"fixtures missing/empty required kind: {kind!r} (need asset_id list)",
                file=sys.stderr,
            )
            sys.exit(3)
        for entry in data[kind]:
            if not isinstance(entry, dict) or "asset_id" not in entry:
                print(
                    f"fixtures malformed entry under {kind!r}: {entry!r} (need dict with asset_id)",
                    file=sys.stderr,
                )
                sys.exit(3)
    return data


def pick_asset_id(fixtures: dict, kind: str, index: int = 0) -> str:
    """Pick the asset_id at `index` from the `kind` list; fails on out-of-range."""
    entries = fixtures[kind]
    if index < 0 or index >= len(entries):
        print(f"asset index {index} out of range for kind {kind!r}", file=sys.stderr)
        sys.exit(2)
    return entries[index]["asset_id"]


def build_payload(
    worker_id: str,
    destination_id: str,
    target_executor_id: str,
    fixtures: dict,
    idempotency_key_suffix: int,
    now_epoch: int | None = None,
) -> dict:
    """Compose a SubmitJobRequest-shaped payload from canonical real assets.

    The idempotency_key is `smoke-one-<worker_id>-<epoch>` to mirror the
    canonical jobs_smoke.sh pattern; second-resolution precision is enough
    for operator pre-flight smokes because the master refuses duplicates only
    inside the typical idempotency_window (here we deliberately avoid
    second-resolution reuse by including worker_id).
    """
    if now_epoch is None:
        now_epoch = int(time.time())
    vo_id = pick_asset_id(fixtures, "voiceover", 0)
    clip_a = pick_asset_id(fixtures, "clips", 0)
    clip_b = pick_asset_id(fixtures, "clips", 1)
    return {
        "idempotency_key": f"smoke-one-{worker_id}-{now_epoch}",
        "video_name": f"Real-asset smoke for {worker_id}@{now_epoch}",
        "project_id": "worker-cert-smoke",
        # Executor is pinned explicitly (does NOT rely on the placement
        # matcher's default derivation; canonical chain referenced in
        # DataServer/internal/jobs/enqueue/normalize.go + the corresponding
        # e2e tests). On master deployments WITH env var
        # VELOX_PLACEMENT_PIN_WORKER_ID=<worker_id>, this is doubly-pinned:
        # same executor AND same worker.
        "target_executor_id": target_executor_id,
        "voiceover_paths": [f"velox-asset://{vo_id}"],
        "scenes": [
            {
                "text": f"Smoke scene 1 — per-worker cert {worker_id}",
                "clip_link": f"velox-asset://{clip_a}",
                "duration_seconds": 3,
            },
            {
                "text": f"Smoke scene 2 — per-worker cert {worker_id}",
                "clip_link": f"velox-asset://{clip_b}",
                "duration_seconds": 3,
            },
        ],
        "delivery_plan": [
            {
                "destination_id": destination_id,
                "priority": 100,
                "retry_budget": 1,
            }
        ],
        # Audit-only diagnostic; the smoke_one.sh harness will read this off
        # the JSON when it parses --payload-file or stdin.
        "_audit": {
            "generator": "tests/worker-cert/build_real_payload.py",
            "generator_rev": 1,
            "generation_time_epoch": now_epoch,
            "idempotency_key_suffix": idempotency_key_suffix,
            "fixture_path": None,  # populated by main() at write time
        },
    }


def assert_no_forbidden(payload: dict, *, path: str = "$") -> list[str]:
    """Walk payload looking for any FORBIDDEN_PATTERNS hit; return hit paths."""
    hits: list[str] = []
    if isinstance(payload, dict):
        for k, v in payload.items():
            hits.extend(assert_no_forbidden(v, path=f"{path}.{k}"))
    elif isinstance(payload, list):
        for i, v in enumerate(payload):
            hits.extend(assert_no_forbidden(v, path=f"{path}[{i}]"))
    elif isinstance(payload, str):
        for pat in FORBIDDEN_PATTERNS:
            if pat.search(payload):
                hits.append(f"{path}={payload!r} (matched {pat.pattern})")
    return hits


def cmd_build(args: argparse.Namespace) -> int:
    fixtures_path = Path(args.fixtures)
    fixtures = load_fixtures(fixtures_path)
    payload = build_payload(
        worker_id=args.worker_id,
        destination_id=args.destination,
        target_executor_id=args.target_executor_id,
        fixtures=fixtures,
        idempotency_key_suffix=args.idempotency_suffix,
    )
    payload["_audit"]["fixture_path"] = str(fixtures_path)

    if args.strict:
        hits = assert_no_forbidden(payload)
        if hits:
            print(
                f"ERROR: {len(hits)} forbidden pattern(s) detected (script regressed?):",
                file=sys.stderr,
            )
            for h in hits:
                print(f"  - {h}", file=sys.stderr)
            return 4

    text = json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=False)
    if args.output:
        out = Path(args.output)
        out.parent.mkdir(parents=True, exist_ok=True)
        # Atomic write: tmp + mv.
        tmp = out.with_suffix(out.suffix + ".tmp")
        tmp.write_text(text, encoding="utf-8")
        tmp.replace(out)
        print(f"wrote {out}", file=sys.stderr)
    else:
        sys.stdout.write(text + "\n")
    return 0


def cmd_selftest(args: argparse.Namespace) -> int:
    """Dry-run without I/O: build a payload, run strict validation, exit."""
    fixtures_path = Path(args.fixtures)
    fixtures = load_fixtures(fixtures_path)
    payload = build_payload(
        worker_id=args.worker_id or "selftest-worker",
        destination_id=args.destination,
        target_executor_id=args.target_executor_id,
        fixtures=fixtures,
        idempotency_key_suffix=0,
    )
    hits = assert_no_forbidden(payload)
    print(f"selftest: built payload with {len(payload.get('scenes'))} scene(s)", file=sys.stderr)
    print(f"selftest: voiceover_paths[0]={payload['voiceover_paths'][0]}", file=sys.stderr)
    print(f"selftest: scenes[*].clip_link={[s['clip_link'] for s in payload['scenes']]}", file=sys.stderr)
    if hits:
        print(f"selftest: FAIL ({len(hits)} forbidden hit(s))", file=sys.stderr)
        return 4
    print("selftest: OK", file=sys.stderr)
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="build_real_payload.py",
        description=(
            "Build a Velox /api/v1/jobs SubmitJobRequest payload from canonical "
            "velox-asset://<asset_id> references. Forbids the path-like "
            "velox-asset://voiceovers/<file>.mp3 shape that has caused application "
            "failures; relies on tests/worker-cert/fixtures/assets.json for "
            "canonical asset IDs (hand-curated from e2e tests)."
        ),
        epilog=(
            "Examples:\n"
            "  # Print payload for host_57_131_20_173 (default destination\n"
            "  # comedy_test, executor scene.composite.v1@1) to stdout:\n"
            "  python3 tests/worker-cert/build_real_payload.py \\\n"
            "    --worker-id host_57_131_20_173\n\n"
            "  # Write to a file for shell piping via --payload-file:\n"
            "  python3 tests/worker-cert/build_real_payload.py \\\n"
            "    --worker-id host_57_131_20_173 \\\n"
            "    --output /tmp/payload.json \\\n"
            "    --strict\n"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--fixtures",
        default="tests/worker-cert/fixtures/assets.json",
        help="path to assets.json with the canonical asset_id set "
        "(default: tests/worker-cert/fixtures/assets.json)",
    )
    parser.add_argument(
        "--worker-id",
        help="worker_id label stamped into the payload's idempotency_key + "
        "scene text (required for build; ignored by selftest unless provided)",
    )
    parser.add_argument(
        "--destination",
        default="comedy_test",
        help="delivery destination_id (default: comedy_test, the canonical "
        "smoke destination per the runbook)",
    )
    parser.add_argument(
        "--target-executor-id",
        default="scene.composite.v1@1",
        help="explicit target_executor_id (default: scene.composite.v1@1, "
        "the canonical executor; avoids relying on the placement matcher "
        "default derivation)",
    )
    parser.add_argument(
        "--idempotency-suffix",
        type=int,
        default=0,
        help="optional integer appended to idempotency_key to disambiguate "
        "back-to-back smokes (default: 0; epoch seconds in the body already "
        "distinguish by the second)",
    )
    parser.add_argument(
        "--output", help="write payload to this file path instead of stdout"
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="run output against the forbidden-pattern validator (defensive — "
        "this script never emits those by construction, but a regression in "
        "the scene/voiceover composers would surface here)",
    )

    sub = parser.add_subparsers(dest="cmd")
    sub.required = False  # default = build
    sub.add_parser("selftest", help="dry-run without I/O; asserts no forbidden "
                    "pattern on a built payload")

    args = parser.parse_args(argv)

    if args.cmd == "selftest":
        return cmd_selftest(args)
    if not args.worker_id:
        print("error: --worker-id is required for build", file=sys.stderr)
        return 2
    return cmd_build(args)


if __name__ == "__main__":
    sys.exit(main())
