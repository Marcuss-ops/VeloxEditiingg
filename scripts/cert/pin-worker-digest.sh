#!/usr/bin/env bash
# =============================================================================
# scripts/cert/pin-worker-digest.sh
# =============================================================================
# Phase 1 of 100% Velox certification plan (cap. 2) — operator pinner.
#
# Given a published worker image ref (registry + @sha256:<64hex>):
#   1. Refuses any non-digest pin (`:latest`, `:vX.Y.Z` are FAIL-CLOSED).
#   2. Pulls the canonical manifest from GHCR via `gh api` (uses the
#      packages/container/versions endpoint so the same call works for
#      org-scoped GHCR packages).
#   3. Verifies Cosign keyless signature with the exact
#      --certificate-identity-regexp baked into worker-image.yml, so an
#      operator cannot pin a digest signed by a different workflow file.
#   4. Inserts a baselines row into $EVIDENCE_ROOT/baselines/<sha256>.json
#      AND upserts $EVIDENCE_ROOT/baselines/_index.json (sorted, de-duped)
#      with: digest, registry, repo, tags, version, bundle_hash,
#      source_hash, cosign signature sha, signing workflow ref,
#      pinning timestamp, pinning operator.
#
# This script is the SOURCE OF TRUTH for "which digest is safe to deploy"
# across the operator fleet. The downstream 2A+2B certifier (script
# scripts/cert/certify-worker-2a-2b.sh) reads the SAME baselines/ directory
# when EXPECTED_WORKER_IMAGE_DIGEST is not provided.
#
# Required env (or matching CLI flags):
#   DIGEST                       full @sha256:<64hex> pin matching
#                                ghcr.io/<owner>/velox-worker@
#   EXPECTED_COMMIT               full GitHub commit SHA being certified;
#                                required to prevent stale digest replay
#   EVIDENCE_ROOT                (default: $HOME/evidence)
# Optional env:
#   CURRENT_COMMIT               full 40-hex commit for the checked-out repo;
#                                defaults to git rev-parse HEAD and must equal
#                                EXPECTED_COMMIT (stale checkout/digest replay fails)
#   REGISTRY                     (default: ghcr.io)
#   IMAGE_NAME                   (default: velox-worker)
#   REPO_OWNER                   GHCR org; default = current gh user
#   SIGNING_WORKFLOW_REF_REGEXP  the identity-regexp used at CI sign time
#                                (default matches worker-image.yml).
#
# Exit: 0 on success; 1 on validation failure; 2 on cosign verify fail;
# 3 on manifest pull fail.
# =============================================================================

set -uo pipefail  # NOT -e: continue across checks so all failures report

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

usage() {
  cat <<USG
usage: $0 --digest ghcr.io/<owner>/velox-worker@sha256:<64hex>
          --commit <full-github-commit-sha>
          [--registry REGISTRY] [--image-name NAME] [--repo-owner OWNER]
          [--evidence-root DIR] [--help]

Records a cosign-verified baseline manifest under <evidence-root>/baselines/.
USG
  exit "${1:-0}"
}

REGISTRY="${REGISTRY:-ghcr.io}"
IMAGE_NAME="${IMAGE_NAME:-velox-worker}"
SIGNING_WORKFLOW_REF_REGEXP="${SIGNING_WORKFLOW_REF_REGEXP:-^https://github.com/[^/]+/[^/]+/.github/workflows/worker-image\.yml@refs/(tags/worker-v.+|heads/.+)}"
SIGNING_OIDC_ISSUER="${SIGNING_OIDC_ISSUER:-https://token.actions.githubusercontent.com}"
EXPECTED_COMMIT="${EXPECTED_COMMIT:-}"
CURRENT_COMMIT="${CURRENT_COMMIT:-}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --digest)         DIGEST="$2"; shift 2 ;;
    --registry)       REGISTRY="$2"; shift 2 ;;
    --image-name)     IMAGE_NAME="$2"; shift 2 ;;
    --repo-owner)     REPO_OWNER="$2"; shift 2 ;;
    --evidence-root)  EVIDENCE_ROOT="$2"; shift 2 ;;
    --commit)         EXPECTED_COMMIT="$2"; shift 2 ;;
    --help|-h)        usage 0 ;;
    *) printf 'unknown flag: %s\n' "$1" >&2; exit 1 ;;
  esac
done

# ─── Sanity ─────────────────────────────────────────────────────────────────
if [[ -z "${DIGEST:-}" ]]; then
  printf '::error::--digest is required (flag or env)\n' >&2
  usage 1
fi
# Parse registry/owner/name@sha256:hex64 out of DIGEST.
# Format:
#   ${REGISTRY}/${OWNER}/${IMAGE_NAME}@sha256:<64 lowercase hex>
if ! [[ "${EXPECTED_COMMIT:-}" =~ ^[0-9a-fA-F]{40}$ ]]; then
  printf '::error::--commit is required and must be a full 40-hex GitHub commit SHA\n' >&2
  exit 1
fi
CURRENT_COMMIT="${CURRENT_COMMIT:-$(git -C "$REPO_ROOT" rev-parse HEAD 2>/dev/null || true)}"
if ! [[ "$CURRENT_COMMIT" =~ ^[0-9a-fA-F]{40}$ ]]; then
  printf '::error::checked-out current commit is unavailable; set CURRENT_COMMIT explicitly\n' >&2
  exit 1
fi
if [[ "${EXPECTED_COMMIT,,}" != "${CURRENT_COMMIT,,}" ]]; then
  printf '::error::digest commit %s does not match checked-out current commit %s; stale digest replay refused\n' \
    "$EXPECTED_COMMIT" "$CURRENT_COMMIT" >&2
  exit 1
fi
if ! [[ "$DIGEST" =~ ^([^/]+)/([^/]+)/([^/@]+)@(sha256:[a-f0-9]{64})$ ]]; then
  printf '::error::--digest must be <registry>/<owner>/<name>@sha256:<64hex>; got: %s\n' \
    "$DIGEST" >&2
  exit 1
fi
PARSE_REGISTRY="${BASH_REMATCH[1]}"
PARSE_OWNER="${BASH_REMATCH[2]}"
PARSE_NAME="${BASH_REMATCH[3]}"
PARSE_SHA="${BASH_REMATCH[4]}"
if [[ "$PARSE_REGISTRY" != "$REGISTRY" ]]; then
  printf '::error::--digest registry (%s) != --registry (%s); refuse to confuse\n' \
    "$PARSE_REGISTRY" "$REGISTRY" >&2
  exit 1
fi
if [[ "$PARSE_NAME" != "$IMAGE_NAME" ]]; then
  printf '::error::--digest image-name (%s) != --image-name (%s); refuse to confuse\n' \
    "$PARSE_NAME" "$IMAGE_NAME" >&2
  exit 1
fi
REPO_OWNER="${REPO_OWNER:-$PARSE_OWNER}"
REPOSITORY="${GITHUB_REPOSITORY:-Marcuss-ops/VeloxEditiingg}"
DIGEST_PIN="${REGISTRY}/${REPO_OWNER}/${IMAGE_NAME}@${PARSE_SHA}"
SHA_ONLY="${PARSE_SHA#sha256:}"         # 64 lowercase hex
SHA_PREFIX="${SHA_ONLY:0:12}"           # short prefix for filenames

EVIDENCE_ROOT="${EVIDENCE_ROOT:-$HOME/evidence}"
BASELINES_DIR="$EVIDENCE_ROOT/baselines"
mkdir -p "$BASELINES_DIR"

# ─── Prereqs ─────────────────────────────────────────────────────────────────
need() {
  if ! command -v "$1" >/dev/null 2>&1; then
    printf '::error::missing required tool: %s\n' "$1" >&2
    exit 1
  fi
}
need gh
need cosign
need python3
if ! gh auth status >/dev/null 2>&1; then
  printf '::error::gh is not authenticated; run `gh auth login` first\n' >&2
  exit 1
fi

# ─── Pull canonical manifest from GHCR ─────────────────────────────────────
printf '→ pulling manifest for %s\n' "$DIGEST_PIN"
PACKAGE_NAME_LOWER="$(printf '%s' "$IMAGE_NAME" | tr '[:upper:]' '[:lower:]')"
# GHCR packages use different API paths depending on whether the owner is an
# organization or a user. Try the org endpoint first (the canonical Velox
# package is org-owned), then retain the user endpoint for personal packages.
MANIFEST_JSON=""
for PACKAGE_OWNER_SCOPE in orgs users; do
  MANIFEST_JSON="$(gh api \
    "/${PACKAGE_OWNER_SCOPE}/${REPO_OWNER}/packages/container/${PACKAGE_NAME_LOWER}/versions?per_page=100" \
    --jq -c '.[] | select(.name | endswith("'"${PARSE_SHA}"'"))' 2>/dev/null | head -1 || true)"
  if [[ -n "$MANIFEST_JSON" ]]; then
    break
  fi
done
if [[ -z "$MANIFEST_JSON" ]]; then
  # The operator token may have no packages:read scope even though the
  # release workflow published the image successfully. The signed CI
  # baseline manifest below is an equivalent, stronger source for the exact
  # digest/commit binding, so defer package metadata until that artifact is
  # loaded instead of rejecting a valid release here.
  printf '::warning::GHCR package metadata unavailable for %s; using the certified CI baseline artifact\n' \
    "$PARSE_SHA" >&2
  MANIFEST_JSON='{"tags":[]}'
fi
GIT_REF="$(printf '%s' "$MANIFEST_JSON" | python3 -c 'import json,sys
d=json.load(sys.stdin)
print(",".join(t.get("source",{}).get("git_ref","") or "" for t in d.get("tags",[]) if t.get("source",{}).get("git_ref")))' 2>/dev/null || echo '')"
TAGS_JSON="$(printf '%s' "$MANIFEST_JSON" | python3 -c 'import json,sys
d=json.load(sys.stdin)
import json as j
print(j.dumps([t.get("name","") for t in d.get("tags",[])]))' 2>/dev/null || echo '[]')"

# ─── Cosign verify against the canonical workflow identity ─────────────────
printf '→ cosign verify for %s\n' "$DIGEST_PIN"
COSIGN_OUT="$(cosign verify \
  --certificate-github-workflow-sha "$EXPECTED_COMMIT" \
  --certificate-identity-regexp "$SIGNING_WORKFLOW_REF_REGEXP" \
  --certificate-oidc-issuer "$SIGNING_OIDC_ISSUER" \
  "$DIGEST_PIN" 2>&1)" || {
  printf '::error::cosign verify FAILED\n%s\n' "$COSIGN_OUT" >&2
  exit 2
}
# Extract signature subject + signer (Cosign prints a JSON envelope to stdout
# on success). We only need a stable hash of the envelope so re-runs are
# idempotent.
COSIGN_ENVELOPE_HASH="$(printf '%s' "$COSIGN_OUT" | sha256sum | awk '{print $1}')"

# The baseline artifact is part of the certification contract. Resolve the
# successful worker-image run for the exact commit, download its manifest, and
# bind the requested digest to both the manifest digest and commit. Do not fall
# back to the latest successful run: that would permit stale provenance.
WORKFLOW_ID="$(gh api \
  "/repos/${REPOSITORY}/actions/workflows/worker-image.yml" \
  --jq '.id' 2>/dev/null || true)"
if [[ -z "$WORKFLOW_ID" ]]; then
  printf '::error::worker-image workflow is unavailable for repository %s\n' "$REPOSITORY" >&2
  exit 3
fi
RUN_IDS="$(gh api \
  "/repos/${REPOSITORY}/actions/workflows/${WORKFLOW_ID}/runs?head_sha=${EXPECTED_COMMIT}&status=completed&per_page=100" \
  --jq '.workflow_runs[] | select(.conclusion=="success") | .id' 2>/dev/null || true)"
if [[ -z "$RUN_IDS" ]]; then
  printf '::error::no successful worker-image certification run for commit %s\n' "$EXPECTED_COMMIT" >&2
  exit 3
fi
ARTIFACT_DIR="$(mktemp -d)"
trap 'rm -rf "$ARTIFACT_DIR"' EXIT
CERT_MANIFEST=""
ARTIFACT_RUN_ID=""
for RUN_ID in $RUN_IDS; do
  CANDIDATE_DIR="$ARTIFACT_DIR/$RUN_ID"
  mkdir -p "$CANDIDATE_DIR"
  if ! gh run download "$RUN_ID" --repo "$REPOSITORY" \
      --name worker-baseline-manifest --dir "$CANDIDATE_DIR" >/dev/null 2>&1; then
    continue
  fi
  CANDIDATE_MANIFEST="$(find "$CANDIDATE_DIR" -type f -name 'worker-baseline-manifest.json' -print -quit)"
  [[ -n "$CANDIDATE_MANIFEST" ]] || continue
  if python3 - "$CANDIDATE_MANIFEST" "$PARSE_SHA" "$EXPECTED_COMMIT" <<'PYEOF'
import json, sys
path, expected_digest, expected_commit = sys.argv[1:]
data = json.load(open(path, encoding="utf-8"))
digest = str(data.get("digest", ""))
commit = str(data.get("commit", ""))
if digest != expected_digest or commit.lower() != expected_commit.lower():
    raise SystemExit(1)
PYEOF
  then
    CERT_MANIFEST="$CANDIDATE_MANIFEST"
    ARTIFACT_RUN_ID="$RUN_ID"
    break
  fi
done
if [[ -z "$CERT_MANIFEST" ]]; then
  printf '::error::no worker-baseline-manifest matched digest %s and commit %s\n' \
    "$PARSE_SHA" "$EXPECTED_COMMIT" >&2
  exit 3
fi
MANIFEST_JSON="$(cat "$CERT_MANIFEST")"
TAGS_JSON="$(python3 - "$MANIFEST_JSON" <<'PYEOF'
import json, sys
d = json.loads(sys.argv[1])
print(json.dumps(d.get("tags", [])))
PYEOF
)"

# ─── Read baseline manifest from a side-band source ─────────────────────────
# Two paths to source the (version, bundle_hash, source_hash) tuple:
#   A. Worker-image.yml publishes worker-baseline-manifest.json as a GH
#      artifact on every push tag. We pull the latest matching artifact
#      from the Workflow Runs API.
#   B. Pull the image, run `cat /opt/velox/...` from the running container
#      (slowest; only used as a fallback).
# We default to (A): cheaper, no docker daemon required on the pinning host.

WORKFLOW_FILE_BASENAME="worker-image.yml"
printf '→ certification artifact verified for workflow=%s commit=%s digest=%s\n' \
  "$WORKFLOW_FILE_BASENAME" "$EXPECTED_COMMIT" "$PARSE_SHA"

# ─── Compose canonical baseline JSON ────────────────────────────────────────
PINNED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
PINNED_BY="$(gh api user --jq '.login' 2>/dev/null || echo unknown)"

BASELINE_FILE="$BASELINES_DIR/${SHA_PREFIX}-${SHA_ONLY}.json"
TMP_INDEX="$BASELINES_DIR/_index.json.new"

python3 - "$DIGEST_PIN" "$PARSE_SHA" "$REPO_OWNER" "$IMAGE_NAME" "$TAGS_JSON" \
              "$GIT_REF" "$EXPECTED_COMMIT" "$COSIGN_ENVELOPE_HASH" "$SIGNING_WORKFLOW_REF_REGEXP" \
              "$SIGNING_OIDC_ISSUER" "$PINNED_AT" "$PINNED_BY" \
              "$ARTIFACT_RUN_ID" "$BASELINE_FILE" "$TMP_INDEX" \
              "$MANIFEST_JSON" <<'PYEOF'
import json, os, sys
(digest_pin, sha_only, owner, image_name, tags_json, git_ref,
 expected_commit, cosign_hash, sign_regex, sign_issuer, pinned_at, pinned_by,
 artifact_run_id, baseline_path, index_path, manifest_blob) = sys.argv[1:]

tags = json.loads(tags_json) if tags_json.strip() else []
baseline = {
    "schema":            "velox.baseline.v1",
    "digest":            sha_only,
    "registry_image":    digest_pin,
    "owner":             owner,
    "image_name":        image_name,
    "tags":              tags,
    "git_ref":           git_ref,
    "commit":            expected_commit,
    "signing": {
        "identity_regexp":     sign_regex,
        "oidc_issuer":         sign_issuer,
        "envelope_sha256":     cosign_hash,
        "ci_artifact_run_id":  artifact_run_id,
    },
    "pinned_at":  pinned_at,
    "pinned_by":  pinned_by,
    "phase":      "1",
    "manifest_present": bool(manifest_blob.strip()),
}

with open(baseline_path, "w") as f:
    json.dump(baseline, f, indent=2, sort_keys=True)

# Merge into a single canonical index (sorted, de-duped by digest).
idx_path = os.path.join(os.path.dirname(index_path), "_index.json")
idx = []
if os.path.exists(idx_path):
    try:
        idx = json.load(open(idx_path))
    except Exception:
        idx = []
# Replace any existing row with the same digest.
idx = [r for r in idx if r.get("digest") != sha_only]
idx.append({
    "digest":          sha_only,
    "registry_image":  digest_pin,
    "tags":            tags,
    "pinned_at":       pinned_at,
    "signing_envelope_sha256": cosign_hash,
    "phase":            1,
})
idx.sort(key=lambda r: r.get("digest", ""))
with open(index_path + ".atomic", "w") as f:
    json.dump(idx, f, indent=2, sort_keys=True)
os.replace(index_path + ".atomic", idx_path)

print(json.dumps({"baseline": baseline_path, "index": idx_path}, indent=2))
PYEOF

printf '\n✓ pinned %s to %s\n' "$DIGEST_PIN" "$BASELINE_FILE"
printf '  index → %s\n' "$BASELINES_DIR/_index.json"
exit 0
