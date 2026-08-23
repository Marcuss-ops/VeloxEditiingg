#!/usr/bin/env python3
"""Guard worker-image certification against stale source commits.

These checks parse workflow and local composite-action text only. They do not
contact GHCR, invoke Cosign, Docker, Ansible, or deploy anything.
"""
from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / ".github" / "workflows" / "worker-image.yml"
ACTIONS = ROOT / ".github" / "actions"
DEPLOY = ROOT / ".github" / "workflows" / "deploy.yml"
WORKER_BUILD = ACTIONS / "worker-build" / "action.yml"
WORKER_SIGN = ACTIONS / "worker-sign" / "action.yml"
WORKER_CERTIFY = ACTIONS / "worker-certify" / "action.yml"


def read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_worker_workflow_checks_exact_checkout_commit() -> None:
    workflow = read(WORKFLOW)
    assert "Verify checkout is the event commit" in workflow
    assert 'CHECKOUT_SHA="$(git rev-parse HEAD)"' in workflow
    assert 'CHECKOUT_SHA" != "$GITHUB_SHA"' in workflow
    assert "Refusing to certify an ambiguous source tree" in workflow


def test_worker_baseline_records_full_commit() -> None:
    workflow = read(WORKFLOW)
    certify = read(WORKER_CERTIFY)
    assert "./.github/actions/worker-certify" in workflow
    assert '"commit":         "${{ github.sha }}"' in certify
    assert '"source_hash":    "$SOURCE_HASH"' in certify
    assert '"workflow_file":  "worker-image.yml"' in certify


def test_worker_build_preserves_digest_and_no_latest_guards() -> None:
    workflow = read(WORKFLOW)
    build = read(WORKER_BUILD)
    assert "./.github/actions/worker-build" in workflow
    assert "latest=false" in build
    assert "docker buildx imagetools inspect" in build
    assert 'name: worker-image-digest' in build
    assert 'echo "digest=${{ steps.build.outputs.digest }}"' in build
    assert 'value: ${{ steps.digest.outputs.digest }}' in build


def test_worker_signing_is_digest_bound_and_workflow_bound() -> None:
    workflow = read(WORKFLOW)
    signing = read(WORKER_SIGN)
    assert "./.github/actions/worker-sign" in workflow
    assert 'IMAGE="${{ inputs.image-repo }}@${{ inputs.digest }}"' in signing
    assert "cosign sign --yes \"$IMAGE\"" in signing
    assert "--certificate-identity-regexp='^https://github.com/Marcuss-ops/VeloxEditiingg/\\.github/workflows/worker-image\\.yml@refs/(tags/worker-v.+|heads/.+)'" in signing
    assert "--certificate-oidc-issuer='https://token.actions.githubusercontent.com'" in signing
    assert "--certificate-identity-regexp='.*'" not in signing
    assert "--certificate-oidc-issuer-regexp='.*'" not in signing


def test_worker_certification_preserves_immutable_digest_gates() -> None:
    workflow = read(WORKFLOW)
    certify = read(WORKER_CERTIFY)
    assert "./.github/actions/worker-certify" in workflow
    assert "docker pull \"$IMAGE_REF\"" in certify
    assert "worker-baseline-manifest.json" in certify
    assert "--bootstrap-report" in certify
    assert "worker-audio-canary.sh" in certify
    assert "worker-mixed-canary.sh" in certify
    assert 'IMAGE="${{ inputs.image-repo }}@${{ inputs.digest }}"' in certify


def test_deploy_worker_signature_is_commit_bound() -> None:
    deploy = read(DEPLOY)
    worker_marker = 'echo "::group::Verifying cosign signature on ${WORKER_REF}"'
    assert worker_marker in deploy
    worker_section = deploy.split(worker_marker, 1)[1].split(
        'echo "::endgroup::"', 1
    )[0]
    assert '--certificate-github-workflow-sha "${WORKER_RELEASE_COMMIT}"' in worker_section
    assert (
        "--certificate-identity-regexp "
        "'^https://github.com/Marcuss-ops/VeloxEditiingg/"
        ".github/workflows/worker-image\\.yml@refs/"
        "(tags/worker-v.+|heads/.+)'"
    ) in worker_section
    assert "--certificate-identity-regexp '.*'" not in worker_section
    assert "--certificate-oidc-issuer-regexp '.*'" not in worker_section


def test_deploy_requires_explicit_release_commit_for_manual_dispatch() -> None:
    deploy = read(DEPLOY)
    assert "worker_release_commit:" in deploy
    assert 'WORKER_RELEASE_COMMIT="${{ inputs.worker_release_commit }}"' in deploy
    assert "worker_release_commit must be a full 40-hex commit SHA" in deploy
    assert 'CHECKOUT_SHA" != "$WORKER_RELEASE_COMMIT"' in deploy


def test_worker_release_guard_runs_before_build() -> None:
    workflow = read(WORKFLOW)
    guard = workflow.index("Verify release freshness controls")
    build = workflow.index("./.github/actions/worker-build")
    assert guard < build


def test_operator_pinner_requires_commit_bound_cosign_verification() -> None:
    pinner = read(ROOT / "scripts" / "cert" / "pin-worker-digest.sh")
    assert "--commit <full-github-commit-sha>" in pinner
    assert "EXPECTED_COMMIT" in pinner
    assert "must be a full 40-hex GitHub commit SHA" in pinner
    assert 'CURRENT_COMMIT="${CURRENT_COMMIT:-$(git -C "$REPO_ROOT" rev-parse HEAD' in pinner
    assert '"${EXPECTED_COMMIT,,}" != "${CURRENT_COMMIT,,}"' in pinner
    assert '--certificate-github-workflow-sha "$EXPECTED_COMMIT"' in pinner
    assert '"commit":            expected_commit' in pinner
    assert "--jq -c '.[] | select(.name | endswith" in pinner
    assert 'gh run download "$RUN_ID"' in pinner
    assert "no worker-baseline-manifest matched digest" in pinner
    assert 'bash scripts/cert/pin-worker-digest.sh "$$@" --digest "$$DIGEST" --commit "$$EXPECTED_COMMIT"' in read(ROOT / "Makefile")


if __name__ == "__main__":
    test_worker_workflow_checks_exact_checkout_commit()
    test_worker_baseline_records_full_commit()
    test_worker_build_preserves_digest_and_no_latest_guards()
    test_worker_signing_is_digest_bound_and_workflow_bound()
    test_worker_certification_preserves_immutable_digest_gates()
    test_deploy_worker_signature_is_commit_bound()
    test_deploy_requires_explicit_release_commit_for_manual_dispatch()
    test_worker_release_guard_runs_before_build()
    test_operator_pinner_requires_commit_bound_cosign_verification()
    print("test-worker-image-freshness: PASS")
