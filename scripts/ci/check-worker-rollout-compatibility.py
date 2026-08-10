#!/usr/bin/env python3
"""Compatibility gate for phased legacy worker rollout removal.

This gate is read-only: it parses files and checks structural contracts. It
never invokes Ansible, Docker, SSH, the Master API, or a worker deployment.
"""
from __future__ import annotations

from pathlib import Path
import os
import subprocess


ROOT = Path(__file__).resolve().parents[2]

# These are the deliberately retained owners of the migration bridge. They may
# continue to mention bundles/local builds until their documented removal
# phase. A new production file may not acquire those entrypoints silently.
LEGACY_BRIDGE_OWNERS = {
    "DataServer/cmd/velox-bundler/main.go",
    "DataServer/data/ansible/playbooks/update_workers.yml",
    "DataServer/data/ansible/playbooks/tasks/canonical_worker_runtime.yml",
    "DataServer/data/ansible/playbooks/tasks/deploy_worker_release.yml",
    "DataServer/data/ansible/playbooks/tasks/prechecks.yml",
    "DataServer/data/ansible/playbooks/tasks/prepare_worker_image.yml",
    "DataServer/internal/handlers/remote/workers/worker_update.go",
    "scripts/bump-version-and-deploy.sh",
}

# Documentation, tests, and this gate describe the migration contract rather
# than execute it. They are not production consumers.
NON_PRODUCTION_PREFIXES = (
    "docs/",
    "tests/",
    "scripts/ci/",
)


def is_legacy_consumer_line(path: str, line: str) -> bool:
    """Recognize deployment consumers without flagging generic asset storage."""
    lowered = line.lower()
    if "api/worker/bundle" in line:
        return True
    if "prepare_worker_image.yml" in line:
        return True
    if "worker_downloads" in line:
        if path.endswith("internal/assets/store.go") or path.endswith("_test.go"):
            return False
        return "worker_code" in lowered or "bundle" in lowered or "api/worker" in lowered
    if "docker build --pull --no-cache" in lowered:
        return "worker" in lowered or "velox" in lowered
    if "docker build --no-cache" in lowered:
        return "worker" in lowered or "velox" in lowered
    return False


def text(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def require(path: str, *snippets: str) -> None:
    value = text(path)
    missing = [snippet for snippet in snippets if snippet not in value]
    assert not missing, f"{path}: missing {missing!r}"


def git(*args: str) -> str:
    probe = subprocess.run(
        ["git", *args],
        cwd=ROOT,
        text=True,
        encoding="utf-8",
        errors="replace",
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if probe.returncode != 0:
        raise AssertionError(f"git {' '.join(args)} failed: {probe.stderr.strip()}")
    return probe.stdout or ""


def compatibility_base() -> str:
    # Require an explicit, already-fetched base. A silent fallback to a stale
    # remote-tracking branch can make a removal check compare the wrong history.
    # CI must pass BASE_REF (for example origin/main after fetch-depth: 0); a
    # local operator must do the same deliberately.
    explicit = os.environ.get("BASE_REF", "")
    candidates = [explicit] if explicit else []
    for candidate in candidates:
        if candidate and subprocess.run(
            ["git", "rev-parse", "--verify", f"{candidate}^{{commit}}"],
            cwd=ROOT,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        ).returncode == 0:
            return candidate
    raise AssertionError(
        "cannot resolve a compatibility base; set BASE_REF to a fetched commit "
        "or branch before running this gate"
    )


def parse_paths(value: str, variable: str) -> set[str]:
    paths = {
        path.strip()
        for path in value.replace(",", "\\n").splitlines()
        if path.strip()
    }
    if not paths:
        raise AssertionError(f"{variable} must contain at least one repository path")
    return paths


def changed_paths(base: str) -> set[str]:
    """Return tracked and untracked paths visible to this compatibility run."""
    dirty = bool(git("status", "--porcelain").strip())
    if dirty:
        tracked = set(
            git(
                "diff",
                "--name-only",
                "--diff-filter=ACMRDT",
                "HEAD",
                "--",
                "DataServer",
                "deploy",
                "RemoteCodex",
                "scripts",
                ".github",
            ).splitlines()
        )
        untracked = set(
            git(
                "ls-files",
                "--others",
                "--exclude-standard",
                "--",
                "DataServer",
                "deploy",
                "RemoteCodex",
                "scripts",
                ".github",
            ).splitlines()
        )
        return tracked | untracked
    return set(
        git(
            "diff",
            "--name-only",
            "--diff-filter=ACMRDT",
            f"{base}...HEAD",
            "--",
            "DataServer",
            "deploy",
            "RemoteCodex",
            "scripts",
            ".github",
        ).splitlines()
    )


def production_paths_changed(base: str) -> list[str]:
    # A dirty checkout can contain unrelated work from another task. Require
    # an explicit scope and matching allowlist for the phase, but still scan
    # every changed production path for legacy consumers outside that scope.
    scope_value = os.environ.get("COMPAT_SCOPE", "")
    dirty = bool(git("status", "--porcelain").strip())
    all_paths = changed_paths(base)
    if dirty:
        if not scope_value:
            raise AssertionError(
                "working tree is dirty; set COMPAT_SCOPE to the paths in the "
                "compatibility change (newline/comma separated)"
            )
        scope = parse_paths(scope_value, "COMPAT_SCOPE")
        allowlist_value = os.environ.get("COMPAT_ALLOWLIST", "")
        if not allowlist_value:
            raise AssertionError(
                "working tree is dirty; set COMPAT_ALLOWLIST to the exact "
                "paths intended for this phase"
            )
        allowlist = parse_paths(allowlist_value, "COMPAT_ALLOWLIST")
        if scope != allowlist:
            raise AssertionError(
                "COMPAT_SCOPE must exactly equal COMPAT_ALLOWLIST; "
                f"scope={sorted(scope)!r} allowlist={sorted(allowlist)!r}"
            )
        allowed_roots = ("DataServer/", "deploy/", "RemoteCodex/", "scripts/", ".github/")
        invalid = sorted(path for path in scope if not path.startswith(allowed_roots))
        if invalid:
            raise AssertionError(f"compatibility scope contains non-production paths: {invalid!r}")
        deleted = set(
            git("diff", "--name-only", "--diff-filter=D", "HEAD", "--").splitlines()
        )
        missing = sorted(
            path for path in scope
            if not (ROOT / path).exists() and path not in deleted
        )
        if missing:
            raise AssertionError(
                "compatibility scope paths are missing without an explicit "
                f"deletion diff: {missing!r}"
            )
        return sorted(all_paths)
    return sorted(all_paths)


def added_lines_for(base: str, path: str) -> list[str]:
    # In a dirty scoped run, inspect only added lines for tracked files. A new
    # untracked file has no diff hunk, so its complete content is the addition.
    if os.environ.get("COMPAT_SCOPE"):
        tracked = subprocess.run(
            ["git", "ls-files", "--error-unmatch", "--", path],
            cwd=ROOT,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        ).returncode == 0
        if not tracked:
            if not (ROOT / path).exists():
                return []
            return (ROOT / path).read_text(encoding="utf-8").splitlines()
        return [
            line[1:]
            for line in git("diff", "--unified=0", "HEAD", "--", path).splitlines()
            if line.startswith("+") and not line.startswith("+++")
        ]
    tracked = subprocess.run(
        ["git", "ls-files", "--error-unmatch", "--", path],
        cwd=ROOT,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    ).returncode == 0
    if not tracked:
        if not (ROOT / path).exists():
            return []
        return (ROOT / path).read_text(encoding="utf-8").splitlines()
    return [
        line[1:]
        for line in git("diff", "--unified=0", f"{base}...HEAD", "--", path).splitlines()
        if line.startswith("+") and not line.startswith("+++")
    ]


def test_legacy_bridge_files_are_not_removed_implicitly() -> None:
    missing = sorted(
        path for path in LEGACY_BRIDGE_OWNERS if not (ROOT / path).is_file()
    )
    assert not missing, (
        "legacy bridge owner files are missing; update the removal phase and "
        f"gate together before deleting them: {missing!r}"
    )


def test_legacy_bridge_remains_explicit_until_migration() -> None:
    require(
        "DataServer/data/ansible/playbooks/update_workers.yml",
        "update_workers.yml is retired for production",
        "fleetctl update",
    )
    require(
        "DataServer/data/ansible/playbooks/tasks/prepare_worker_image.yml",
        "ACTIVATE WORKER IMAGE — NO REMOTE BUILD",
        "worker_image_ref",
        "velox-worker-activate-image",
    )
    require(
        "DataServer/data/ansible/playbooks/tasks/deploy_worker_release.yml",
        "DeployWorker | Activate pinned worker image (no remote build)",
    )
    require(
        "DataServer/data/ansible/playbooks/tasks/canonical_worker_runtime.yml",
        "Remote bundle update disabled; image activation is FleetController-owned",
    )


def test_fleet_update_delegate_is_removed() -> None:
    assert not (ROOT / "deploy/playbooks/fleet-update.yml").exists(), (
        "deploy/playbooks/fleet-update.yml is a retired duplicate operator path"
    )


def test_install_workers_delegate_is_removed() -> None:
    assert not (ROOT / "DataServer/data/ansible/playbooks/install_workers.yml").exists(), (
        "install_workers.yml is a retired duplicate production rollout path"
    )


def test_current_worker_release_provenance_and_operator_bridge() -> None:
    require(
        ".github/workflows/worker-image.yml",
        "Verify checkout is the event commit",
        '"commit":         "${{ github.sha }}"',
    )
    # scripts/fleetctl is only the thin executable launcher now. The canonical
    # Master-API implementation lives in the typed Go fleetctl package:
    # update/rollback POST /api/v1/admin/workers/{worker_id}/update with a
    # pinned target_digest and poll the fleet_operations ledger. The Ansible
    # host-rollout bridge (FLEET_INVENTORY / ansible-playbook /
    # rollout-worker-digest.yml) is gone.
    require(
        "DataServer/cmd/fleetctl/handlers_helpers.go",
        "/api/v1/admin/workers/",
        "/api/v1/admin/operations/",
    )
    require("DataServer/cmd/fleetctl/handlers_mutations.go", "target_digest")
    launcher = text("scripts/fleetctl")
    assert "exec" in launcher
    assert "FLEET_INVENTORY" not in launcher
    assert "ansible-playbook" not in launcher
    assert "ansible-inventory" not in launcher


def test_removal_plan_matches_gate_contract() -> None:
    require(
        "docs/operations/legacy-worker-removal-plan.md",
        "Phase 3 — remove local Docker builds",
        "Phase 4 — retire bundle distribution",
        "Phase 5 — retire duplicate operator paths",
        "check-worker-rollout-compatibility.py",
    )


def test_current_tree_has_no_legacy_removal_marker() -> None:
    # A future removal commit must first change this gate and the plan in an
    # explicit phase. This prevents silently deleting a bridge while the gate
    # still describes it as required.
    plan = text("docs/operations/legacy-worker-removal-plan.md")
    assert "No files are deleted in Phase 0." in plan
    assert "The production rollout path is now the FleetController API." in plan
    normalized_plan = " ".join(plan.split())
    assert "Phase 3 — remove local Docker builds" in normalized_plan
    assert "Status: COMPLETE" in normalized_plan


def test_new_legacy_consumers_are_rejected() -> None:
    """Reject additions in non-owner production files, including untracked files.

    The old implementation compared only ``base...HEAD``. That was false-green
    during local preparation because the new plan/checker and any uncommitted
    production consumer were invisible. This check compares the resolved base
    with the current worktree and separately includes untracked files.
    """
    base = compatibility_base()
    violations: list[str] = []
    for path in production_paths_changed(base):
        if path in LEGACY_BRIDGE_OWNERS or path.startswith(NON_PRODUCTION_PREFIXES):
            continue
        try:
            additions = added_lines_for(base, path)
        except UnicodeDecodeError:
            continue
        for line in additions:
            if is_legacy_consumer_line(path, line):
                violations.append(f"{path}: {line}")
    assert not violations, "new legacy rollout consumer detected:\n" + "\n".join(violations)


def main() -> None:
    test_legacy_bridge_files_are_not_removed_implicitly()
    test_legacy_bridge_remains_explicit_until_migration()
    test_fleet_update_delegate_is_removed()
    test_current_worker_release_provenance_and_operator_bridge()
    test_removal_plan_matches_gate_contract()
    test_current_tree_has_no_legacy_removal_marker()
    test_new_legacy_consumers_are_rejected()
    print("check-worker-rollout-compatibility: PASS")


if __name__ == "__main__":
    main()
