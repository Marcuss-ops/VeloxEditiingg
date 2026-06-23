#!/usr/bin/env bash
# scripts/ci/operator-history-scrub.sh
#
# PR-5 5e — operator-only git-history rewrites for PR-5 (P0 closure).
#
# ─── WHY THIS SCRIPT IS SAFE-BY-DEFAULT ─────────────────────────────────
# History rewriting via git-filter-repo (or BFG repo-cleaner) is FORCE-PUSH
# territory: every existing clone becomes stale, in-flight feature branches
# need rebase or rebuild, and a botched run can corrupt tags. We refuse
# to perform any destructive action without an explicit confirmation env:
#
#     YES_I_REALLY_WANT_TO_SCRUB=1 bash scripts/ci/operator-history-scrub.sh
#
# Without that env, this script PRINTS the exact command plan and exits 0.
# The operator copies the commands into their own shell session so they
# can stage / spread / communicate before force-pushing to the canonical
# remote.
#
# ─── WHY WE DO NOT AUTO-FORCE-PUSH ──────────────────────────────────────
# Force-push must be coordinated across the deploy channel. Multiple
# branches (dev, staging, feature/*) might need concurrent rewrites.
# The canonical-gate (origin/main) force-push is OUT OF SCOPE for this
# script: it's the LAST step the operator performs, and they verify the
# rewritten tree has zero matches of the regex in
# scripts/ci/check-secrets.sh before announcing completion.
#
# ─── TOOLING ─────────────────────────────────────────────────────────────
# The repo audit confirmed git-filter-repo (Python) is installed. BFG
# (Java) is NOT installed. We use git-filter-repo unless the operator
# explicitly passes `--tool=bfg`. git-filter-repo is the modern, faster,
# and core-git-recommended choice.
#
# ─── EXIT CODES ─────────────────────────────────────────────────────────
#   0   success (or plan printed, in default mode)
#   1   operator refused to set YES_I_REALLY_WANT_TO_SCRUB=1 (NOT an error
#       in default mode; only in --execute mode)
#   2   usage error (unknown flag, missing dep)
#   3   sanity check failed (uncommitted changes, no remote, etc.)
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

# ── Defaults / arg parsing ───────────────────────────────────────────────
TOOL="git-filter-repo"
MODE="plan"
while [[ $# -gt 0 ]]; do
    case "$1" in
        --tool=git-filter-repo|--tool=bfg)
            TOOL="${1#--tool=}"
            ;;
        --execute)
            MODE="execute"
            ;;
        --dry-run)
            MODE="dry-run"
            ;;
        -h|--help)
            sed -n '2,33p' "${BASH_SOURCE[0]}"
            exit 0
            ;;
        *)
            printf 'unknown flag: %s\n' "$1" >&2
            exit 2
            ;;
    esac
    shift
done

# ── Path patterns to invert-remove ───────────────────────────────────────
# These come from the PR-5 audit (`git log --all --diff-filter=AM` paths):
#   * VeloxEditing/refactored/DataServer/client_secret*.json (multi)
#   * VeloxEditing/refactored/DataServer/data/{drive,secrets,youtube}/tokens/*.json (multi)
#   * YoutubePosting/Modules/tokens/*.json (multi)
#   * Any *.ts.net / *.duckdns.org / *.apps.googleusercontent.com literal
# We remove PATH-MATCH first (simpler, lower blast radius). Textual scrub
# is a follow-up if the post-scrub audit fires on surviving literals.
SCRUB_PATHS=(
    "VeloxEditing/refactored/DataServer/client_secret*.json"
    "VeloxEditing/refactored/DataServer/data/drive/tokens/*.json"
    "VeloxEditing/refactored/DataServer/data/secrets/youtube/tokens/*.json"
    "YoutubePosting/Modules/tokens/*.json"
)

# ── Plan emission (always prints first, regardless of mode) ───────────────
# Operators see the exact commands before anything destructive fires.
emit_plan() {
    printf '\n=== PR-5 history-scrub plan (operator execution) ===\n'
    printf 'Tool:    %s\n' "$TOOL"
    printf 'Repo:    %s\n' "$REPO_ROOT"
    printf '\nSTEP 1 — Sanity (must pass):\n'
    printf '  git status --porcelain | wc -l        # MUST be 0\n'
    printf '  git remote -v | grep -c origin        # MUST be ≥ 1\n'
    printf '  Test:  bash scripts/ci/check-secrets.sh | head -10\n'
    printf '\nSTEP 2 — Snapshot for forensics (private branch, off-origin):\n'
    printf '  git checkout -b pre-pr5-history-scrub\n'
    printf '  git push <private-remote> pre-pr5-history-scrub\n'
    printf '\nSTEP 3 — git-filter-repo path-match removal:\n'
    for path in "${SCRUB_PATHS[@]}"; do
        printf '  git-filter-repo --invert-paths --path-glob "%s" --force\n' "$path"
    done
    printf '\nSTEP 4 — Prune reflog so unreachable objects are eligible for GC:\n'
    printf '  git reflog expire --expire=now --all\n'
    printf '  git gc --prune=now --aggressive\n'
    printf '\nSTEP 5 — Re-run the post-scrub audit (MUST exit 0):\n'
    printf '  bash scripts/ci/check-secrets.sh\n'
    printf '  bash scripts/ci/check-secrets.sh --include-untracked\n'
    printf '\nSTEP 6 — Force-push (LAST step; coordinate in deploy channel):\n'
    printf '  git push --force-with-lease origin main\n'
    printf '\nSTEP 7 — Verify remote is rewritten:\n'
    printf '  git fetch origin\n'
    printf '  git log --all --diff-filter=A --name-only | grep -E '\''client_secret|tokens'\''\n'
    printf '  # MUST be empty.\n'
    printf '\n=== END PLAN ===\n'
}

# Always print the plan first; operators should run --dry-run before
# committing to --execute even if they set YES_I_REALLY_WANT_TO_SCRUB=1.
emit_plan

# ── Default mode: plan-only, do not execute ───────────────────────────────
if [[ "$MODE" != "execute" ]]; then
    printf '\nPlan printed. To actually execute, run:\n'
    printf '  YES_I_REALLY_WANT_TO_SCRUB=1 bash %s --execute\n' "$0"
    exit 0
fi

# ── Execute mode: now require explicit YES_I_REALLY_WANT_TO_SCRUB ─────────
if [[ "${YES_I_REALLY_WANT_TO_SCRUB:-}" != "1" ]]; then
    printf '\nrefusing to execute: env YES_I_REALLY_WANT_TO_SCRUB is not 1\n' >&2
    printf 'set YES_I_REALLY_WANT_TO_SCRUB=1 in the SAME invocation as %s --execute\n' "$0" >&2
    exit 1
fi

# ── Sanity ────────────────────────────────────────────────────────────────
if [[ -n "$(git status --porcelain 2>/dev/null)" ]]; then
    printf 'sanity fail: working tree dirty. Commit / stash first.\n' >&2
    exit 3
fi
if ! command -v "$TOOL" >/dev/null 2>&1; then
    if [[ "$TOOL" == "git-filter-repo" ]] && python3 -c 'import git_filter_repo' 2>/dev/null; then
        : # fine, callable via python3 -m git_filter_repo
    else
        printf 'sanity fail: %s not installed. Run `pip install git-filter-repo` (or --tool=bfg with BFG).\n' "$TOOL" >&2
        exit 3
    fi
fi
if ! git remote -v 2>/dev/null | grep -q origin; then
    printf 'sanity fail: no origin remote. Operator must add origin before force-push.\n' >&2
    exit 3
fi

# ── Snapshot (informational; the operator does the actual push to a        ─
# private branch — we do not auto-push because the private remote is        ─
# operator-specific)                                                       ─
printf '\nCreating snapshot branch pre-pr5-history-scrub (NOT pushing):\n'
# PR-5 reviewer F5: fail loud if snapshot branch already exists (don't
# silently resume against a possibly-stale partial-rewrite tree).
# Reviewer J5: use `git branch` (no checkout) so HEAD stays put on the
# operator's current branch.
if git rev-parse --verify refs/heads/pre-pr5-history-scrub >/dev/null 2>&1; then
    printf 'sanity fail: snapshot branch pre-pr5-history-scrub already exists.\n' >&2
    printf 'Inspect (git log pre-pr5-history-scrub) or delete (`git branch -D pre-pr5-history-scrub`) before re-running.\n' >&2
    exit 3
fi
git branch pre-pr5-history-scrub

# ── Run git-filter-repo path-match removal ────────────────────────────────
for path in "${SCRUB_PATHS[@]}"; do
    printf '\n-- removing path-glob: %s\n' "$path"
    if ! git filter-repo --invert-paths --path-glob "$path" --force; then
        printf 'git-filter-repo failed for %s — aborting.\n' "$path" >&2
        printf 'Investigate the error before re-running; the repo is now in a partial-rewrite state.\n' >&2
        exit 3
    fi
done

# ── Prune reflog + gc ─────────────────────────────────────────────────────
git reflog expire --expire=now --all
git gc --prune=now --aggressive

# ── Post-scrub audit ─────────────────────────────────────────────────────
printf '\n=== Post-scrub audit (MUST exit 0 before operator force-pushes) ===\n'
if ! bash scripts/ci/check-secrets.sh; then
    printf '\nPOST-SCRUB AUDIT FAILED. Investigate before force-pushing:\n' >&2
    printf '  bash scripts/ci/check-secrets.sh\n' >&2
    exit 1
fi

# ── Reminder: operator performs the force-push ────────────────────────────
printf '\n=== Path scrub complete. Force-push is YOUR responsibility: ===\n'
printf 'Verification needed before force-push:\n'
printf '  git log --all --diff-filter=A --name-only | grep -E "client_secret|tokens"\n'
printf '  # MUST be empty.\n'
printf '\nThen, in coordination with the deploy channel:\n'
printf '  git push --force-with-lease origin main\n'
printf '\nDONE. The script does NOT force-push.\n'
