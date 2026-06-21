#!/usr/bin/env bash
# scripts/ci/check-single-writer.sh
#
# Enforce the single-writer principle (see docs/architecture/OWNERSHIP.md).
# For each critical mutation symbol, grep the PR diff and assert the
# symbol occurs ONLY inside its canonical owner.
#
# Two layers (defence in depth):
#   1. SQL layer: catches raw SQL like `UPDATE jobs SET status = 'SUCCEEDED'`.
#      Most production code goes through the repository layer, so this
#      is mostly a backstop.
#   2. Go method layer: catches `repo.MarkSucceeded(...)`,
#      `outboxStore.MarkProcessed(...)`, etc. -- the path that real
#      contributors use. This is the layer that actually fires when
#      a handler decides to flip status itself.
#
# ALL rules are scoped to the current branch's diff via scoped_grep --
# HEAD main is trivially green; PRs surface new regressions only.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"
source "$(dirname "${BASH_SOURCE[0]}")/lib/diff-scope.sh"

fail() { printf 'SINGLE-WRITER ERROR: %s\n' "$*" >&2; exit 1; }
violations=0

# scan uses awk so that quoted-owner paths (e.g. with parentheses)
# still anchor correctly. We compare on the file:line:match tuple's
# first column (the file path) against the owner prefix.
scan() {
  local pattern="$1"
  local owner="$2"
  local hits
  hits="$(scoped_grep "$pattern" -- \
            ':!*_test.go' \
            ':!*.sql' \
            ':!docs/**' \
            ':!frontend_standalone/**' \
            ':!scripts/ci/check-single-writer.sh' \
            ':!scripts/ci/lib/diff-scope.sh')"
  [[ -z "$hits" ]] && return 0

  local disallowed
  disallowed="$(printf '%s\n' "$hits" \
                  | awk -F: -v owner="$owner" \
                    '$1 !~ "^"owner { print }' || true)"
  if [[ -n "$disallowed" ]]; then
    printf 'NEW pattern "%s" found OUTSIDE canonical owner %s:\n%s\n\n' \
      "$pattern" "$owner" "$disallowed" >&2
    violations=$((violations + 1))
  fi
}

scan "UPDATE jobs SET status = 'SUCCEEDED'"  DataServer/internal/artifacts/
scan "UPDATE jobs SET status = 'FAILED'"     DataServer/internal/artifacts/
scan "UPDATE outbox_events SET status"       DataServer/internal/outbox/
scan "INSERT INTO outbox_events"             DataServer/internal/outbox/
scan "UPDATE deliveries SET status"          DataServer/internal/deliveries/
scan "UPDATE asset_blobs"                    DataServer/internal/assets/
scan "UPDATE workers SET last_heartbeat"     DataServer/internal/workers/

scan "\.MarkSucceeded\("                     DataServer/internal/artifacts/
scan "\.MarkFinalized\("                     DataServer/internal/artifacts/
scan "\.Finalize\("                          DataServer/internal/artifacts/
scan "\.MarkFailed\("                        DataServer/internal/outbox/
scan "\.MarkProcessed\("                     DataServer/internal/outbox/

# Queue facade (removed in PR "refactor(jobs): remove queue compatibility facade")
# Reintroducing any queue alias or the internal/queue package is forbidden.
scan '"velox-server/internal/queue"'            DataServer/NO_SUCH_DIR_DO_NOT_IMPORT_QUEUE/
scan '\*queue\.FileQueue'                      DataServer/NO_SUCH_DIR_DO_NOT_USE_QUEUE/
scan 'queue\.JobStatus'                        DataServer/NO_SUCH_DIR_DO_NOT_USE_QUEUE/
scan 'queue\.QueueItem'                        DataServer/NO_SUCH_DIR_DO_NOT_USE_QUEUE/
scan 'queue\.Job[^a-zA-Z]'                     DataServer/NO_SUCH_DIR_DO_NOT_USE_QUEUE/

# Compatibility shims: any NEW file in the PR that introduces a
# COMPATIBILITY marker MUST carry `Remove after:`. Existing shims are
# grandfathered.
shim_files="$(scoped_grep 'COMPATIBILITY:' -- '*.go' \
              | cut -d: -f1 | sort -u || true)"
for f in $shim_files; do
  if ! grep -q 'Remove after:' "$f"; then
    printf 'NEW COMPATIBILITY shim without removal deadline: %s\n' \
      "$f" >&2
    violations=$((violations + 1))
  fi
done

if [[ "$violations" -gt 0 ]]; then
  printf '%d single-writer violation(s) -- see above\n' \
    "$violations" >&2
  exit 1
fi

echo "check-single-writer: OK"
