#!/usr/bin/env bash
# Structural checks for the canonical worker runtime.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

fail() { printf 'canonical-worker-runtime: FAIL: %s\n' "$*" >&2; exit 1; }
pass() { printf 'canonical-worker-runtime: %s\n' "$*"; }

bash -n deploy/runtime/prepare-host.sh
bash -n deploy/runtime/checklist-verify.sh

[[ -f deploy/runtime/velox-worker.service ]] || fail 'canonical systemd unit missing'
[[ ! -e deploy/runtime/migrate-legacy-worker.sh ]] || fail 'legacy migrator still present'
! grep -Rqs 'migrate-legacy-worker.sh' deploy/runtime DataServer/data/ansible \
  || fail 'legacy migrator still referenced by runtime convergence'

grep -q '^ExecStart=/usr/bin/docker compose --project-name velox-worker' \
  deploy/runtime/velox-worker.service || fail 'systemd unit does not own canonical Compose project'
grep -q '^ExecStop=/usr/bin/docker compose --project-name velox-worker' \
  deploy/runtime/velox-worker.service || fail 'systemd unit does not stop canonical Compose project'
! grep -q 'velox-worker-' deploy/runtime/velox-worker.service \
  || fail 'systemd unit contains a per-host worker name'

grep -q '^name: velox-worker$' deploy/runtime/compose.yml \
  || fail 'Compose project is not fixed to velox-worker'
grep -q '^    container_name: velox-worker$' deploy/runtime/compose.yml \
  || fail 'Compose container is not fixed to velox-worker'
grep -q '\${VELOX_STATE_DIR:?VELOX_STATE_DIR must be set' deploy/runtime/compose.yml \
  || fail 'Compose state mount does not require VELOX_STATE_DIR'
grep -q '^VELOX_STATE_DIR=' deploy/runtime/worker.env.example \
  || fail 'worker env template does not require VELOX_STATE_DIR'
grep -q '/etc/velox-worker/worker.env' deploy/runtime/compose.yml \
  || fail 'canonical env mount missing'

grep -q 'assert_no_legacy_dropins' deploy/runtime/prepare-host.sh \
  || fail 'prepare-host does not reject legacy drop-ins'
grep -q 'VELOX_STATE_DIR is missing' deploy/runtime/prepare-host.sh \
  || fail 'prepare-host does not require VELOX_STATE_DIR'
grep -q 'systemctl enable --now velox-worker.service' deploy/runtime/prepare-host.sh \
  || fail 'prepare-host does not converge canonical systemd unit'
! grep -q 'chown -R.*VELOX_STATE_DIR\|chown -R.*var/lib/velox-worker' \
  deploy/runtime/prepare-host.sh deploy/scripts/apply-local-worker-config.sh \
  || fail 'runtime helpers recursively change state ownership'

grep -q 'velox_state_dir is defined' \
  DataServer/data/ansible/playbooks/tasks/canonical_worker_runtime.yml \
  || fail 'Ansible canonical runtime does not require velox_state_dir'
grep -q 'velox_worker_container_name: "velox-worker"' \
  DataServer/data/ansible/playbooks/tasks/canonical_worker_runtime.yml \
  || fail 'Ansible canonical container identity missing'

pass 'canonical runtime checks passed'
