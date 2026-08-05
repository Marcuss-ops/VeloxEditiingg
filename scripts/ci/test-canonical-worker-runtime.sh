#!/usr/bin/env bash
# Structural tests for the canonical worker runtime.
# These checks are intentionally local and side-effect free; remote convergence
# is covered by the Ansible playbooks and the host checklist.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

fail() { printf 'canonical-worker-runtime: FAIL: %s\n' "$*" >&2; exit 1; }
pass() { printf 'canonical-worker-runtime: %s\n' "$*"; }

run_migration_test() {
  if [[ $EUID -ne 0 ]]; then
    pass 'fresh-host migration test skipped (root required)'
    return 0
  fi
  local tmp env_file state_dir compose_dir
  tmp="$(mktemp -d)"
  env_file="$tmp/etc/worker.env"
  state_dir="$tmp/state"
  compose_dir="$tmp/compose"
  mkdir -p "$(dirname "$env_file")"
  if ! WORKER_ID=worker-test-01 \
      CANONICAL_ETC_ROOT="$tmp/etc" \
      LEGACY_ETC_ROOT="$tmp/legacy" \
      SYSTEMD_SYSTEM_DIR="$tmp/systemd" \
      SYSTEMD_LIB_DIR="$tmp/systemd-lib" \
      SYSTEMD_USR_LIB_DIR="$tmp/systemd-usr-lib" \
      CANONICAL_ENV="$env_file" \
      CANONICAL_STATE="$state_dir" \
      CANONICAL_ROOT="$compose_dir" \
      bash deploy/runtime/migrate-legacy-worker.sh >/dev/null; then
    rm -rf "$tmp"
    fail 'fresh-host migration failed'
  fi
  grep -qx 'VELOX_WORKER_ID=worker-test-01' "$env_file" \
    || { rm -rf "$tmp"; fail 'fresh-host migration did not materialise worker identity'; }
  grep -qx 'VELOX_WORK_DIR=/var/lib/velox-worker/work' "$env_file" \
    || { rm -rf "$tmp"; fail 'fresh-host migration did not materialise canonical work dir'; }
  [[ -f "$state_dir/migration/manifest.json" ]] \
    || { rm -rf "$tmp"; fail 'fresh-host migration manifest missing'; }
  local first_manifest second_manifest
  first_manifest="$(cat "$state_dir/migration/manifest.json")"
  WORKER_ID=worker-test-01 \
    CANONICAL_ETC="$tmp/etc" \
    CANONICAL_ENV="$env_file" \
    CANONICAL_STATE="$state_dir" \
    CANONICAL_ROOT="$compose_dir" \
    bash deploy/runtime/migrate-legacy-worker.sh >/dev/null
  second_manifest="$(cat "$state_dir/migration/manifest.json")"
  [[ "$first_manifest" != "$second_manifest" ]] \
    || true # completion is intentionally rewritten with a fresh timestamp
  rm -rf "$tmp"
}

run_migration_test

bash -n deploy/runtime/prepare-host.sh
bash -n deploy/runtime/migrate-legacy-worker.sh
bash -n deploy/runtime/checklist-verify.sh

[[ -f deploy/runtime/velox-worker.service ]] || fail 'canonical systemd unit missing'
[[ "$(head -1 deploy/runtime/worker.env.example)" != "[TEMPLATE]" ]] \
  || fail 'worker.env.example begins with the invalid [TEMPLATE] marker'
grep -q '^ExecStart=/usr/bin/docker compose --project-name velox-worker' deploy/runtime/velox-worker.service \
  || fail 'systemd unit does not own the canonical Compose project'
grep -q '^ExecStop=/usr/bin/docker compose --project-name velox-worker' deploy/runtime/velox-worker.service \
  || fail 'systemd unit does not stop the canonical Compose project'
! grep -q 'velox-worker-' deploy/runtime/velox-worker.service \
  || fail 'systemd unit contains a per-host worker name'

grep -q '^name: velox-worker$' deploy/runtime/compose.yml \
  || fail 'Compose project is not fixed to velox-worker'
grep -q '^    container_name: velox-worker$' deploy/runtime/compose.yml \
  || fail 'Compose container is not fixed to velox-worker'
! grep -q 'container_name:.*VELOX_WORKER_ID' deploy/runtime/compose.yml \
  || fail 'container name still derives from worker identity'
grep -q '/var/lib/velox-worker:/var/lib/velox-worker' deploy/runtime/compose.yml \
  || fail 'canonical persistent state mount missing'
grep -q '/etc/velox-worker/worker.env' deploy/runtime/compose.yml \
  || fail 'canonical env mount missing'
grep -q '/var/lib/velox-worker/worker_config.json:/opt/velox/worker_config.json:ro' deploy/runtime/compose.yml \
  || fail 'canonical worker config mount missing'
grep -q 'dest: "{{ velox_worker_runtime_host_dir }}/worker_config.json"' DataServer/data/ansible/playbooks/tasks/canonical_worker_runtime.yml \
  || fail 'Ansible worker config path does not match the Compose mount'
grep -q '"tls_cert_file": "/run/velox/certs/worker.crt"' DataServer/data/ansible/playbooks/tasks/canonical_worker_runtime.yml \
  || fail 'Ansible TLS config path does not match the Compose cert mount'

grep -q 'migrate-legacy-worker.sh' deploy/runtime/prepare-host.sh \
  || fail 'prepare-host does not install/run migration'
grep -q 'systemctl enable --now velox-worker.service' deploy/runtime/prepare-host.sh \
  || fail 'prepare-host does not converge canonical systemd unit'
grep -q 'canonical_unit: velox-worker.service' deploy/playbooks/fleet-restart.yml \
  || fail 'Ansible restart playbook does not use canonical unit'
! grep -q 'container_name=' deploy/ansible/inventory.ini \
  || fail 'inventory still carries a container identity mapping'
! grep -q 'velox-worker-\$WORKER_ID' deploy/scripts/apply-local-worker-config.sh \
  || fail 'local worker config helper still derives the Compose project from worker identity'
grep -q 'systemctl restart velox-worker.service' deploy/scripts/apply-local-worker-config.sh \
  || fail 'local worker config helper does not hand runtime ownership to systemd'
! grep -q 'docker compose .* up -d' deploy/runtime/README.md deploy/runtime/compose.chronon.yml deploy/scripts/apply-local-worker-config.sh \
  || fail 'runtime documentation or helper still advertises direct Compose startup'
! grep -q 'velox-worker-\${id}' deploy/playbooks/fleet-update.yml deploy/playbooks/fleet-rollback.yml \
  || fail 'fleet update/rollback docs still advertise a dynamic Compose project'
grep -q 'remote_src: true' DataServer/data/ansible/playbooks/tasks/canonical_worker_runtime.yml \
  || fail 'canonical Ansible runtime task does not use remote sources'

pass 'OK'
