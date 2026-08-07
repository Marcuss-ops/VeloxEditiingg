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
  mkdir -p "$(dirname "$env_file")" "$tmp/legacy" "$tmp/legacy-runtime/nested"
  mkdir -p "$state_dir/work"
  chmod 0711 "$state_dir"
  chmod 0705 "$state_dir/work"
  printf 'VELOX_WORKER_SECRET=legacy-secret\n' > "$tmp/legacy/velox-worker-test.env"
  printf 'legacy payload\n' > "$tmp/legacy-runtime/nested/payload.txt"
  printf 'legacy existing payload\n' > "$tmp/legacy-runtime/nested/existing.txt"
  chmod 0600 "$tmp/legacy-runtime/nested/payload.txt" "$tmp/legacy-runtime/nested/existing.txt"
  chown 0:0 "$tmp/legacy-runtime/nested/payload.txt" "$tmp/legacy-runtime/nested/existing.txt"
  printf 'VELOX_WORKER_ID=worker-test-01\nVELOX_STATE_DIR=%s\n' "$state_dir" > "$env_file"
  if ! WORKER_ID=worker-test-01 \
      CANONICAL_ETC_ROOT="$tmp/etc" \
      LEGACY_ETC_ROOT="$tmp/legacy" \
      LEGACY_RUNTIME_DIR="$tmp/legacy-runtime" \
      SYSTEMD_SYSTEM_DIR="$tmp/systemd" \
      SYSTEMD_LIB_DIR="$tmp/systemd-lib" \
      SYSTEMD_USR_LIB_DIR="$tmp/systemd-usr-lib" \
      CANONICAL_ENV="$env_file" \
      VELOX_STATE_DIR="$state_dir" \
      CANONICAL_ROOT="$compose_dir" \
      bash deploy/runtime/migrate-legacy-worker.sh >/dev/null; then
    rm -rf "$tmp"
    fail 'fresh-host migration failed'
  fi
  grep -qx 'VELOX_WORKER_ID=worker-test-01' "$env_file" \
    || { rm -rf "$tmp"; fail 'fresh-host migration did not materialise worker identity'; }
  grep -qx 'VELOX_WORKER_SECRET=legacy-secret' "$env_file" \
    || { rm -rf "$tmp"; fail 'fresh-host migration did not preserve legacy credential'; }
  grep -qx "VELOX_STATE_DIR=$state_dir" "$env_file" \
    || { rm -rf "$tmp"; fail 'fresh-host migration did not preserve canonical state dir'; }
  grep -qx "VELOX_WORK_DIR=$state_dir/work" "$env_file" \
    || { rm -rf "$tmp"; fail 'fresh-host migration did not materialise canonical work dir'; }
  [[ -f "$state_dir/migration/manifest.json" ]] \
    || { rm -rf "$tmp"; fail 'fresh-host migration manifest missing'; }
  [[ "$(stat -c '%a' "$state_dir")" == 711 ]] \
    || { rm -rf "$tmp"; fail 'migration changed existing state directory mode'; }
  [[ "$(stat -c '%a' "$state_dir/work")" == 705 ]] \
    || { rm -rf "$tmp"; fail 'migration changed existing work directory mode'; }
  [[ -f "$state_dir/nested/payload.txt" ]] \
    || { rm -rf "$tmp"; fail 'migration did not copy a missing legacy file'; }
  [[ "$(stat -c '%u:%g:%a' "$state_dir/nested/payload.txt")" == '10001:10001:640' ]] \
    || { rm -rf "$tmp"; fail 'migration imported legacy file metadata instead of canonical defaults'; }
  local first_manifest second_manifest
  first_manifest="$(cat "$state_dir/migration/manifest.json")"
  WORKER_ID=worker-test-01 \
    CANONICAL_ETC_ROOT="$tmp/etc" \
    LEGACY_ETC_ROOT="$tmp/legacy" \
    CANONICAL_ENV="$env_file" \
    VELOX_STATE_DIR="$state_dir" \
    CANONICAL_ROOT="$compose_dir" \
    bash deploy/runtime/migrate-legacy-worker.sh >/dev/null
  second_manifest="$(cat "$state_dir/migration/manifest.json")"
  [[ "$first_manifest" == "$second_manifest" ]] \
    || { rm -rf "$tmp"; fail 'completed migration was not idempotent'; }
  [[ -f "$state_dir/migration/completed" ]] \
    || { rm -rf "$tmp"; fail 'completed migration marker missing'; }

  # Existing destination files are operator-owned and must remain untouched.
  printf 'keep me\n' > "$state_dir/nested/existing.txt"
  chown 0:0 "$state_dir/nested/existing.txt"
  chmod 0600 "$state_dir/nested/existing.txt"
  # A completed marker deliberately prevents any second migration, so this
  # assertion is covered by the destination-skip branch in a fresh fixture.
  rm -f "$state_dir/migration/completed" "$state_dir/migration/manifest.json"
  WORKER_ID=worker-test-01 \
    CANONICAL_ETC_ROOT="$tmp/etc" \
    LEGACY_ETC_ROOT="$tmp/legacy" \
    LEGACY_RUNTIME_DIR="$tmp/legacy-runtime" \
    CANONICAL_ENV="$env_file" \
    VELOX_STATE_DIR="$state_dir" \
    CANONICAL_ROOT="$compose_dir" \
    bash deploy/runtime/migrate-legacy-worker.sh >/dev/null
  [[ "$(stat -c '%u:%g:%a' "$state_dir/nested/existing.txt")" == '0:0:600' ]] \
    || { rm -rf "$tmp"; fail 'migration changed existing destination file metadata'; }

  rm -rf "$tmp"
}

run_migration_test

run_migration_symlink_rejection_test() {
  if [[ $EUID -ne 0 ]]; then
    return 0
  fi
  local tmp env_file state_dir compose_dir
  tmp="$(mktemp -d)"
  env_file="$tmp/etc/worker.env"
  state_dir="$tmp/state"
  compose_dir="$tmp/compose"
  mkdir -p "$(dirname "$env_file")" "$tmp/legacy" "$tmp/legacy-runtime"
  printf 'VELOX_WORKER_ID=worker-symlink-test\nVELOX_STATE_DIR=%s\n' "$state_dir" > "$env_file"
  ln -s /etc/passwd "$tmp/legacy-runtime/escape"
  if WORKER_ID=worker-symlink-test \
      CANONICAL_ETC_ROOT="$tmp/etc" \
      LEGACY_ETC_ROOT="$tmp/legacy" \
      LEGACY_RUNTIME_DIR="$tmp/legacy-runtime" \
      SYSTEMD_SYSTEM_DIR="$tmp/systemd" \
      SYSTEMD_LIB_DIR="$tmp/systemd-lib" \
      SYSTEMD_USR_LIB_DIR="$tmp/systemd-usr-lib" \
      CANONICAL_ENV="$env_file" \
      VELOX_STATE_DIR="$state_dir" \
      CANONICAL_ROOT="$compose_dir" \
      bash deploy/runtime/migrate-legacy-worker.sh >/dev/null 2>&1; then
    rm -rf "$tmp"
    fail 'migration accepted a symlinked legacy state entry'
  fi
  rm -rf "$tmp"
}

run_migration_symlink_rejection_test

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
grep -q "\${VELOX_STATE_DIR:?VELOX_STATE_DIR must be set" deploy/runtime/compose.yml \
  || fail 'Compose state mount does not require VELOX_STATE_DIR'
grep -q '^VELOX_STATE_DIR=' deploy/runtime/worker.env.example \
  || fail 'worker env template does not require VELOX_STATE_DIR'
grep -q '/etc/velox-worker/worker.env' deploy/runtime/compose.yml \
  || fail 'canonical env mount missing'
grep -q 'VELOX_STATE_DIR.*worker_config.json:/opt/velox/worker_config.json:ro' deploy/runtime/compose.yml \
  || fail 'canonical worker config mount missing'
grep -q 'dest: "{{ velox_worker_runtime_host_dir }}/worker_config.json"' DataServer/data/ansible/playbooks/tasks/canonical_worker_runtime.yml \
  || fail 'Ansible worker config path does not match the Compose mount'
grep -q '"tls_cert_file": "/run/velox/certs/worker.crt"' DataServer/data/ansible/playbooks/tasks/canonical_worker_runtime.yml \
  || fail 'Ansible TLS config path does not match the Compose cert mount'

grep -q 'migrate-legacy-worker.sh' deploy/runtime/prepare-host.sh \
  || fail 'prepare-host does not install/run migration'
grep -q 'assert_no_legacy_dropins' deploy/runtime/prepare-host.sh \
  || fail 'prepare-host does not reject legacy systemd drop-ins'
grep -q '/lib/systemd/system/velox-worker-' deploy/runtime/prepare-host.sh \
  || fail 'prepare-host does not inspect legacy drop-ins in /lib'
grep -q '/usr/lib/systemd/system/velox-worker-' deploy/runtime/prepare-host.sh \
  || fail 'prepare-host does not inspect legacy drop-ins in /usr/lib'
grep -q 'VELOX_STATE_DIR is missing' deploy/runtime/prepare-host.sh \
  || fail 'prepare-host does not require VELOX_STATE_DIR'
! grep -q 'chown -R.*VELOX_STATE_DIR\|chown -R.*var/lib/velox-worker' deploy/runtime/prepare-host.sh deploy/scripts/apply-local-worker-config.sh \
  || fail 'runtime helpers recursively change state ownership'
grep -q 'systemctl enable --now velox-worker.service' deploy/runtime/prepare-host.sh \
  || fail 'prepare-host does not converge canonical systemd unit'
grep -q 'canonical_unit: velox-worker.service' deploy/playbooks/fleet-restart.yml \
  || fail 'Ansible restart playbook does not use canonical unit'
! grep -q 'container_name=' deploy/ansible/inventory.ini \
  || fail 'inventory still carries a container identity mapping'
! grep -q "velox-worker-\$WORKER_ID" deploy/scripts/apply-local-worker-config.sh \
  || fail 'local worker config helper still derives the Compose project from worker identity'
grep -q 'systemctl restart velox-worker.service' deploy/scripts/apply-local-worker-config.sh \
  || fail 'local worker config helper does not hand runtime ownership to systemd'
! grep -q 'docker compose .* up -d' deploy/runtime/README.md deploy/runtime/compose.chronon.yml deploy/scripts/apply-local-worker-config.sh \
  || fail 'runtime documentation or helper still advertises direct Compose startup'
! grep -q "velox-worker-\${id}" deploy/playbooks/fleet-update.yml deploy/playbooks/fleet-rollback.yml \
  || fail 'fleet update/rollback docs still advertise a dynamic Compose project'
grep -q 'velox_state_dir is defined' DataServer/data/ansible/playbooks/tasks/canonical_worker_runtime.yml \
  || fail 'Ansible canonical runtime does not require velox_state_dir'
grep -q 'register: canonical_runtime_dir_result' DataServer/data/ansible/playbooks/tasks/canonical_worker_runtime.yml \
  || fail 'Ansible runtime directory task does not register its result'
grep -q 'Create missing bootstrap fixture directory preserving metadata' DataServer/data/ansible/playbooks/tasks/canonical_worker_runtime.yml \
  || fail 'Ansible fixture directory creation does not preserve metadata'
grep -q 'VELOX_STATE_DIR: "{{ velox_state_dir }}"' DataServer/data/ansible/playbooks/tasks/canonical_worker_runtime.yml \
  || fail 'Ansible migration does not receive VELOX_STATE_DIR'
grep -q 'Reject legacy worker drop-ins before mutation' DataServer/data/ansible/playbooks/tasks/canonical_worker_runtime.yml \
  || fail 'Ansible legacy drop-in gate is not before host mutation'
grep -q 'remote_src: true' DataServer/data/ansible/playbooks/tasks/canonical_worker_runtime.yml \
  || fail 'canonical Ansible runtime task does not use remote sources'

pass 'OK'
