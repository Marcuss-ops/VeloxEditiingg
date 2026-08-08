#!/usr/bin/env bash
# scripts/operator/provision-worker-openbao.sh
#
# Canonical operator-side bootstrap for worker OpenBao AppRole material.
#
# With --import-legacy, the missing worker credential KV leaf is imported
# from the matching worker's existing runtime file over strict SSH. mTLS
# certificates and private keys are never imported into KV: the worker's key
# remains local and the certificate is issued through OpenBao PKI. Values never
# appear in stdout/stderr or command-line arguments. The script then installs
# the per-worker role-id/secret-id, OpenBao TLS CA, canonical fetcher, and only
# the four VELOX_OPENBAO_* settings in /etc/velox-worker/worker.env.
#
# The worker talks to https://127.0.0.1:8200 through the canonical reverse
# tunnel. The operator-side OpenBao API defaults to
# https://127.0.0.1:18200, forwarded by remote-velox-tunnel.sh.
#
# Worker SSH connectivity comes from the operator, mirroring the Master
# WorkerNodeRegistry (`ansible_hosts` DB table — the canonical source):
# pass --ssh-host and --ssh-user (or VELOX_WORKER_SSH_HOST/_USER), e.g.
# the values registered for this worker in the registry.
#
# Usage (exactly one worker per invocation):
#   ./scripts/operator/provision-worker-openbao.sh --worker <worker-id> \
#     --ssh-host <host> --ssh-user <user> --import-legacy
#   ./scripts/operator/provision-worker-openbao.sh --worker <worker-id> \
#     --ssh-host <host> --ssh-user <user> --dry-run
#
# AppRole material remains under the gitignored .velox state tree.

set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# Worker SSH connectivity (from the Master WorkerNodeRegistry / ansible_hosts
# table, the canonical source — no static Ansible inventory anymore).
SSH_HOST="${VELOX_WORKER_SSH_HOST:-}"
SSH_USER="${VELOX_WORKER_SSH_USER:-}"
STATE_DIR="${OPENBAO_STATE_DIR:-$ROOT/.velox/openbao}"
ADDR="${OPENBAO_OPERATOR_ADDR:-https://127.0.0.1:18200}"
CA_FILE="${OPENBAO_CA_FILE:-$STATE_DIR/tls/server.crt}"
TOKEN_FILE="$STATE_DIR/root-token"
WORKER_ADDR="${VELOX_WORKER_OPENBAO_ADDR:-https://127.0.0.1:8200}"
WORKER_CA_FILE="/etc/velox-worker/certs/openbao-ca.crt"
ROLE_ID_PATH="/etc/velox-worker/secrets/approle/role-id"
SECRET_ID_PATH="/etc/velox-worker/secrets/approle/secret-id"
FETCH_PATH="/opt/velox-worker/openbao-fetch-worker-secrets.sh"
ENV_PATH="/etc/velox-worker/worker.env"
# The operator's canonical fleet key is the same key used for worker SSH
# access (WorkerNodeRegistry connectivity). Callers may override it
# explicitly for a separate administrative setup.
SSH_KEY="${VELOX_SSH_KEY:-$HOME/.ssh/id_ed25519}"
TUNNEL_SCRIPT="$ROOT/scripts/operator/remote-worker-openbao-tunnel.sh"

IMPORT_LEGACY=0
DRY_RUN=0
CHECK=1
SELECTED=()
CURL_CONFIG=""

log() { printf '[worker-openbao] %s\n' "$*"; }
die() { printf '[worker-openbao] FATAL: %s\n' "$*" >&2; exit 1; }

usage() {
  sed -n '2,30p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
  exit "${1:-0}"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --import-legacy) IMPORT_LEGACY=1; shift ;;
    --dry-run) DRY_RUN=1; CHECK=0; shift ;;
    --no-check) CHECK=0; shift ;;
    --worker)
      [[ $# -ge 2 && -n "${2:-}" && "${2#--}" == "$2" ]] \
        || die "--worker requires a non-empty worker id"
      SELECTED+=("$2")
      shift 2
      ;;
    --ssh-host)
      [[ $# -ge 2 && -n "${2:-}" && "${2#--}" == "$2" ]] \
        || die "--ssh-host requires a non-empty value"
      SSH_HOST="$2"
      shift 2
      ;;
    --ssh-user)
      [[ $# -ge 2 && -n "${2:-}" && "${2#--}" == "$2" ]] \
        || die "--ssh-user requires a non-empty value"
      SSH_USER="$2"
      shift 2
      ;;
    -h|--help) usage 0 ;;
    *) die "unknown option: $1" ;;
  esac
done

[[ ${#SELECTED[@]} -eq 1 ]] \
  || die "exactly one --worker <worker-id> is required per invocation (got ${#SELECTED[@]})"
WORKER_ID="${SELECTED[0]}"
[[ "$WORKER_ID" =~ ^[A-Za-z0-9][A-Za-z0-9._:-]*$ ]] \
  || die "invalid worker id: $WORKER_ID"

command -v curl >/dev/null 2>&1 || die "curl is required"
command -v jq >/dev/null 2>&1 || die "jq is required"
command -v ssh >/dev/null 2>&1 || die "ssh is required"
[[ -n "$SSH_HOST" && -n "$SSH_USER" ]] \
  || die "--ssh-host and --ssh-user are required (worker connectivity from the Master WorkerNodeRegistry / ansible_hosts)"
[[ -s "$CA_FILE" ]] || die "OpenBao CA file missing: $CA_FILE"
[[ -s "$TOKEN_FILE" ]] || die "OpenBao root token file missing: $TOKEN_FILE"
[[ -r "$SSH_KEY" ]] || die "SSH private key missing or not readable: $SSH_KEY"
[[ -x "$TUNNEL_SCRIPT" ]] || die "canonical tunnel helper missing: $TUNNEL_SCRIPT"

# Keep the OpenBao token out of every curl argv. Curl reads this protected
# config file; the path, not the token, is visible to process inspection.
CURL_CONFIG="$(mktemp)"
chmod 0600 "$CURL_CONFIG"
trap 'rm -f "$CURL_CONFIG"' EXIT
printf 'header = "X-Vault-Token: %s"\n' "$([[ -n "$(<"$TOKEN_FILE")" ]] && cat "$TOKEN_FILE")" > "$CURL_CONFIG"

api_code() {
  local method="$1" path="$2" body_file="${3:-}" out code
  out="$(mktemp)"
  if [[ -n "$body_file" ]]; then
    code="$(curl -sS --config "$CURL_CONFIG" --cacert "$CA_FILE" -X "$method" \
      -H 'Content-Type: application/json' --data-binary "@$body_file" \
      -o "$out" -w '%{http_code}' "$ADDR/v1/$path" 2>/dev/null || true)"
  else
    code="$(curl -sS --config "$CURL_CONFIG" --cacert "$CA_FILE" -X "$method" \
      -o "$out" -w '%{http_code}' "$ADDR/v1/$path" 2>/dev/null || true)"
  fi
  rm -f "$out"
  printf '%s' "$code"
}

kv_code() {
  api_code GET "velox/data/production/workers/$1/$2"
}

kv_value_hash() {
  local worker="$1" leaf="$2" out code hash
  out="$(mktemp)"
  code="$(curl -sS --config "$CURL_CONFIG" --cacert "$CA_FILE" \
    "$ADDR/v1/velox/data/production/workers/$worker/$leaf" \
    -o "$out" -w '%{http_code}' 2>/dev/null || true)"
  if [[ "$code" != "200" ]]; then
    rm -f "$out"
    printf 'ABSENT'
    return 0
  fi
  hash="$(jq -erj '.data.data.value // empty' "$out" \
    | python3 -c 'import sys; sys.stdout.buffer.write(sys.stdin.buffer.read().rstrip(b"\r\n"))' \
    | sha256sum | awk '{print $1}')" \
    || { rm -f "$out"; die "invalid or empty OpenBao value for $worker/$leaf"; }
  rm -f "$out"
  printf '%s' "$hash"
}

verify_approle() {
  local worker="$1" role_dir="$STATE_DIR/approle/worker-$1"
  local role_id secret_id body response token
  role_id="$(<"$role_dir/role-id")"
  secret_id="$(<"$role_dir/secret-id")"
  body="$(mktemp)"
  response="$(mktemp)"
  jq -n --arg role_id "$role_id" --arg secret_id "$secret_id" \
    '{role_id: $role_id, secret_id: $secret_id}' > "$body"
  local code
  code="$(curl -sS --cacert "$CA_FILE" -X POST \
    -H 'Content-Type: application/json' --data-binary "@$body" \
    -o "$response" -w '%{http_code}' \
    "$ADDR/v1/auth/approle/login" 2>/dev/null || true)"
  token="$(jq -er '.auth.client_token // empty' "$response" 2>/dev/null || true)"
  rm -f "$body" "$response"
  [[ "$code" == "200" && -n "$token" ]] \
    || die "AppRole login verification failed for worker-$worker (HTTP $code)"
  log "PASS AppRole login worker-$worker"
}

ssh_worker() {
  local worker="$1" command="$2"
  ssh -o BatchMode=yes -o ConnectTimeout=10 -o ConnectionAttempts=1 \
    -o StrictHostKeyChecking=yes -i "$SSH_KEY" "$SSH_USER@$SSH_HOST" "$command"
}

import_remote_file() {
  local worker="$1" leaf="$2" source="$3" tmp code
  [[ "$IMPORT_LEGACY" == "1" ]] || die "OpenBao leaf missing for $worker/$leaf; rerun with --import-legacy"
  log "importing legacy material for $worker/$leaf (value redacted)"
  tmp="$(mktemp)"
  if ! ssh_worker "$worker" "sudo -n cat '$source'" \
      | python3 -c 'import sys; sys.stdout.buffer.write(sys.stdin.buffer.read().rstrip(b"\r\n"))' \
      | jq -Rs '{data: {value: .}}' > "$tmp"; then
    rm -f "$tmp"
    die "cannot read legacy source for $worker/$leaf"
  fi
  code="$(api_code POST "velox/data/production/workers/$worker/$leaf" "$tmp")"
  rm -f "$tmp"
  [[ "$code" == "200" ]] || die "OpenBao import failed for $worker/$leaf (HTTP $code)"
}

compare_legacy_hashes() {
  local worker="$1" leaf="$2" source="$3" remote_hash bao_hash
  remote_hash="$(ssh_worker "$worker" "sudo -n cat '$source'" \
    | python3 -c 'import sys; sys.stdout.buffer.write(sys.stdin.buffer.read().rstrip(b"\r\n"))' \
    | sha256sum | awk '{print $1}')"
  bao_hash="$(kv_value_hash "$worker" "$leaf")"
  [[ "$remote_hash" =~ ^[[:xdigit:]]{64}$ && "$bao_hash" =~ ^[[:xdigit:]]{64}$ ]] \
    || die "cannot compare legacy credential with OpenBao for $worker/$leaf"
  [[ "$remote_hash" == "$bao_hash" ]] \
    || die "legacy credential hash mismatch for $worker/$leaf; refusing to continue"
}

send_file() {
  local worker="$1" local_file="$2" remote_path="$3" mode="$4"
  [[ -s "$local_file" ]] || die "local source missing or empty: $local_file"
  cat "$local_file" \
    | ssh_worker "$worker" "sudo -n tee '$remote_path' >/dev/null && sudo -n chmod '$mode' '$remote_path'"
}

install_remote_env() {
  local worker="$1"
  ssh -o BatchMode=yes -o ConnectTimeout=10 -o ConnectionAttempts=1 \
    -o StrictHostKeyChecking=yes -i "$SSH_KEY" "$SSH_USER@$SSH_HOST" \
    sudo -n python3 - "$WORKER_ADDR" "$WORKER_CA_FILE" \
      "$ROLE_ID_PATH" "$SECRET_ID_PATH" "$ENV_PATH" <<'PY'
import os
import sys
import tempfile

addr, ca_file, role_file, secret_file, env_path = sys.argv[1:]
updates = {
    "VELOX_OPENBAO_ADDR": addr,
    "VELOX_OPENBAO_CA_FILE": ca_file,
    "VELOX_OPENBAO_ROLE_ID_FILE": role_file,
    "VELOX_OPENBAO_SECRET_ID_FILE": secret_file,
}
if not os.path.isfile(env_path):
    raise SystemExit(f"canonical env missing: {env_path}")
with open(env_path, encoding="utf-8") as handle:
    lines = handle.readlines()
seen = set()
out = []
for line in lines:
    key = line.split("=", 1)[0] if "=" in line else ""
    if key in updates:
        if key not in seen:
            out.append(f"{key}={updates[key]}\n")
            seen.add(key)
        continue
    out.append(line)
for key, value in updates.items():
    if key not in seen:
        out.append(f"{key}={value}\n")
parent = os.path.dirname(env_path)
fd, tmp = tempfile.mkstemp(prefix=".worker.env.", dir=parent, text=True)
try:
    os.fchmod(fd, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        handle.writelines(out)
    os.replace(tmp, env_path)
    os.chmod(env_path, 0o600)
finally:
    try:
        os.unlink(tmp)
    except FileNotFoundError:
        pass
PY
}

install_worker_bootstrap() {
  local worker="$1" role_dir="$STATE_DIR/approle/worker-$1"
  local role_id="$role_dir/role-id" secret_id="$role_dir/secret-id"
  [[ -s "$role_id" && -s "$secret_id" ]] \
    || die "local AppRole material missing for worker-$worker"
  if [[ "$DRY_RUN" == "1" ]]; then
    log "dry-run $worker: would install AppRole, CA, fetcher and OpenBao env"
    return 0
  fi
  ssh_worker "$worker" "sudo -n install -d -m 0700 /etc/velox-worker/secrets/approle /etc/velox-worker/certs"
  send_file "$worker" "$role_id" "$ROLE_ID_PATH" 0600
  send_file "$worker" "$secret_id" "$SECRET_ID_PATH" 0600
  send_file "$worker" "$CA_FILE" "$WORKER_CA_FILE" 0644
  send_file "$worker" "$ROOT/deploy/runtime/openbao-fetch-worker-secrets.sh" "$FETCH_PATH" 0755
  install_remote_env "$worker"
  log "installed canonical OpenBao bootstrap for $worker"
}

verify_worker() {
  local worker="$1"
  [[ "$CHECK" == "1" ]] || return 0
  log "starting canonical reverse tunnel for $worker"
  "$TUNNEL_SCRIPT" start "$worker" "$SSH_HOST" "$SSH_USER" >/dev/null
  local check_file
  check_file="$(mktemp)"
  # Refresh material first so the check compares the runtime cache against
  # OpenBao's normalized value rather than a legacy file that may end in LF.
  local fetch_command
  fetch_command="sudo -n sh -c 'set -a; . /etc/velox-worker/worker.env; set +a; exec /opt/velox-worker/openbao-fetch-worker-secrets.sh --provision'"
  local check_command
  check_command="sudo -n sh -c 'set -a; . /etc/velox-worker/worker.env; set +a; exec /opt/velox-worker/openbao-fetch-worker-secrets.sh --check'"
  if ssh -o BatchMode=yes -o ConnectTimeout=10 -o ConnectionAttempts=1 \
      -o StrictHostKeyChecking=yes -i "$SSH_KEY" "$SSH_USER@$SSH_HOST" \
      "$fetch_command" >"$check_file" 2>&1 \
    && ssh -o BatchMode=yes -o ConnectTimeout=10 -o ConnectionAttempts=1 \
      -o StrictHostKeyChecking=yes -i "$SSH_KEY" "$SSH_USER@$SSH_HOST" \
      "$check_command" >>"$check_file" 2>&1; then
    log "PASS $worker fetcher --check"
  else
    cat "$check_file" >&2 || true
    rm -f "$check_file"
    die "OpenBao fetcher --check failed for $worker"
  fi
  rm -f "$check_file"
}

verify_approle "$WORKER_ID"
# Legacy import is deliberately limited to the static credential. The
# worker.key must remain local; worker.crt/ca.crt come from the PKI CSR flow.
leaf="credential"
source="/etc/velox-worker/secrets/worker_credential"
code="$(kv_code "$WORKER_ID" "$leaf")"
case "$code" in
  200)
    # An existing branch is authoritative only after the legacy runtime
    # credential matches byte-for-byte (apart from a terminal CR/LF). This
    # read-only gate also runs during --dry-run; never overwrite or continue
    # after a mismatch, even without --import-legacy.
    compare_legacy_hashes "$WORKER_ID" "$leaf" "$source"
    ;;
  404)
    [[ "$IMPORT_LEGACY" == "1" ]] \
      || die "OpenBao leaf missing for $WORKER_ID/$leaf; rerun with --import-legacy"
    if [[ "$DRY_RUN" == "1" ]]; then
      log "dry-run $WORKER_ID/$leaf: would import the matching legacy credential"
    else
      import_remote_file "$WORKER_ID" "$leaf" "$source"
      # A successful HTTP write is not sufficient: re-read the canonical value
      # and verify it against the unchanged worker file before installing any
      # AppRole/bootstrap material. This keeps an acknowledged-but-corrupt
      # import fail-closed.
      compare_legacy_hashes "$WORKER_ID" "$leaf" "$source"
    fi
    ;;
  *) die "cannot inspect OpenBao leaf $WORKER_ID/$leaf (HTTP $code)" ;;
esac
install_worker_bootstrap "$WORKER_ID"

verify_worker "$WORKER_ID"

if [[ "$CHECK" == "1" ]]; then
  log "1/1 PASS: $WORKER_ID matches OpenBao via canonical fetcher --check"
else
  log "bootstrap complete for $WORKER_ID; checks skipped"
fi
