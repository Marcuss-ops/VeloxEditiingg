#!/usr/bin/env bash
# scripts/ci/test-openbao-master-tokens.sh
# ─────────────────────────────────────────────────────────────────────────────
# Test della risoluzione env master da OpenBao
# (deploy/openbao/scripts/resolve-master-env.sh) contro un MOCK HTTP server
# (python3) che simula AppRole login + KV v2, più check strutturali su policy,
# template e workflow CI (migrazione fase 6: no Ansible, no vault_velox_*).
#
# Il resolver materializza l'env file COMPLETO (0600) del master: il test
# verifica che l'output contenga le chiavi runtime canoniche, che NON
# compaia alcun naming legacy vault_velox_*, e che l'env file superi il
# validatore canonico deploy/validate-master-env.sh (stesso gate usato da
# install-server.sh e da scripts/operator/deploy-production.sh).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
RESOLVE="$ROOT/deploy/openbao/scripts/resolve-master-env.sh"
MIGRATE="$ROOT/deploy/openbao/scripts/migrate-master-tokens.sh"
VALIDATOR="$ROOT/deploy/validate-master-env.sh"
TMP="$(mktemp -d)"
MOCK_PID=""
PORT=$((25000 + RANDOM % 20000))
ENV_FILE="$TMP/master.env"
MISSING="${1:-}"   # path (suffisso) che il mock deve rispondere 404, es. instaedit-control-jwt-secret

fail() { printf 'openbao-master-tokens: FAIL: %s\n' "$*" >&2; exit 1; }
pass() { printf 'openbao-master-tokens: %s\n' "$*"; }

cleanup() {
    [[ -z "$MOCK_PID" ]] || kill "$MOCK_PID" 2>/dev/null || true
    rm -rf "$TMP"
}
trap cleanup EXIT

# SHA256 pinned ref per VELOX_SERVER_IMAGE (64 hex).
PINNED_SHA="$(printf 'a%.0s' $(seq 1 64))"

# Input non-secret del resolver: tutti i required del validatore + l'opt-in
# TLS dev (il mock gira su HTTP, quindi INSECURE_DEV=true è obbligatorio).
REQUIRED_ENV=(
    OPENBAO_ADDR="http://127.0.0.1:$PORT"
    OPENBAO_ALLOW_INSECURE_HTTP_TEST=1
    OPENBAO_ENV_FILE="$ENV_FILE"
    VELOX_MASTER_PUBLIC_URL="https://master.example.com"
    VELOX_GRPC_CONTROL_ENDPOINT="master.example.com:9000"
    VELOX_VERSION="1.2.28-test"
    VELOX_SERVER_IMAGE="ghcr.io/marcuss-ops/velox-server@sha256:$PINNED_SHA"
    VELOX_ALLOWED_WORKERS="worker-1,worker-2"
    VELOX_SOCIAL_API_URL="https://social.example.com/api"
    VELOX_SOCIAL_CALLBACK_BASE_URL="https://social.example.com/cb"
    VELOX_GRPC_ALLOW_INSECURE_DEV="true"
)

# ── 0. Syntax ────────────────────────────────────────────────────────────────
bash -n "$RESOLVE"
bash -n "$MIGRATE"
if command -v shellcheck >/dev/null 2>&1; then
    shellcheck -x "$RESOLVE" "$MIGRATE"
fi
pass 'syntax OK (bash -n, shellcheck)'

# ── 1. Strutturali (migrazione fase 6) ───────────────────────────────────────
[[ ! -e "$ROOT/deploy/openbao/scripts/resolve-master-tokens.sh" ]] \
    || fail 'resolve-master-tokens.sh (legacy Ansible extra-vars) deve essere stato eliminato'
[[ -x "$RESOLVE" ]] || fail 'resolve-master-env.sh non presente o non eseguibile'
grep -q 'services/registry/\*' "$ROOT/deploy/openbao/policies/master.hcl" \
    || fail 'master.hcl non copre services/registry/*'
grep -q 'read cap on services/registry' "$ROOT/deploy/openbao/scripts/verify-approle.sh" \
    || fail 'verify-approle.sh non verifica l accesso master a services/registry'
grep -q 'production deploy is local-only' "$ROOT/.github/workflows/deploy.yml" \
    || fail 'deploy.yml deve essere CI-only per la produzione'
grep -q 'scripts/operator/deploy-production.sh' "$ROOT/.github/workflows/deploy.yml" \
    || fail 'deploy.yml non indirizza al deploy locale OpenBao → master'
grep -q 'resolve-master-env.sh' "$ROOT/scripts/operator/deploy-production.sh" \
    || fail 'deploy-production.sh non usa il resolver canonical resolve-master-env.sh'
grep -q 'velox/production/master/admin-token' "$ROOT/deploy/velox-server.env.example" \
    || fail 'velox-server.env.example non documenta l origine OpenBao'
[[ ! -e "$ROOT/deploy/group_vars/vault.yml.example" ]] \
    || fail 'group_vars/vault.yml.example (struttura Ansible Vault legacy) deve essere stato eliminato'
[[ ! -d "$ROOT/deploy/group_vars" ]] \
    || fail 'deploy/group_vars/ deve essere stato eliminato (OpenBao è l unica origine)'
pass 'check strutturali OK (policy, verify, deploy.yml, deploy-production, template, group_vars rimosso)'

# ── 2. Non configurato → exit 1, nessun env file ─────────────────────────────
if OPENBAO_ENV_FILE="$ENV_FILE" bash "$RESOLVE" >/dev/null 2>&1; then
    fail 'senza OPENBAO_ADDR deve fallire (nessun fallback Vault)'
fi
[[ -f "$ENV_FILE" ]] && fail 'non configurato non deve scrivere env file'
pass 'non configurato → exit 1, nessun env file'

# ── 3. Mock server ───────────────────────────────────────────────────────────
cat > "$TMP/mock.py" <<PYEOF
import http.server, json, sys
PORT = int(sys.argv[1])
VALID_SECRET = sys.argv[2]
MISSING = sys.argv[3] if len(sys.argv) > 3 else ''
VALUES = {
    'production/master/admin-token': 'mock-admin-token',
    'production/master/instaedit-control-jwt-secret': 'mock-instaedit-jwt',
    'production/master/social-api-token': 'mock-social-token',
    'production/master/social-webhook-secret': 'mock-webhook',
    'production/master/commit-hmac-key': 'mock-hmac',
}
class H(http.server.BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass
    def _json(self, code, obj):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)
    def do_POST(self):
        ln = int(self.headers.get('Content-Length', 0))
        data = json.loads(self.rfile.read(ln) or b'{}')
        if self.path == '/v1/auth/approle/login':
            if data.get('secret_id') != VALID_SECRET:
                self._json(403, {'errors': ['invalid secret-id']})
                return
            self._json(200, {'auth': {'client_token': 'mock-token'}})
            return
        self._json(404, {'errors': ['not found']})
    def do_GET(self):
        if self.headers.get('X-Vault-Token') != 'mock-token':
            self._json(403, {'errors': ['permission denied']})
            return
        p = self.path
        if not p.startswith('/v1/velox/data/'):
            self._json(404, {'errors': ['not found']})
            return
        key = p[len('/v1/velox/data/'):]
        if MISSING and MISSING in key:
            self._json(404, {'errors': ['not found']})
            return
        if key in VALUES:
            self._json(200, {'data': {'data': {'value': VALUES[key]}}})
            return
        self._json(404, {'errors': ['not found']})
http.server.ThreadingHTTPServer(('127.0.0.1', PORT), H).serve_forever()
PYEOF
python3 "$TMP/mock.py" "$PORT" "valid-secret-id" "$MISSING" > "$TMP/mock.log" 2>&1 &
MOCK_PID=$!
sleep 0.3
for _ in $(seq 1 50); do
    code="$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$PORT/v1/nope" 2>/dev/null || echo 000)"
    [[ "$code" != "000" ]] && break
    sleep 0.1
done
if ! kill -0 "$MOCK_PID" 2>/dev/null; then
    cat "$TMP/mock.log" >&2 || true
    fail 'mock server non è partito'
fi

# ── 4. Login fallito → exit 1 ────────────────────────────────────────────────
if env "${REQUIRED_ENV[@]}" \
    OPENBAO_ROLE_ID=r OPENBAO_SECRET_ID=wrong \
    bash "$RESOLVE" >/dev/null 2>&1; then
    fail 'secret-id non valido deve far fallire il login (exit 1)'
fi
pass 'login fallito → exit 1 (fail-closed)'

# ── 5. Risoluzione completa → env file 0600, chiavi canoniche, valida ────────
rm -f "$ENV_FILE"
env "${REQUIRED_ENV[@]}" \
    OPENBAO_ROLE_ID=r OPENBAO_SECRET_ID=valid-secret-id \
    bash "$RESOLVE" >/dev/null \
    || fail 'risoluzione completa deve uscire 0'

[[ -f "$ENV_FILE" ]] || fail 'env file non scritto'
[[ "$(stat -c '%a' "$ENV_FILE")" == "600" ]] || fail 'env file deve essere 0600'

for pair in \
    'VELOX_ADMIN_TOKEN=mock-admin-token' \
    'INSTAEDIT_CONTROL_JWT_SECRET=mock-instaedit-jwt' \
    'SOCIAL_API_TOKEN=mock-social-token' \
    'SOCIAL_WEBHOOK_SECRET=mock-webhook' \
    'VELOX_COMMIT_HMAC_KEY=mock-hmac'; do
    grep -qxF "$pair" "$ENV_FILE" || fail "env file non contiene: $pair"
done
for line in \
    'GIN_MODE=release' \
    'VELOX_CONTROL_PLANE_REST_PUBLIC_URL=https://master.example.com' \
    'VELOX_CONTROL_PLANE_GRPC_URL=master.example.com:9000' \
    'VELOX_ALLOWED_WORKERS=worker-1,worker-2' \
    "VELOX_SERVER_IMAGE=ghcr.io/marcuss-ops/velox-server@sha256:$PINNED_SHA" \
    'SOCIAL_API_URL=https://social.example.com/api' \
    'SOCIAL_CALLBACK_BASE_URL=https://social.example.com/cb' \
    'VELOX_GRPC_ALLOW_INSECURE_DEV=true'; do
    grep -qxF "$line" "$ENV_FILE" || fail "env file non contiene: $line"
done
if grep -q 'vault_velox_' "$ENV_FILE"; then
    fail 'naming legacy vault_velox_* presente nell env materializzato'
fi
pass 'risoluzione completa → env file 0600 con chiavi canoniche, zero vault_velox_*'

# L'env materializzato deve superare il validatore canonico (stesso gate di
# install-server.sh e deploy-production.sh). Il validatore è tracciato
# non-eseguibile (0644): si invoca via `bash`, come fa install-server.sh.
bash "$VALIDATOR" "$ENV_FILE" || fail 'env file deve superare deploy/validate-master-env.sh'
pass 'env materializzato → validatore canonico PASS'

# ── 6. Required mancante con --require-all → exit 1 ──────────────────────────
if [[ -z "$MISSING" ]]; then
    # Riavvio del mock con instaedit mancante (404) per il caso fail-closed.
    kill "$MOCK_PID" 2>/dev/null || true
    MOCK_PID=""
    python3 "$TMP/mock.py" "$PORT" "valid-secret-id" "instaedit-control-jwt-secret" > "$TMP/mock.log" 2>&1 &
    MOCK_PID=$!
    sleep 0.3
fi
rm -f "$ENV_FILE"
if env "${REQUIRED_ENV[@]}" \
    OPENBAO_ROLE_ID=r OPENBAO_SECRET_ID=valid-secret-id \
    bash "$RESOLVE" --require-all >/dev/null 2>&1; then
    fail 'required mancante con --require-all deve uscire 1'
fi
[[ -f "$ENV_FILE" ]] && fail 'required mancante non deve scrivere env file'
pass 'required mancante + --require-all → exit 1, nessun env file (deploy bloccato)'

# ── 7. migrate-master-tokens: fail-closed senza required ─────────────────────
printf 'VELOX_ADMIN_TOKEN=\nINSTAEDIT_CONTROL_JWT_SECRET=\n' > "$TMP/incomplete.env"
if OPENBAO_DIR="$ROOT/deploy/openbao" bash "$MIGRATE" --env-file "$TMP/incomplete.env" >/dev/null 2>&1; then
    fail 'migrate con required mancanti deve uscire 1'
fi
pass 'migrate fail-closed (required mancanti → exit 1)'

pass 'OK'
