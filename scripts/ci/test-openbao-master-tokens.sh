#!/usr/bin/env bash
# scripts/ci/test-openbao-master-tokens.sh
# ─────────────────────────────────────────────────────────────────────────────
# Test della risoluzione token master da OpenBao
# (deploy/openbao/scripts/resolve-master-tokens.sh) contro un MOCK HTTP server
# (python3) che simula AppRole login + KV v2, più check strutturali su policy,
# template e workflow CI (migrazione fase 6).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
RESOLVE="$ROOT/deploy/openbao/scripts/resolve-master-tokens.sh"
MIGRATE="$ROOT/deploy/openbao/scripts/migrate-master-tokens.sh"
TMP="$(mktemp -d)"
MOCK_PID=""
PORT=$((25000 + RANDOM % 20000))
VARS_FILE="$TMP/openbao-vars.yml"
MISSING="${1:-}"   # path (suffisso) che il mock deve rispondere 404, es. instaedit-control-jwt-secret

fail() { printf 'openbao-master-tokens: FAIL: %s\n' "$*" >&2; exit 1; }
pass() { printf 'openbao-master-tokens: %s\n' "$*"; }

cleanup() {
    [[ -z "$MOCK_PID" ]] || kill "$MOCK_PID" 2>/dev/null || true
    rm -rf "$TMP"
}
trap cleanup EXIT

# ── 0. Syntax ────────────────────────────────────────────────────────────────
bash -n "$RESOLVE"
bash -n "$MIGRATE"
if command -v shellcheck >/dev/null 2>&1; then
    shellcheck -x "$RESOLVE" "$MIGRATE"
fi
pass 'syntax OK (bash -n, shellcheck)'

# ── 1. Strutturali (migrazione fase 6) ───────────────────────────────────────
grep -q 'services/registry/\*' "$ROOT/deploy/openbao/policies/master.hcl" \
    || fail 'master.hcl non copre services/registry/*'
grep -q 'read cap on services/registry' "$ROOT/deploy/openbao/scripts/verify-approle.sh" \
    || fail 'verify-approle.sh non verifica l accesso master a services/registry'
grep -q 'Resolve master tokens from OpenBao' "$ROOT/.github/workflows/deploy.yml" \
    || fail 'deploy.yml non contiene lo step di risoluzione OpenBao'
grep -q '/tmp/openbao-vars.yml' "$ROOT/.github/workflows/deploy.yml" \
    || fail 'deploy.yml non inietta gli extra-vars OpenBao'
grep -q 'velox/production/master/admin-token' "$ROOT/deploy/velox-server.env.example" \
    || fail 'velox-server.env.example non documenta l origine OpenBao'
grep -q 'MIGRAZIONE OpenBao' "$ROOT/deploy/group_vars/vault.yml.example" \
    || fail 'vault.yml.example non documenta la migrazione OpenBao'
pass 'check strutturali OK (policy, verify, deploy.yml, template, vault)'

# ── 2. Non configurato → exit 0, nessun vars file ────────────────────────────
if ! OPENBAO_VARS_FILE="$VARS_FILE" bash "$RESOLVE" >/dev/null 2>&1; then
    fail 'senza OPENBAO_ADDR deve uscire 0 (flusso vault legacy)'
fi
[[ -f "$VARS_FILE" ]] && fail 'non configurato non deve scrivere vars file'
pass 'non configurato → exit 0, nessun vars file'

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
    'production/services/registry/username': 'mock-reg-user',
    'production/services/registry/token': 'mock-reg-token',
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
if OPENBAO_ADDR="http://127.0.0.1:$PORT" \
    OPENBAO_ALLOW_INSECURE_HTTP_TEST=1 \
    OPENBAO_ROLE_ID=r OPENBAO_SECRET_ID=wrong \
    OPENBAO_VARS_FILE="$VARS_FILE" bash "$RESOLVE" >/dev/null 2>&1; then
    fail 'secret-id non valido deve far fallire il login (exit 1)'
fi
pass 'login fallito → exit 1 (fail-closed)'

# ── 5. Risoluzione completa → vars file 0600 con i 6 valori ─────────────────
rm -f "$VARS_FILE"
OPENBAO_ADDR="http://127.0.0.1:$PORT" \
    OPENBAO_ALLOW_INSECURE_HTTP_TEST=1 \
    OPENBAO_ROLE_ID=r OPENBAO_SECRET_ID=valid-secret-id \
    OPENBAO_VARS_FILE="$VARS_FILE" bash "$RESOLVE" >/dev/null \
    || fail 'risoluzione completa deve uscire 0'

[[ -f "$VARS_FILE" ]] || fail 'vars file non scritto'
[[ "$(stat -c '%a' "$VARS_FILE")" == "600" ]] || fail 'vars file deve essere 0600'
for pair in \
    'vault_velox_admin_token: "mock-admin-token"' \
    'vault_velox_instaedit_control_jwt_secret: "mock-instaedit-jwt"' \
    'vault_velox_social_api_token: "mock-social-token"' \
    'vault_velox_social_webhook_secret: "mock-webhook"' \
    'vault_velox_registry_username: "mock-reg-user"' \
    'vault_velox_registry_token: "mock-reg-token"'; do
    grep -qxF "$pair" "$VARS_FILE" || fail "vars file non contiene: $pair"
done
pass 'risoluzione completa → 6 extra-vars, file 0600'

# ── 6. Required mancante con --require-all → exit 1 ──────────────────────────
if [[ -z "$MISSING" ]]; then
    # Riavvio del mock con instaedit mancante (404) per il caso fail-closed.
    kill "$MOCK_PID" 2>/dev/null || true
    MOCK_PID=""
    python3 "$TMP/mock.py" "$PORT" "valid-secret-id" "instaedit-control-jwt-secret" > "$TMP/mock.log" 2>&1 &
    MOCK_PID=$!
    sleep 0.3
fi
if OPENBAO_ADDR="http://127.0.0.1:$PORT" \
    OPENBAO_ALLOW_INSECURE_HTTP_TEST=1 \
    OPENBAO_ROLE_ID=r OPENBAO_SECRET_ID=valid-secret-id \
    OPENBAO_VARS_FILE="$VARS_FILE" bash "$RESOLVE" --require-all >/dev/null 2>&1; then
    fail 'required mancante con --require-all deve uscire 1'
fi
pass 'required mancante + --require-all → exit 1 (deploy bloccato)'

# ── 7. migrate-master-tokens: fail-closed senza required ─────────────────────
printf 'VELOX_ADMIN_TOKEN=\nINSTAEDIT_CONTROL_JWT_SECRET=\n' > "$TMP/incomplete.env"
if OPENBAO_DIR="$ROOT/deploy/openbao" bash "$MIGRATE" --env-file "$TMP/incomplete.env" >/dev/null 2>&1; then
    fail 'migrate con required mancanti deve uscire 1'
fi
pass 'migrate fail-closed (required mancanti → exit 1)'

pass 'OK'
