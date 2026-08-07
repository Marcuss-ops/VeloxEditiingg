#!/usr/bin/env bash
# scripts/ci/test-openbao-worker-secrets.sh
# ─────────────────────────────────────────────────────────────────────────────
# Test del resolver worker-secrets da OpenBao
# (deploy/runtime/openbao-fetch-worker-secrets.sh) contro un MOCK HTTP server
# locale (python3) che simula l'AppRole login + il KV v2 di OpenBao.
#
# Copre: non-configurato (fallback file), login fallito, fetch completo,
# idempotenza, --check coerente, --check con mismatch, cert non provisionati.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
FETCH="$ROOT/deploy/runtime/openbao-fetch-worker-secrets.sh"
TMP="$(mktemp -d)"
MOCK_PID=""
PORT=$((20000 + RANDOM % 20000))
SECRETS_DIR="$TMP/secrets"
CERTS_DIR="$TMP/certs"

fail() { printf 'openbao-worker-secrets: FAIL: %s\n' "$*" >&2; exit 1; }
pass() { printf 'openbao-worker-secrets: %s\n' "$*"; }

cleanup() {
    [[ -z "$MOCK_PID" ]] || kill "$MOCK_PID" 2>/dev/null || true
    rm -rf "$TMP"
}
trap cleanup EXIT

# ── 0. Syntax ────────────────────────────────────────────────────────────────
bash -n "$FETCH"
bash -n "$ROOT/deploy/runtime/prepare-host.sh"
if command -v shellcheck >/dev/null 2>&1; then
    shellcheck -x "$FETCH"
fi
pass 'syntax OK (bash -n, shellcheck)'

# ── 1. Non configurato → exit 0 senza scrivere nulla ────────────────────────
env -i PATH="$PATH" HOME="$HOME" \
    VELOX_WORKER_SECRETS_DIR="$SECRETS_DIR" VELOX_WORKER_CERTS_DIR="$CERTS_DIR" \
    VELOX_WORKER_ID=w1 bash "$FETCH" >/dev/null 2>&1 \
    || fail 'senza VELOX_OPENBAO_ADDR deve uscire 0 (fallback file)'
[[ -d "$SECRETS_DIR" ]] && fail 'non-configurato non deve creare dirs'
pass 'non configurato → exit 0, nessuna scrittura'

# ── 1b. --check su host NON configurato → exit 1 (no false positive) ────────
if env -i PATH="$PATH" HOME="$HOME" \
    VELOX_WORKER_ID=w1 \
    VELOX_WORKER_SECRETS_DIR="$SECRETS_DIR" VELOX_WORKER_CERTS_DIR="$CERTS_DIR" \
    bash "$FETCH" --check >/dev/null 2>&1; then
    fail '--check su host non configurato deve uscire 1 (impossibile verificare)'
fi
pass '--check non configurato → exit 1 (no false positive)'

# ── 2. Mock server ───────────────────────────────────────────────────────────
cat > "$TMP/mock.py" <<'PYEOF'
import http.server, json, sys
PORT = int(sys.argv[1])
VALID_SECRET = sys.argv[2]
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
        m = {}
        if p.endswith('/credential'):
            m = {'value': 'secret-cred-1'}
        elif '/workers/w1/' in p:
            if p.endswith('/cert/cert'):
                m = {'value': 'FAKE-CERT-PEM'}
            elif p.endswith('/cert/key'):
                m = {'value': 'FAKE-KEY-PEM'}
            elif p.endswith('/cert/ca'):
                m = {'value': 'FAKE-CA-PEM'}
        if m:
            self._json(200, {'data': {'data': m}})
            return
        self._json(404, {'errors': ['not found']})
# Threading: una connessione interrotta dal client (es. curl -f che chiude
# su 403) non deve far morire il server durante il test.
http.server.ThreadingHTTPServer(('127.0.0.1', PORT), H).serve_forever()
PYEOF
python3 "$TMP/mock.py" "$PORT" "valid-secret-id" > "$TMP/mock.log" 2>&1 &
MOCK_PID=$!
sleep 0.3
for _ in $(seq 1 50); do
    code="$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$PORT/v1/nope" 2>/dev/null || echo 000)"
    [[ "$code" != "000" ]] && break
    sleep 0.1
done
if ! kill -0 "$MOCK_PID" 2>/dev/null; then
    cat "$TMP/mock.log" >&2 || true
    fail 'mock server non è partito (vedi mock.log sopra)'
fi

mkdir -p "$SECRETS_DIR" "$CERTS_DIR"
printf 'not-used' > "$SECRETS_DIR/approle-role-id"
printf 'not-used' > "$SECRETS_DIR/approle-secret-id"

# ── 3. Login fallito → exit 1 ────────────────────────────────────────────────
if env -i PATH="$PATH" HOME="$HOME" \
    VELOX_OPENBAO_ADDR="http://127.0.0.1:$PORT" \
    VELOX_OPENBAO_ROLE_ID_FILE="$SECRETS_DIR/approle-role-id" \
    VELOX_OPENBAO_SECRET_ID_FILE="$SECRETS_DIR/approle-secret-id" \
    VELOX_WORKER_ID=w1 \
    VELOX_WORKER_SECRETS_DIR="$SECRETS_DIR" VELOX_WORKER_CERTS_DIR="$CERTS_DIR" \
    bash "$FETCH" >/dev/null 2>&1; then
    fail 'secret-id non valido deve far fallire il login (exit 1)'
fi
pass 'login fallito → exit 1 (fail-closed)'

# ── 4. Fetch completo → exit 0, file scritti coi valori del KV ───────────────
printf 'valid-secret-id' > "$SECRETS_DIR/approle-secret-id"
env -i PATH="$PATH" HOME="$HOME" \
    VELOX_OPENBAO_ADDR="http://127.0.0.1:$PORT" \
    VELOX_OPENBAO_ROLE_ID_FILE="$SECRETS_DIR/approle-role-id" \
    VELOX_OPENBAO_SECRET_ID_FILE="$SECRETS_DIR/approle-secret-id" \
    VELOX_WORKER_ID=w1 \
    VELOX_WORKER_SECRETS_DIR="$SECRETS_DIR" VELOX_WORKER_CERTS_DIR="$CERTS_DIR" \
    bash "$FETCH" >/dev/null \
    || fail 'fetch completo deve uscire 0'

[[ "$(cat "$SECRETS_DIR/worker_credential")" == "secret-cred-1" ]] \
    || fail 'worker_credential non risolto dal KV'
[[ "$(cat "$CERTS_DIR/worker.crt")" == "FAKE-CERT-PEM" ]] \
    || fail 'worker.crt non risolto dal KV'
[[ "$(cat "$CERTS_DIR/worker.key")" == "FAKE-KEY-PEM" ]] \
    || fail 'worker.key non risolto dal KV'
[[ "$(cat "$CERTS_DIR/ca.crt")" == "FAKE-CA-PEM" ]] \
    || fail 'ca.crt non risolto dal KV'
[[ "$(stat -c '%a' "$CERTS_DIR/worker.key")" == "600" ]] \
    || fail 'worker.key deve essere 0600'
[[ "$(stat -c '%a' "$CERTS_DIR/worker.crt")" == "644" ]] \
    || fail 'worker.crt deve essere 0644'
[[ "$(stat -c '%a' "$SECRETS_DIR/worker_credential")" == "600" ]] \
    || fail 'worker_credential deve essere 0600'
pass 'fetch completo → credential + 3 cert scritti con permessi canonici'

# ── 5. Idempotenza ───────────────────────────────────────────────────────────
env -i PATH="$PATH" HOME="$HOME" \
    VELOX_OPENBAO_ADDR="http://127.0.0.1:$PORT" \
    VELOX_OPENBAO_ROLE_ID_FILE="$SECRETS_DIR/approle-role-id" \
    VELOX_OPENBAO_SECRET_ID_FILE="$SECRETS_DIR/approle-secret-id" \
    VELOX_WORKER_ID=w1 \
    VELOX_WORKER_SECRETS_DIR="$SECRETS_DIR" VELOX_WORKER_CERTS_DIR="$CERTS_DIR" \
    bash "$FETCH" >/dev/null \
    || fail 'secondo fetch deve uscire 0 (idempotente)'
pass 'fetch idempotente'

# ── 6. --check coerente → exit 0 ─────────────────────────────────────────────
env -i PATH="$PATH" HOME="$HOME" \
    VELOX_OPENBAO_ADDR="http://127.0.0.1:$PORT" \
    VELOX_OPENBAO_ROLE_ID_FILE="$SECRETS_DIR/approle-role-id" \
    VELOX_OPENBAO_SECRET_ID_FILE="$SECRETS_DIR/approle-secret-id" \
    VELOX_WORKER_ID=w1 \
    VELOX_WORKER_SECRETS_DIR="$SECRETS_DIR" VELOX_WORKER_CERTS_DIR="$CERTS_DIR" \
    bash "$FETCH" --check >/dev/null \
    || fail '--check su file coerenti deve uscire 0'
pass '--check coerente → exit 0'

# ── 7. --check con mismatch → exit 1 ─────────────────────────────────────────
printf 'tampered' > "$SECRETS_DIR/worker_credential"
if env -i PATH="$PATH" HOME="$HOME" \
    VELOX_OPENBAO_ADDR="http://127.0.0.1:$PORT" \
    VELOX_OPENBAO_ROLE_ID_FILE="$SECRETS_DIR/approle-role-id" \
    VELOX_OPENBAO_SECRET_ID_FILE="$SECRETS_DIR/approle-secret-id" \
    VELOX_WORKER_ID=w1 \
    VELOX_WORKER_SECRETS_DIR="$SECRETS_DIR" VELOX_WORKER_CERTS_DIR="$CERTS_DIR" \
    bash "$FETCH" --check >/dev/null 2>&1; then
    fail '--check con credential alterato deve uscire 1'
fi
printf 'secret-cred-1' > "$SECRETS_DIR/worker_credential"
pass '--check con mismatch → exit 1'

# ── 8. Cert non provisionati (worker w2: credential sì, cert 404) ───────────
# NB: dirs pulite — i file di w1 dal caso 4 devono sparire prima di questo test.
rm -f "$CERTS_DIR"/worker.crt "$CERTS_DIR"/worker.key "$CERTS_DIR"/ca.crt \
      "$SECRETS_DIR"/worker_credential
env -i PATH="$PATH" HOME="$HOME" \
    VELOX_OPENBAO_ADDR="http://127.0.0.1:$PORT" \
    VELOX_OPENBAO_ROLE_ID_FILE="$SECRETS_DIR/approle-role-id" \
    VELOX_OPENBAO_SECRET_ID_FILE="$SECRETS_DIR/approle-secret-id" \
    VELOX_WORKER_ID=w2 \
    VELOX_WORKER_SECRETS_DIR="$SECRETS_DIR" VELOX_WORKER_CERTS_DIR="$CERTS_DIR" \
    bash "$FETCH" >/dev/null \
    || fail 'worker senza cert in OpenBao deve uscire 0 (cert skip, credential scritta)'
[[ "$(cat "$SECRETS_DIR/worker_credential")" == "secret-cred-1" ]] \
    || fail 'credential di w2 non scritta'
[[ -f "$CERTS_DIR/worker.crt" ]] \
    && fail 'cert di w2 non deve essere scritto (404 in OpenBao)'
pass 'cert non provisionati → skip senza errori'

# ── 9. Errore di trasporto (server giù) → exit 1 ─────────────────────────────
kill "$MOCK_PID" 2>/dev/null || true
MOCK_PID=""
if env -i PATH="$PATH" HOME="$HOME" \
    VELOX_OPENBAO_ADDR="http://127.0.0.1:$PORT" \
    VELOX_OPENBAO_ROLE_ID_FILE="$SECRETS_DIR/approle-role-id" \
    VELOX_OPENBAO_SECRET_ID_FILE="$SECRETS_DIR/approle-secret-id" \
    VELOX_WORKER_ID=w1 \
    VELOX_WORKER_SECRETS_DIR="$SECRETS_DIR" VELOX_WORKER_CERTS_DIR="$CERTS_DIR" \
    bash "$FETCH" >/dev/null 2>&1; then
    fail 'server irraggiungibile deve far fallire il fetch (exit 1)'
fi
pass 'server giù → exit 1 (fail-closed, il chiamante decide il fallback)'

pass 'OK'
