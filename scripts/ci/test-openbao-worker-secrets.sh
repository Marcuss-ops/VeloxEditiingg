#!/usr/bin/env bash
# scripts/ci/test-openbao-worker-secrets.sh
# Offline contract test for deploy/runtime/openbao-fetch-worker-secrets.sh.
# The mock exposes AppRole login, KV credential read, and OpenBao PKI CSR sign.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
FETCH="$ROOT/deploy/runtime/openbao-fetch-worker-secrets.sh"
PREPARE="$ROOT/deploy/runtime/prepare-host.sh"
PKI_PROVISION="$ROOT/deploy/openbao/scripts/provision-pki.sh"
KV_PROVISION="$ROOT/deploy/openbao/scripts/provision-kv.sh"
WORKER_POLICY="$ROOT/deploy/openbao/policies/worker.hcl.tmpl"
OPERATOR_BOOTSTRAP="$ROOT/scripts/operator/provision-worker-openbao.sh"
TMP="$(mktemp -d)"
MOCK_PID=""
PROVISION_PID=""
PORT=$((20000 + RANDOM % 20000))
SECRETS_DIR="$TMP/secrets"
CERTS_DIR="$TMP/certs"
RUNTIME_CERTS_DIR="$CERTS_DIR/current"
MODE_FILE="$TMP/mode"
CA_DIR="$TMP/ca"

fail() { printf 'openbao-worker-secrets: FAIL: %s\n' "$*" >&2; exit 1; }
pass() { printf 'openbao-worker-secrets: %s\n' "$*"; }
cleanup() {
    [[ -z "$MOCK_PID" ]] || kill "$MOCK_PID" 2>/dev/null || true
    [[ -z "$PROVISION_PID" ]] || kill "$PROVISION_PID" 2>/dev/null || true
    rm -rf "$TMP"
}
trap cleanup EXIT

bash -n "$FETCH" "$PREPARE" "$PKI_PROVISION" "$KV_PROVISION" "$OPERATOR_BOOTSTRAP"
if command -v shellcheck >/dev/null 2>&1; then
    shellcheck -x "$FETCH" "$PKI_PROVISION" "$KV_PROVISION" "$OPERATOR_BOOTSTRAP" ||
        fail 'shellcheck failed'
fi
command -v openssl >/dev/null 2>&1 || fail 'openssl is required for the CSR test'

# Static contract: worker mTLS is PKI/CSR material, never a KV leaf. Keep this
# assertion close to the live mock so a future operator or policy change cannot
# silently reintroduce worker.key/cert/CA storage in OpenBao KV.
grep -q 'worker.key never enters the request body or KV' "$FETCH" \
    || fail 'resolver does not document the local-key/no-KV contract'
grep -q 'path "velox/data/production/workers/{{ WORKER_ID }}/credential"' "$WORKER_POLICY" \
    || fail 'worker policy does not expose only its credential KV path'
grep -q 'pki/sign/worker-{{ WORKER_ID }}' "$WORKER_POLICY" \
    || fail 'worker policy does not expose its dedicated PKI sign path'
grep -q 'compare_legacy_hashes' "$OPERATOR_BOOTSTRAP" \
    || fail 'operator bootstrap lacks the fail-closed existing-branch hash gate'
grep -q 'refusing to continue' "$OPERATOR_BOOTSTRAP" \
    || fail 'operator bootstrap does not stop on hash mismatch'
grep -q 'rstrip(b"\\\\r\\\\n")' "$OPERATOR_BOOTSTRAP" \
    || fail 'operator bootstrap does not normalize terminal line endings before hashing'
grep -Fq "allowed_uri_sans=\"\$spiffe_uri\"" "$PKI_PROVISION" \
    || fail 'PKI provisioning does not constrain the per-worker SPIFFE URI'
grep -q 'use_csr_sans=true' "$PKI_PROVISION" \
    || fail 'PKI provisioning does not preserve the CSR URI SAN'
grep -q 'path "pki/issue/worker-{{ WORKER_ID }}"' "$WORKER_POLICY" \
    || fail 'worker policy does not explicitly deny the issue path'
policy_a="$TMP/worker-a.hcl"
sed 's/{{ WORKER_ID }}/worker-a/g' "$WORKER_POLICY" > "$policy_a"
grep -q 'velox/data/production/workers/worker-a/credential' "$policy_a" \
    || fail 'worker-a policy lost its own credential path'
grep -q 'pki/sign/worker-worker-a' "$policy_a" \
    || fail 'worker-a policy lost its own PKI role'
if grep -q 'worker-b' "$policy_a"; then
    fail 'worker-a policy contains a cross-worker identity'
fi
if grep -Eq 'cert/(cert|key|ca)|OPENBAO_VALUE_WORKER_(CERT|KEY|CA)|/etc/velox-worker/certs/(worker\.crt|worker\.key|ca\.crt)' \
    "$KV_PROVISION" "$OPERATOR_BOOTSTRAP"; then
    fail 'worker mTLS certificate/key/CA import contract was reintroduced'
fi
pass 'syntax + PKI local-key/no-KV contract OK (bash -n, shellcheck, static)'

# Exercise the operator-side credential gate completely offline. The fake SSH
# command returns the worker's legacy credential and safely consumes all other
# bootstrap streams; the mock OpenBao server varies only the credential leaf.
PROVISION_TMP="$TMP/operator"
PROVISION_BIN="$PROVISION_TMP/bin"
PROVISION_STATE="$PROVISION_TMP/state"
PROVISION_PORT=$((40000 + RANDOM % 10000))
PROVISION_MODE="$PROVISION_TMP/mode"
PROVISION_POSTS="$PROVISION_TMP/posts"
PROVISION_ALL_POSTS="$PROVISION_TMP/all-non-login-posts"
PROVISION_SSH_LOG="$PROVISION_TMP/ssh.log"
mkdir -p "$PROVISION_BIN" "$PROVISION_STATE/approle/worker-w1"
printf 'role-id' > "$PROVISION_STATE/approle/worker-w1/role-id"
printf 'secret-id' > "$PROVISION_STATE/approle/worker-w1/secret-id"
printf 'root-token' > "$PROVISION_STATE/root-token"
printf 'test-ca' > "$PROVISION_STATE/ca.crt"
printf 'ssh-key' > "$PROVISION_TMP/ssh-key"
printf 'match' > "$PROVISION_MODE"
: > "$PROVISION_POSTS"
: > "$PROVISION_ALL_POSTS"
: > "$PROVISION_SSH_LOG"
cat > "$PROVISION_BIN/ansible-inventory" <<'EOF'
#!/usr/bin/env bash
printf '%s\\n' '{"_meta":{"hostvars":{"mock-host":{"worker_id":"w1","ansible_host":"127.0.0.1","ansible_user":"mock"}}}}'
EOF
cat > "$PROVISION_BIN/ssh" <<'EOF'
#!/usr/bin/env bash
command="${*: -1}"
printf '%s\\n' "$command" >> "${PROVISION_SSH_LOG:?}"
if [[ "$command" == *"sudo -n cat '/etc/velox-worker/secrets/worker_credential'"* ]]; then
    printf 'secret-cred-1\\r\\n'
else
    cat >/dev/null
fi
EOF
cat > "$PROVISION_BIN/remote-worker-openbao-tunnel.sh" <<'EOF'
#!/usr/bin/env bash
exit 0
EOF
chmod 0755 "$PROVISION_BIN/ansible-inventory" "$PROVISION_BIN/ssh" "$PROVISION_BIN/remote-worker-openbao-tunnel.sh"
cat > "$PROVISION_TMP/mock.py" <<'PYEOF'
import http.server, json, os, sys
PORT, mode_file, posts_file = map(str, sys.argv[1:])
class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass
    def reply(self, code, payload):
        body = json.dumps(payload).encode()
        self.send_response(code)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)
    def do_GET(self):
        if self.headers.get('X-Vault-Token') != 'root-token':
            self.reply(403, {'errors': ['invalid token']})
            return
        if self.path == '/v1/velox/data/production/workers/w1/credential':
            if open(mode_file).read().strip() == 'missing':
                self.reply(404, {'errors': ['missing']})
            else:
                value = 'secret-cred-1\\r\\n' if open(mode_file).read().strip() == 'match' else 'different-credential'
                self.reply(200, {'data': {'data': {'value': value}}})
            return
        self.reply(404, {'errors': ['not found']})
    def do_POST(self):
        length = int(self.headers.get('Content-Length', 0))
        body = self.rfile.read(length)
        if self.path != '/v1/auth/approle/login':
            with open(os.path.join(os.path.dirname(posts_file), 'all-non-login-posts'), 'ab') as handle:
                handle.write(self.path.encode() + b' ' + body + b'\\n')
        if self.path == '/v1/auth/approle/login':
            self.reply(200, {'auth': {'client_token': 'mock-token'}})
            return
        if self.path == '/v1/velox/data/production/workers/w1/credential':
            if self.headers.get('X-Vault-Token') != 'root-token':
                self.reply(403, {'errors': ['invalid token']})
                return
            try:
                payload = json.loads(body)
                if payload.get('data', {}).get('value') != 'secret-cred-1':
                    raise ValueError('unexpected credential payload')
            except (ValueError, json.JSONDecodeError):
                self.reply(400, {'errors': ['invalid credential payload']})
                return
            with open(posts_file, 'ab') as handle:
                handle.write(body + b'\\n')
            self.reply(200, {'data': {}})
            return
        self.reply(404, {'errors': ['not found']})
http.server.ThreadingHTTPServer(('127.0.0.1', int(PORT)), Handler).serve_forever()
PYEOF
python3 "$PROVISION_TMP/mock.py" "$PROVISION_PORT" "$PROVISION_MODE" "$PROVISION_POSTS" >/dev/null 2>&1 &
PROVISION_PID=$!
for _ in $(seq 1 50); do
    code="$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$PROVISION_PORT/v1/nope" 2>/dev/null || true)"
    [[ "$code" =~ ^[0-9]{3}$ && "$code" != 000 ]] && break
    sleep 0.1
done
kill -0 "$PROVISION_PID" 2>/dev/null || fail 'operator mock server did not start'
provision_env=(
    PATH="$PROVISION_BIN:$PATH"
    HOME="$HOME"
    OPENBAO_OPERATOR_ADDR="http://127.0.0.1:$PROVISION_PORT"
    OPENBAO_CA_FILE="$PROVISION_STATE/ca.crt"
    OPENBAO_STATE_DIR="$PROVISION_STATE"
    VELOX_WORKER_INVENTORY="$PROVISION_TMP/inventory.ini"
    VELOX_SSH_KEY="$PROVISION_TMP/ssh-key"
    PROVISION_SSH_LOG="$PROVISION_SSH_LOG"
)
printf '[mock inventory]\\n' > "$PROVISION_TMP/inventory.ini"
run_operator() {
    env -i "${provision_env[@]}" bash "$OPERATOR_BOOTSTRAP" --worker w1 "$@"
}
run_operator --dry-run >/dev/null 2>&1 \
    || fail 'matching existing OpenBao branch did not pass the hash gate'
pass 'matching existing branch → hash equality permits continuation'
printf 'mismatch' > "$PROVISION_MODE"
: > "$PROVISION_SSH_LOG"
: > "$PROVISION_ALL_POSTS"
if run_operator --dry-run >/dev/null 2>"$PROVISION_TMP/mismatch.log"; then
    fail 'mismatched existing OpenBao branch unexpectedly continued'
fi
if [[ -s "$PROVISION_ALL_POSTS" || -s "$PROVISION_POSTS" ]]; then
    fail 'hash mismatch caused an unexpected OpenBao write'
fi
grep -Fq 'legacy credential hash mismatch for w1/credential; refusing to continue' "$PROVISION_TMP/mismatch.log" \
    || fail 'hash mismatch did not fail closed with a redacted diagnostic'
if grep -Eq 'tee|worker\.env|install -d' "$PROVISION_SSH_LOG"; then
    fail 'hash mismatch reached a remote write or bootstrap command'
fi
if grep -Eq 'secret-cred-1|different-credential|[[:xdigit:]]{64}' "$PROVISION_TMP/mismatch.log"; then
    fail 'hash mismatch diagnostic exposed a secret or hash'
fi
pass 'mismatched existing branch → aborts before any write'
printf 'missing' > "$PROVISION_MODE"
: > "$PROVISION_POSTS"
if run_operator --dry-run >/dev/null 2>&1; then
    fail 'missing OpenBao branch without --import-legacy unexpectedly continued'
fi
pass 'missing branch without import authorization → rejected'
run_operator --import-legacy --dry-run >/dev/null 2>&1 \
    || fail 'dry-run authorized import unexpectedly failed'
[[ ! -s "$PROVISION_POSTS" ]] || fail 'dry-run authorized import performed a write'
pass 'dry-run authorized import → no write'
run_operator --import-legacy --no-check >/dev/null 2>&1 \
    || fail 'missing branch with --import-legacy did not import and continue'
grep -Fq 'secret-cred-1' "$PROVISION_POSTS" \
    || fail 'authorized legacy import did not write the credential leaf'
pass 'missing branch with --import-legacy → imports and continues'
kill "$PROVISION_PID" 2>/dev/null || true
PROVISION_PID=""

# Deploy operations are strict: no OpenBao address means no provisioning.
env -i PATH="$PATH" HOME="$HOME" \
    VELOX_WORKER_SECRETS_DIR="$SECRETS_DIR" VELOX_WORKER_CERTS_DIR="$CERTS_DIR" \
    VELOX_WORKER_ID=w1 bash "$FETCH" --provision >/dev/null 2>&1 \
    && fail 'provisioning without VELOX_OPENBAO_ADDR unexpectedly succeeded'
[[ ! -d "$CERTS_DIR" ]] || fail 'failed provisioning must not create runtime directories'
pass 'provisioning without OpenBao → fail closed and no writes'

# Cache mode is explicit and read-only; without an OpenBao provenance marker it
# must reject even otherwise valid-looking local material.
env -i PATH="$PATH" HOME="$HOME" \
    VELOX_WORKER_SECRETS_DIR="$SECRETS_DIR" VELOX_WORKER_CERTS_DIR="$CERTS_DIR" \
    VELOX_WORKER_ID=w1 bash "$FETCH" --runtime-cache >/dev/null 2>&1 \
    && fail 'unattested runtime cache unexpectedly succeeded'
pass 'unattested runtime cache → rejected'

mkdir -p "$SECRETS_DIR" "$CERTS_DIR" "$CA_DIR"
printf 'role-id' > "$SECRETS_DIR/role-id"
printf 'valid-secret-id' > "$SECRETS_DIR/secret-id"
printf 'ok' > "$MODE_FILE"

openssl req -x509 -newkey rsa:2048 -nodes -days 30 \
    -subj '/CN=Velox Test CA' -keyout "$CA_DIR/ca.key" -out "$CA_DIR/ca.crt" \
    >/dev/null 2>&1

cat > "$TMP/mock.py" <<'PYEOF'
import http.server, json, os, subprocess, sys, tempfile
PORT, SECRET, CA_CERT, CA_KEY, MODE_FILE = sys.argv[1:]
PORT = int(PORT)
class H(http.server.BaseHTTPRequestHandler):
    sign_calls = 0
    def log_message(self, *args): pass
    def _json(self, code, obj):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)
    def do_POST(self):
        length = int(self.headers.get('Content-Length', 0))
        raw = self.rfile.read(length)
        try: data = json.loads(raw)
        except Exception: self._json(400, {'errors':['invalid json']}); return
        if self.path == '/v1/auth/approle/login':
            if data.get('secret_id') != SECRET: self._json(403, {'errors':['invalid secret-id']}); return
            self._json(200, {'auth': {'client_token': 'mock-token'}}); return
        if self.path == '/v1/pki/sign/worker-w1':
            H.sign_calls += 1
            with open(os.path.join(os.path.dirname(MODE_FILE), 'request.json'), 'w') as f: json.dump(data, f)
            if open(MODE_FILE).read().strip() == 'fail': self._json(500, {'errors':['sign failed']}); return
            csr = data.get('csr', '')
            if 'PRIVATE KEY' in csr or 'worker.key' in raw.decode(errors='ignore'):
                self._json(400, {'errors':['private key disclosure']}); return
            with tempfile.TemporaryDirectory() as d:
                csr_file = os.path.join(d, 'worker.csr')
                cert_file = os.path.join(d, 'worker.crt')
                with open(csr_file, 'w') as f: f.write(csr)
                ext_file = os.path.join(d, 'ext.cnf')
                identity = 'w1' if open(MODE_FILE).read().strip() != 'wrong-identity' else 'other-worker'
                with open(ext_file, 'w') as f:
                    f.write('basicConstraints=CA:FALSE\nextendedKeyUsage=clientAuth\nkeyUsage=digitalSignature,keyEncipherment\nsubjectAltName=URI:spiffe://velox/worker/' + identity + '\n')
                p = subprocess.run(['openssl','x509','-req','-in',csr_file,'-CA',CA_CERT,'-CAkey',CA_KEY,'-CAcreateserial','-out',cert_file,'-days','20','-sha256','-extfile',ext_file], capture_output=True)
                if p.returncode != 0: self._json(500, {'errors':['certificate signing failed']}); return
                certificate = open(cert_file).read()
            self._json(200, {'data': {'certificate': certificate, 'issuing_ca': open(CA_CERT).read(), 'ca_chain': [open(CA_CERT).read()]}}); return
        self._json(404, {'errors':['not found']})
    def do_GET(self):
        if self.headers.get('X-Vault-Token') != 'mock-token': self._json(403, {'errors':['permission denied']}); return
        if self.path == '/v1/velox/data/production/workers/w1/credential':
            self._json(200, {'data': {'data': {'value': 'secret-cred-1'}}}); return
        self._json(404, {'errors':['not found']})
http.server.ThreadingHTTPServer(('127.0.0.1', PORT), H).serve_forever()
PYEOF
python3 "$TMP/mock.py" "$PORT" valid-secret-id "$CA_DIR/ca.crt" "$CA_DIR/ca.key" "$MODE_FILE" >/dev/null 2>&1 &
MOCK_PID=$!
for _ in $(seq 1 50); do
    code="$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$PORT/v1/nope" 2>/dev/null || true)"
    [[ "$code" =~ ^[0-9]{3}$ && "$code" != 000 ]] && break
    sleep 0.1
done
kill -0 "$MOCK_PID" 2>/dev/null || fail 'mock server did not start'

common_env=(
    VELOX_OPENBAO_ADDR="http://127.0.0.1:$PORT"
    VELOX_OPENBAO_ALLOW_INSECURE_HTTP_TEST=1
    VELOX_OPENBAO_ROLE_ID_FILE="$SECRETS_DIR/role-id"
    VELOX_OPENBAO_SECRET_ID_FILE="$SECRETS_DIR/secret-id"
    VELOX_WORKER_ID=w1
    VELOX_WORKER_SECRETS_DIR="$SECRETS_DIR"
    VELOX_WORKER_CERTS_DIR="$CERTS_DIR"
    VELOX_OPENBAO_PKI_ROLE=worker-w1
    VELOX_MTLS_RENEW_BEFORE_SECONDS=604800
    IMAGE_UID="$(id -u)"
    IMAGE_GID="$(id -g)"
)
env -i PATH="$PATH" HOME="$HOME" "${common_env[@]}" bash "$FETCH" --provision >/dev/null \
    || fail 'initial AppRole + KV + CSR flow failed'

[[ "$(cat "$SECRETS_DIR/worker_credential")" == secret-cred-1 ]] || fail 'credential not resolved from KV'
[[ -L "$CERTS_DIR/current" ]] || fail 'mTLS current bundle is not an atomic symlink'
[[ -s "$RUNTIME_CERTS_DIR/worker.key" && -s "$RUNTIME_CERTS_DIR/worker.crt" && -s "$RUNTIME_CERTS_DIR/ca.crt" && -s "$RUNTIME_CERTS_DIR/.openbao-pki-issued" ]] || fail 'mTLS runtime bundle missing'
[[ "$(stat -c '%a' "$RUNTIME_CERTS_DIR/worker.key")" == 600 ]] || fail 'worker.key must be 0600'
[[ "$(stat -c '%a' "$RUNTIME_CERTS_DIR/worker.crt")" == 644 ]] || fail 'worker.crt must be 0644'
[[ "$(stat -c '%a' "$RUNTIME_CERTS_DIR/ca.crt")" == 644 ]] || fail 'ca.crt must be 0644'
openssl verify -CAfile "$RUNTIME_CERTS_DIR/ca.crt" "$RUNTIME_CERTS_DIR/worker.crt" >/dev/null || fail 'issued cert does not verify'
openssl x509 -in "$RUNTIME_CERTS_DIR/worker.crt" -noout -subject -nameopt RFC2253 |
    sed 's/^subject=//' | tr ',' '\n' | grep -Fxq 'CN=w1' || fail 'certificate CN identity is not exact w1'
python3 - "$TMP/request.json" <<'PY'
import json, sys
request = json.load(open(sys.argv[1]))
assert request['common_name'] == 'w1'
assert request['uri_sans'] == ['spiffe://velox/worker/w1']
assert 'csr' in request and 'PRIVATE KEY' not in request['csr']
PY
san_output="$(openssl x509 -in "$RUNTIME_CERTS_DIR/worker.crt" -noout -ext subjectAltName |
    tr -d '[:space:]')"
[[ "$san_output" == *'URI:spiffe://velox/worker/w1'* ]] || fail 'certificate SPIFFE URI SAN is not exact'
pass 'local key + CSR signing OK; exact CN + SPIFFE URI SAN; private key was not sent to OpenBao'

# An attested runtime cache is the only allowed outage path. Remove the
# AppRole inputs to prove this mode is offline/read-only and does not contact
# OpenBao; provisioning and renewal remain strict modes.
rm -f "$SECRETS_DIR/role-id" "$SECRETS_DIR/secret-id"
env -i PATH="$PATH" HOME="$HOME" \
    VELOX_WORKER_SECRETS_DIR="$SECRETS_DIR" VELOX_WORKER_CERTS_DIR="$CERTS_DIR" \
    VELOX_WORKER_ID=w1 bash "$FETCH" --runtime-cache >/dev/null \
    || fail 'attested runtime cache failed during simulated outage'
pass 'attested runtime cache works without OpenBao/AppRole inputs'
printf 'role-id' > "$SECRETS_DIR/role-id"
printf 'valid-secret-id' > "$SECRETS_DIR/secret-id"

key_hash_1="$(sha256sum "$RUNTIME_CERTS_DIR/worker.key" | awk '{print $1}')"
# Valid cache is not re-signed.
env -i PATH="$PATH" HOME="$HOME" "${common_env[@]}" bash "$FETCH" --provision >/dev/null || fail 'cache-hit run failed'
key_hash_2="$(sha256sum "$RUNTIME_CERTS_DIR/worker.key" | awk '{print $1}')"
[[ "$key_hash_1" == "$key_hash_2" ]] || fail 'valid cache unexpectedly rotated'
pass 'valid mTLS cache is reused'

# Explicit renewal creates a fresh local key and a fresh CSR/certificate.
env -i PATH="$PATH" HOME="$HOME" "${common_env[@]}" bash "$FETCH" --renew >/dev/null || fail 'forced renewal failed'
key_hash_3="$(sha256sum "$RUNTIME_CERTS_DIR/worker.key" | awk '{print $1}')"
[[ "$key_hash_3" != "$key_hash_2" ]] || fail 'renewal did not generate a fresh local key'
pass 'forced renewal rotates the local key and certificate'

# A certificate for another worker must be rejected, preserving the valid bundle.
crt_hash="$(sha256sum "$RUNTIME_CERTS_DIR/worker.crt" | awk '{print $1}')"
key_hash="$(sha256sum "$RUNTIME_CERTS_DIR/worker.key" | awk '{print $1}')"
printf 'wrong-identity' > "$MODE_FILE"
if env -i PATH="$PATH" HOME="$HOME" "${common_env[@]}" bash "$FETCH" --renew >/dev/null 2>&1; then fail 'mismatched worker identity unexpectedly succeeded'; fi
[[ "$crt_hash" == "$(sha256sum "$RUNTIME_CERTS_DIR/worker.crt" | awk '{print $1}')" ]] || fail 'identity mismatch changed worker.crt'
[[ "$key_hash" == "$(sha256sum "$RUNTIME_CERTS_DIR/worker.key" | awk '{print $1}')" ]] || fail 'identity mismatch changed worker.key'
pass 'mismatched SPIFFE identity is rejected and previous bundle is preserved'

# A failed renewal must not destroy the previously valid bundle.
crt_hash="$(sha256sum "$RUNTIME_CERTS_DIR/worker.crt" | awk '{print $1}')"
key_hash="$(sha256sum "$RUNTIME_CERTS_DIR/worker.key" | awk '{print $1}')"
printf 'fail' > "$MODE_FILE"
if env -i PATH="$PATH" HOME="$HOME" "${common_env[@]}" bash "$FETCH" --renew >/dev/null 2>&1; then fail 'failed PKI signing unexpectedly succeeded'; fi
[[ "$crt_hash" == "$(sha256sum "$RUNTIME_CERTS_DIR/worker.crt" | awk '{print $1}')" ]] || fail 'failed renewal changed worker.crt'
[[ "$key_hash" == "$(sha256sum "$RUNTIME_CERTS_DIR/worker.key" | awk '{print $1}')" ]] || fail 'failed renewal changed worker.key'
pass 'failed renewal preserves the previous runtime bundle'

pass 'OK'
