#!/usr/bin/env bash
# deploy/runtime/openbao-fetch-worker-secrets.sh
# ─────────────────────────────────────────────────────────────────────────────
# Resolve the worker credential from OpenBao KV and obtain the worker mTLS leaf
# from OpenBao PKI without ever sending or storing the worker private key in
# OpenBao. The private key is generated locally on the worker, the CSR is sent
# to OpenBao, and only the signed certificate chain returns to this host.
#
# Runtime paths:
#   /etc/velox-worker/secrets/worker_credential  (0600, uid 10001)
#   /etc/velox-worker/certs/current/worker.crt  (0644, root:10001)
#   /etc/velox-worker/certs/current/worker.key  (0600, uid 10001, local only)
#   /etc/velox-worker/certs/current/ca.crt      (0644, root:10001)
#
# The current certificate/key pair is retained when it is valid beyond the
# renewal window. A deploy/provision/renewal run still requires a successful
# AppRole login and KV credential read; a normal reboot consumes the already
# materialized runtime cache and does not invoke this script.
#
# Config:
#   VELOX_OPENBAO_ADDR                 https://127.0.0.1:8200 (required for PKI)
#   VELOX_OPENBAO_ROLE_ID_FILE         default .../approle/role-id
#   VELOX_OPENBAO_SECRET_ID_FILE       default .../approle/secret-id
#   VELOX_OPENBAO_CA_FILE              OpenBao TLS CA (never -k)
#   VELOX_OPENBAO_PKI_MOUNT            default pki
#   VELOX_OPENBAO_PKI_ROLE             default worker-$VELOX_WORKER_ID
#   VELOX_OPENBAO_PKI_TTL              default 168h
#   VELOX_MTLS_RENEW_BEFORE_SECONDS    default 172800 (48 hours)
#   VELOX_MTLS_FORCE_RENEW             set to 1 to rotate immediately
#
# Flags (exactly one is required):
#   --provision       require OpenBao and materialize credential/PKI data
#   --renew           require OpenBao and force a new local key/certificate
#   --check           require OpenBao and verify remote/local coherence
#   --runtime-cache   use only already-attested runtime material during an
#                     explicitly declared temporary OpenBao outage; no network
#                     access, provisioning, or renewal is attempted
#
# Deploy/provision/renew/check fail closed when OpenBao is not configured.
# Manually copied files are never accepted as runtime cache material.

set -euo pipefail

SECRETS_DIR="${VELOX_WORKER_SECRETS_DIR:-/etc/velox-worker/secrets}"
CERTS_ROOT="${VELOX_WORKER_CERTS_DIR:-/etc/velox-worker/certs}"
CERTS_DIR="$CERTS_ROOT/current"
ROLE_ID_FILE="${VELOX_OPENBAO_ROLE_ID_FILE:-$SECRETS_DIR/approle/role-id}"
SECRET_ID_FILE="${VELOX_OPENBAO_SECRET_ID_FILE:-$SECRETS_DIR/approle/secret-id}"
IMAGE_UID="${IMAGE_UID:-10001}"
IMAGE_GID="${IMAGE_GID:-10001}"
PKI_MOUNT="${VELOX_OPENBAO_PKI_MOUNT:-pki}"
[[ "$PKI_MOUNT" == "pki" ]] || {
    printf '[openbao-fetch] FATAL: VELOX_OPENBAO_PKI_MOUNT must be the canonical pki mount\n' >&2
    exit 1
}
RENEW_BEFORE_SECONDS="${VELOX_MTLS_RENEW_BEFORE_SECONDS:-172800}"
CHECK=0
FORCE_RENEW="${VELOX_MTLS_FORCE_RENEW:-0}"
MODE=""

select_mode() {
    [[ -z "$MODE" ]] || {
        echo "exactly one operation mode is required" >&2
        exit 2
    }
    MODE="$1"
}

for arg in "$@"; do
    case "$arg" in
        --provision) select_mode provision ;;
        --runtime-cache) select_mode cache ;;
        --check) CHECK=1; select_mode check ;;
        --renew) FORCE_RENEW=1; select_mode renew ;;
        -h|--help) sed -n '2,47p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "unknown option: $arg" >&2; exit 2 ;;
    esac
done

[[ -n "$MODE" ]] || {
    echo "usage: $0 --provision | --renew | --check | --runtime-cache" >&2
    exit 2
}

log() { printf '[openbao-fetch] %s\n' "$*"; }
fail() { printf '[openbao-fetch] FATAL: %s\n' "$*" >&2; exit 1; }
cleanup() {
    if [[ -n "${LOCK_DIR:-}" ]]; then rmdir "$LOCK_DIR" 2>/dev/null || true; fi
    if [[ -n "${WORK_DIR:-}" ]]; then rm -rf "$WORK_DIR"; fi
}
trap cleanup EXIT

command -v openssl >/dev/null 2>&1 || fail "openssl not found on PATH"
command -v sha256sum >/dev/null 2>&1 || fail "sha256sum not found on PATH"

ADDR="${VELOX_OPENBAO_ADDR:-}"
ADDR="${ADDR%/}"
WORKER_ID="${VELOX_WORKER_ID:-}"
[[ "$WORKER_ID" =~ ^[A-Za-z0-9][A-Za-z0-9._:-]*$ ]] \
    || fail "VELOX_WORKER_ID mancante o non valido"
[[ "$RENEW_BEFORE_SECONDS" =~ ^[0-9]+$ ]] \
    || fail "VELOX_MTLS_RENEW_BEFORE_SECONDS deve essere un intero non negativo"

if [[ "$MODE" != "cache" ]]; then
    command -v curl >/dev/null 2>&1 || fail "curl not found on PATH"
    command -v jq >/dev/null 2>&1 || fail "jq not found on PATH"
    [[ -n "$ADDR" ]] || fail "VELOX_OPENBAO_ADDR obbligatorio per $MODE — nessun fallback a file locali"
    [[ -f "$ROLE_ID_FILE" && -s "$ROLE_ID_FILE" ]] || fail "role-id mancante: $ROLE_ID_FILE"
    [[ -f "$SECRET_ID_FILE" && -s "$SECRET_ID_FILE" ]] || fail "secret-id mancante: $SECRET_ID_FILE"
fi

if [[ "$MODE" == "cache" ]]; then
    CURL_TLS=()
elif [[ "$ADDR" == https://* ]]; then
    [[ -n "${VELOX_OPENBAO_CA_FILE:-}" && -s "$VELOX_OPENBAO_CA_FILE" ]] ||
        fail "VELOX_OPENBAO_CA_FILE mancante o vuoto — TLS verification obbligatoria"
    CURL_TLS=(--cacert "$VELOX_OPENBAO_CA_FILE")
elif [[ "$ADDR" == http://* && "${VELOX_OPENBAO_ALLOW_INSECURE_HTTP_TEST:-0}" == "1" ]]; then
    CURL_TLS=()
else
    fail "VELOX_OPENBAO_ADDR must use https:// (HTTP is test-only and requires VELOX_OPENBAO_ALLOW_INSECURE_HTTP_TEST=1)"
fi

# Cache mode is intentionally read-only and does not create a lock or runtime
# directories. It is an explicit outage procedure, never an implicit fallback.
# Both provenance markers are written only after OpenBao-authoritative material
# has been validated/materialized by this script.
KV_ROOT="velox/data/production/workers/$WORKER_ID"
CERT_FILE="$CERTS_DIR/worker.crt"
KEY_FILE="$CERTS_DIR/worker.key"
CA_FILE="$CERTS_DIR/ca.crt"
ISSUED_MARKER="$CERTS_DIR/.openbao-pki-issued"
CREDENTIAL_MARKER="$SECRETS_DIR/.openbao-credential-issued"

sha_file() { sha256sum "$1" 2>/dev/null | awk '{print $1}'; }

credential_cache_ready() {
    [[ -s "$SECRETS_DIR/worker_credential" && -s "$CREDENTIAL_MARKER" ]] || return 1
    [[ "$(sha_file "$SECRETS_DIR/worker_credential")" == "$(cat "$CREDENTIAL_MARKER")" ]]
}

login_token() {
    jq -n --arg r "$(cat "$ROLE_ID_FILE")" --arg s "$(cat "$SECRET_ID_FILE")" \
        '{role_id: $r, secret_id: $s}' |
        curl -fsS "${CURL_TLS[@]}" -X POST -H 'Content-Type: application/json' \
            --data-binary @- "$ADDR/v1/auth/approle/login" 2>/dev/null |
        jq -r '.auth.client_token // empty' 2>/dev/null
}

kv_read() {
    local path="$1" out code value
    out="$(mktemp)"
    code="$(curl -sS "${CURL_TLS[@]}" -H "X-Vault-Token: $TOKEN" \
        -o "$out" -w '%{http_code}' "$ADDR/v1/$KV_ROOT/$path" 2>/dev/null || echo 000)"
    if [[ "$code" == "404" ]]; then rm -f "$out"; return 3; fi
    if [[ "$code" != "200" ]]; then rm -f "$out"; return 1; fi
    value="$(jq -r '.data.data.value // empty' "$out" 2>/dev/null || true)"
    rm -f "$out"
    [[ -n "$value" ]] || return 1
    printf '%s' "$value"
}

write_atomic() {
    local file="$1" content="$2" mode="$3" owner="$4" parent tmp
    parent="$(dirname "$file")"
    mkdir -p "$parent"
    tmp="$(mktemp "$parent/.$(basename "$file").XXXXXX")"
    printf '%s' "$content" > "$tmp"
    chmod "$mode" "$tmp"
    if [[ $EUID -eq 0 ]]; then
        chown "$owner:$IMAGE_GID" "$tmp" || { rm -f "$tmp"; return 1; }
    fi
    mv -f "$tmp" "$file" || { rm -f "$tmp"; return 1; }
    if [[ $EUID -eq 0 ]]; then
        chown "$owner:$IMAGE_GID" "$file" || return 1
    fi
    [[ "$(stat -c '%a' "$file")" == "${mode#0}" ]] || return 1
    return 0
}

certificate_matches_key() {
    local cert="$1" key="$2" cert_pub key_pub
    cert_pub="$(openssl x509 -in "$cert" -pubkey -noout 2>/dev/null | openssl pkey -pubin -outform DER 2>/dev/null | sha256sum | awk '{print $1}')" || return 1
    key_pub="$(openssl pkey -in "$key" -pubout -outform DER 2>/dev/null | sha256sum | awk '{print $1}')" || return 1
    [[ -n "$cert_pub" && "$cert_pub" == "$key_pub" ]]
}

certificate_identity_matches_worker() {
    local cert="$1" expected_uri="spiffe://velox/worker/$WORKER_ID" cn
    cn="$(openssl x509 -in "$cert" -noout -subject -nameopt RFC2253 2>/dev/null |
        sed 's/^subject=//' | tr ',' '\n' | sed -n 's/^CN=//p' | head -n 1)" || return 1
    [[ "$cn" == "$WORKER_ID" ]] || return 1
    openssl x509 -in "$cert" -noout -ext subjectAltName 2>/dev/null |
        tr ',' '\n' | sed 's/^[[:space:]]*//' |
        grep -Fxq "URI:$expected_uri"
}

local_certificate_ready() {
    # A pre-existing/manual bundle is not accepted as an OpenBao cache hit.
    # The marker is written only after a successful PKI CSR enrollment and
    # binds the cache to the issued certificate fingerprint.
    [[ -s "$ISSUED_MARKER" && -s "$CERT_FILE" && -s "$KEY_FILE" && -s "$CA_FILE" ]] || return 1
    [[ "$(sha_file "$CERT_FILE")" == "$(cat "$ISSUED_MARKER")" ]] || return 1
    openssl x509 -in "$CERT_FILE" -noout -checkend "$RENEW_BEFORE_SECONDS" >/dev/null 2>&1 || return 1
    openssl verify -CAfile "$CA_FILE" "$CERT_FILE" >/dev/null 2>&1 || return 1
    certificate_matches_key "$CERT_FILE" "$KEY_FILE" || return 1
    certificate_identity_matches_worker "$CERT_FILE" || return 1
    return 0
}

if [[ "$MODE" == "cache" ]]; then
    credential_cache_ready || fail "credential cache assente o senza attestazione OpenBao"
    local_certificate_ready || fail "mTLS cache assente, manuale, non valida o scaduta"
    log "runtime-cache: OK — uso esclusivo di materiale già attestato da OpenBao durante outage"
    exit 0
fi

# Serialize provisioning/renewal for one host. mkdir is atomic and does not
# expose secret material. Cache mode never reaches this path.
LOCK_DIR="$CERTS_ROOT/.openbao-fetch.lock"
mkdir -p "$CERTS_ROOT"
if ! mkdir "$LOCK_DIR" 2>/dev/null; then
    fail "another OpenBao fetch/renewal is already running: $LOCK_DIR"
fi
chmod 0700 "$LOCK_DIR"

# ── 1. Authenticate and resolve the only KV value still needed ───────────────
log "login AppRole verso $ADDR ..."
TOKEN="$(login_token)" || fail "login AppRole fallito verso $ADDR"
[[ -n "$TOKEN" ]] || fail "login AppRole restituito senza token"

CRED="$(kv_read credential)" || fail "workers/$WORKER_ID/credential non leggibile da OpenBao"

if [[ "$CHECK" == "1" ]]; then
    credential_cache_ready || fail "credential locale priva di attestazione OpenBao"
    local_cred_sha="$(sha_file "$SECRETS_DIR/worker_credential")"
    remote_cred_sha="$(printf '%s' "$CRED" | sha256sum | awk '{print $1}')"
    [[ -n "$local_cred_sha" && "$local_cred_sha" == "$remote_cred_sha" ]] ||
        fail "credential locale non coerente con OpenBao"
    local_certificate_ready || fail "materiale mTLS locale mancante, non valido o prossimo alla scadenza"
    log "verify-openbao: OK — credential coerente; key privata locale e certificato mTLS validi"
    exit 0
fi

mkdir -p "$SECRETS_DIR" "$CERTS_ROOT"
if [[ $EUID -eq 0 ]]; then
    chown "root:$IMAGE_GID" "$SECRETS_DIR" "$CERTS_ROOT" || fail "cannot set runtime directory ownership"
    chmod 0750 "$SECRETS_DIR" "$CERTS_ROOT" || fail "cannot set runtime directory mode"
fi
materialize_credential() {
    write_atomic "$SECRETS_DIR/worker_credential" "$CRED" 0600 "$IMAGE_UID" ||
        fail "cannot materialize worker credential safely"
    write_atomic "$CREDENTIAL_MARKER" "$(sha_file "$SECRETS_DIR/worker_credential")" 0644 root ||
        fail "cannot materialize OpenBao credential provenance marker"
    log "scritto worker_credential (sha256 $(sha256sum "$SECRETS_DIR/worker_credential" | awk '{print substr($1,1,12)}')...)"
}

# Credential KV and PKI certificate are independent OpenBao authorities. Mark
# the credential as soon as its OpenBao value has been materialized, before
# the CSR path runs, so a missing PKI issuer cannot hide a successful KV
# migration or make the next diagnostic report the wrong failure.
materialize_credential

# A valid pair is a cache hit. OpenBao was still contacted above, so deploy
# and renewal cannot silently proceed with an unverified authority.
if [[ "$MODE" == "provision" && "$FORCE_RENEW" != "1" ]] && local_certificate_ready; then
    materialize_credential
    log "mTLS cache valida oltre la renewal window — nessuna nuova chiave/CSR necessaria"
    exit 0
fi

command -v flock >/dev/null 2>&1 || true
WORK_DIR="$(mktemp -d "$CERTS_ROOT/.mtls-rotation.XXXXXX")"
chmod 0700 "$WORK_DIR"
STAGED_KEY="$WORK_DIR/worker.key"
STAGED_CSR="$WORK_DIR/worker.csr"
STAGED_CERT="$WORK_DIR/worker.crt"
STAGED_CA="$WORK_DIR/ca.crt"
PKI_ROLE="${VELOX_OPENBAO_PKI_ROLE:-worker-$WORKER_ID}"
[[ "$PKI_ROLE" == "worker-$WORKER_ID" ]] ||
    fail "VELOX_OPENBAO_PKI_ROLE must be worker-$WORKER_ID; cross-worker role override is forbidden"
PKI_TTL="${VELOX_OPENBAO_PKI_TTL:-168h}"

# Generate the private key only on this worker. The CSR is the sole key-related
# value sent to OpenBao; worker.key never enters the request body or KV.
log "generazione locale della nuova private key e CSR per $WORKER_ID"
openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out "$STAGED_KEY" 2>/dev/null
chmod 0600 "$STAGED_KEY"
openssl req -new -sha256 -key "$STAGED_KEY" -subj "/CN=$WORKER_ID/O=Velox/OU=Worker" \
    -addext "subjectAltName=URI:spiffe://velox/worker/$WORKER_ID" \
    -out "$STAGED_CSR" 2>/dev/null
chmod 0600 "$STAGED_CSR"
CSR="$(cat "$STAGED_CSR")"

# OpenBao PKI CSR signing. The role/policy are per-worker and must reject a
# CSR whose identity does not equal this worker ID.
SPIFFE_URI="spiffe://velox/worker/$WORKER_ID"
REQUEST="$(jq -n --arg csr "$CSR" --arg cn "$WORKER_ID" --arg uri "$SPIFFE_URI" --arg ttl "$PKI_TTL" \
    '{csr:$csr, common_name:$cn, uri_sans:[$uri], ttl:$ttl, format:"pem"}')"
RESPONSE_FILE="$WORK_DIR/sign-response.json"
HTTP_CODE="$(curl -sS "${CURL_TLS[@]}" -o "$RESPONSE_FILE" -w '%{http_code}' \
    -X POST -H "X-Vault-Token: $TOKEN" -H 'Content-Type: application/json' \
    --data-binary "$REQUEST" "$ADDR/v1/$PKI_MOUNT/sign/$PKI_ROLE" 2>/dev/null || echo 000)"
[[ "$HTTP_CODE" == "200" ]] || fail "OpenBao PKI CSR signing fallito (HTTP $HTTP_CODE)"

CERT_VALUE="$(jq -r '.data.certificate // empty' "$RESPONSE_FILE" 2>/dev/null)"
CA_VALUE="$(jq -r '(.data.ca_chain // []) | if type == "array" then join("\n") else . end' "$RESPONSE_FILE" 2>/dev/null)"
if [[ -z "$CA_VALUE" ]]; then
    CA_VALUE="$(jq -r '.data.issuing_ca // empty' "$RESPONSE_FILE" 2>/dev/null)"
fi
[[ "$CERT_VALUE" == *"BEGIN CERTIFICATE"* ]] || fail "OpenBao PKI response senza certificate PEM"
[[ "$CA_VALUE" == *"BEGIN CERTIFICATE"* ]] || fail "OpenBao PKI response senza ca_chain/issuing_ca PEM"

printf '%s\n' "$CERT_VALUE" > "$STAGED_CERT"
printf '%s\n' "$CA_VALUE" > "$STAGED_CA"
chmod 0644 "$STAGED_CERT" "$STAGED_CA"
if [[ $EUID -eq 0 ]]; then
    chown "root:$IMAGE_GID" "$STAGED_CERT" "$STAGED_CA" || fail "cannot set staged certificate ownership"
    chown "$IMAGE_UID:$IMAGE_GID" "$STAGED_KEY" || fail "cannot set staged private key ownership"
fi
[[ "$(stat -c '%a' "$STAGED_KEY")" == 600 ]] || fail "staged private key mode is not 0600"
[[ "$(stat -c '%a' "$STAGED_CERT")" == 644 && "$(stat -c '%a' "$STAGED_CA")" == 644 ]] || fail "staged certificate modes are not 0644"

# Validate the complete staged bundle before touching live paths. This catches
# a wrong worker identity, malformed chain, or server response that does not
# correspond to the locally generated key.
openssl x509 -in "$STAGED_CERT" -noout -checkend 1 >/dev/null 2>&1 || fail "certificato PKI già scaduto o non valido"
openssl verify -CAfile "$STAGED_CA" "$STAGED_CERT" >/dev/null 2>&1 || fail "catena PKI non verificabile con ca_chain"
certificate_matches_key "$STAGED_CERT" "$STAGED_KEY" || fail "certificato PKI non corrisponde alla private key locale"
certificate_identity_matches_worker "$STAGED_CERT" ||
    fail "identità certificato diversa da VELOX_WORKER_ID=$WORKER_ID (CN/SPIFFE URI SAN mismatch)"

# Promote a complete versioned bundle, then atomically switch one symlink.
# The running container mounts `current`, so it can never observe a mixed
# worker.key/worker.crt/ca.crt set during renewal. The previous bundle remains
# available until the new bundle has been validated and selected.
BUNDLE_ROOT="$CERTS_ROOT/.bundles"
NEW_BUNDLE="$BUNDLE_ROOT/$(date +%s).$$"
CURRENT_LINK="$CERTS_ROOT/current"
SWITCH_LINK="$CERTS_ROOT/.current.$$"
mkdir -p "$BUNDLE_ROOT" "$NEW_BUNDLE"
for name in worker.key worker.crt ca.crt; do
    mv -f "$WORK_DIR/$name" "$NEW_BUNDLE/$name" || fail "staging bundle promotion failed for $name"
done
if ! write_atomic "$NEW_BUNDLE/.openbao-pki-issued" "$(sha_file "$NEW_BUNDLE/worker.crt")" 0644 root; then
    fail "materializzazione della provenienza PKI fallita"
fi
if [[ -e "$CURRENT_LINK" && ! -L "$CURRENT_LINK" ]]; then
    fail "$CURRENT_LINK exists as a directory/file; refusing non-atomic replacement"
fi
ln -s ".bundles/$(basename "$NEW_BUNDLE")" "$SWITCH_LINK" || fail "cannot prepare atomic certificate switch"
mv -Tf "$SWITCH_LINK" "$CURRENT_LINK" || { rm -f "$SWITCH_LINK"; fail "atomic certificate switch failed"; }
log "mTLS rinnovato da OpenBao PKI: private key generata localmente; bundle versionato selezionato con switch atomico"
