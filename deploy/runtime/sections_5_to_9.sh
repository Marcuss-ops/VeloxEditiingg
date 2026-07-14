# deploy/runtime/sections_5_to_9.sh
# ─────────────────────────────────────────────────────────────────────────────
# Pre-deploy file/config integrity checks split out of checklist-verify.sh
# as part of the per-category refactor (sections 5-9 / deploy / security /
# master_check / canary + lib/common.sh). These five sections run BEFORE
# the worker is deployed and verify the artifacts on disk: image pull,
# local digest, env file integrity, TLS certs, compose config.
#
# Sourced by the orchestrator AFTER lib/common.sh (which provides
# section_header, record, log, fail, vrb) and AFTER run_preconditions
# (which sets IMAGE, WORKER_ID, ENV_FILE, HEALTH_PORT as readonly globals).
#
# Each function tolerates partial failure: on any sub-step error it calls
# record ... FAIL ... ; return 0 to prevent errexit from terminating the
# orchestrator. This preserves the "full sweep" diagnostic contract.
# ─────────────────────────────────────────────────────────────────────────────

# ═════════════════════════════════════════════════════════════════════════════
# Section 5 — Pull via digest
# ═════════════════════════════════════════════════════════════════════════════
section_5_pull() {
    section_header 5 "Pull via digest"

    if [[ "$IMAGE" != *@sha256:* ]]; then
        record 5 "Pull via digest" FAIL \
            "image ref does not use @sha256: digest syntax (got: $IMAGE)"
        return 0
    fi

    vrb "docker pull $IMAGE"
    local pull_out
    if ! pull_out="$(docker pull "$IMAGE" 2>&1)"; then
        record 5 "Pull via digest" FAIL "docker pull exited non-zero (output below)"
        printf '%s\n' "$pull_out" | sed 's/^/       /' >&2
        return 0
    fi
    vrb "$pull_out"

    if printf '%s' "$pull_out" | grep -Eiq \
        'unauthorized|access denied|authentication required|manifest unknown'; then
        record 5 "Pull via digest" FAIL "pull output mentions auth/manifest error"
        return 0
    fi
    if printf '%s' "$pull_out" | grep -Eq \
        'Status: Downloaded newer image|Status: Image is up to date|Status: Downloaded'; then
        record 5 "Pull via digest" PASS "pull OK"
        return 0
    fi

    record 5 "Pull via digest" FAIL "pull returned unexpected status line"
}

# ═════════════════════════════════════════════════════════════════════════════
# Section 6 — Local digest + architecture
# ═════════════════════════════════════════════════════════════════════════════
section_6_digest() {
    section_header 6 "Local digest + architecture"

    local digests_json
    if ! digests_json="$(docker image inspect "$IMAGE" \
            --format '{{json .RepoDigests}}' 2>/dev/null)"; then
        record 6 "Local digest + architecture" FAIL \
            "image not present locally (inspect failed — did section 5 succeed?)"
        return 0
    fi

    if ! printf '%s' "$digests_json" | grep -q -- "$IMAGE"; then
        record 6 "Local digest + architecture" FAIL \
            "RepoDigests does not contain $IMAGE (got: $digests_json)"
        return 0
    fi

    local os_arch
    os_arch="$(docker image inspect "$IMAGE" \
        --format 'OS={{.Os}} ARCH={{.Architecture}}' 2>/dev/null || true)"
    vrb "$os_arch"

    if [[ "$os_arch" == "OS=linux ARCH=amd64" ]]; then
        record 6 "Local digest + architecture" PASS "$os_arch"
    else
        record 6 "Local digest + architecture" FAIL \
            "unexpected OS/ARCH (want OS=linux ARCH=amd64, got $os_arch)"
    fi
}

# ═════════════════════════════════════════════════════════════════════════════
# Section 7 — worker.env integrity
# ═════════════════════════════════════════════════════════════════════════════
section_7_worker_env() {
    section_header 7 "worker.env protected"

    local perms
    if ! perms="$(stat -c '%U:%G %a' "$ENV_FILE" 2>/dev/null)"; then
        record 7 "worker.env protected" FAIL "stat failed on $ENV_FILE"
        return 0
    fi
    if [[ "$perms" != "root:root 600" ]]; then
        record 7 "worker.env protected" FAIL \
            "want perms root:root 600 (got: $perms)"
        return 0
    fi

    local required=(
        VELOX_WORKER_ID
        VELOX_WORKER_NAME
        VELOX_WORKER_IMAGE
        VELOX_GRPC_MASTER_URL
        VELOX_WORKER_CREDENTIAL_FILE
        VELOX_GRPC_TLS_CERT_FILE
        VELOX_GRPC_TLS_KEY_FILE
        VELOX_GRPC_TLS_CA_FILE
        VELOX_WORK_DIR
        VELOX_MAX_ACTIVE_JOBS
        VELOX_HEALTH_PORT
    )
    local missing=()
    local v
    for v in "${required[@]}"; do
        # Fixed list of identifiers — safe to expand unquoted into the regex
        # here (no shell metacharacters in any var name).
        grep -qE "^${v}=" "$ENV_FILE" || missing+=("$v")
    done
    if [[ ${#missing[@]} -gt 0 ]]; then
        record 7 "worker.env protected" FAIL \
            "missing vars: ${missing[*]}"
        return 0
    fi

    local img_decl
    img_decl="$(grep -E '^VELOX_WORKER_IMAGE=' "$ENV_FILE" | head -1 || true)"
    if ! printf '%s' "$img_decl" | grep -q '@sha256:'; then
        record 7 "worker.env protected" FAIL \
            "VELOX_WORKER_IMAGE must use @sha256: digest (got: $img_decl)"
        return 0
    fi

    # Cover all GitHub token families in one alternation:
    #   ghp_  classic PAT    github_pat_  fine-grained PAT
    #   ghs_  GitHub App sec gho_          OAuth
    #   ghu_  user-to-server
    if grep -Eq 'gh[psou]_|github_pat_' "$ENV_FILE"; then
        record 7 "worker.env protected" FAIL \
            "PAT pattern (ghp_ or github_pat_) found in worker.env — git-history hazard"
        return 0
    fi
    if grep -Eiq 'allow_insecure' "$ENV_FILE"; then
        record 7 "worker.env protected" FAIL \
            "allow_insecure token found in worker.env — must live in worker_config.json only"
        return 0
    fi

    record 7 "worker.env protected" PASS "perms=${perms}; all required vars; digest-pinned"
}

# ═════════════════════════════════════════════════════════════════════════════
# Section 8 — TLS certs + credential
# ═════════════════════════════════════════════════════════════════════════════
section_8_certs() {
    section_header 8 "TLS certs + credential"

    local certs_dir="/etc/velox-worker/certs"
    local secrets_dir="/etc/velox-worker/secrets"

    local required=(
        "$certs_dir/worker.crt"
        "$certs_dir/worker.key"
        "$certs_dir/ca.crt"
        "$secrets_dir/worker_credential"
    )
    local missing=()
    local f
    for f in "${required[@]}"; do
        [[ -e "$f" ]] || missing+=("$f")
    done
    if [[ ${#missing[@]} -gt 0 ]]; then
        record 8 "TLS certs + credential" FAIL "missing files: ${missing[*]}"
        return 0
    fi

    local bad_perms=()
    local mode
    for f in "$certs_dir/worker.key" "$secrets_dir/worker_credential"; do
        mode="$(stat -c '%a' "$f")"
        [[ "$mode" == "600" ]] || bad_perms+=("$f=mode$mode (want 600)")
    done
    for f in "$certs_dir/worker.crt" "$certs_dir/ca.crt"; do
        mode="$(stat -c '%a' "$f")"
        # Both 600 (tightened) and 644 (default) are acceptable for read-only certs.
        [[ "$mode" == "600" || "$mode" == "644" ]] \
            || bad_perms+=("$f=mode$mode (want 600 or 644)")
    done
    if [[ ${#bad_perms[@]} -gt 0 ]]; then
        record 8 "TLS certs + credential" FAIL "bad perms: ${bad_perms[*]}"
        return 0
    fi

    local verify_out
    if ! verify_out="$(openssl verify -CAfile "$certs_dir/ca.crt" \
            "$certs_dir/worker.crt" 2>&1)"; then
        record 8 "TLS certs + credential" FAIL \
            "openssl verify exited non-zero: $verify_out"
        return 0
    fi
    vrb "$verify_out"
    if [[ "$verify_out" != "worker.crt: OK" ]]; then
        record 8 "TLS certs + credential" FAIL "verify output: $verify_out"
        return 0
    fi

    local not_after=""
    # `if ! … ; then … return 0 ; fi` explicitly disables errexit AND pipefail
    # propagation for this command substitution. Without it, an openssl or
    # sed failure terminates the verifier before `record` can emit FAIL.
    if ! not_after="$(openssl x509 -in "$certs_dir/worker.crt" \
            -noout -enddate 2>/dev/null | sed 's/^notAfter=//')"; then
        record 8 "TLS certs + credential" FAIL \
            "openssl x509 -enddate failed (readable cert? permissions?)"
        return 0
    fi
    vrb "notAfter=$not_after"
    local not_after_epoch now_epoch
    if ! not_after_epoch="$(date -d "$not_after" +%s 2>/dev/null)"; then
        record 8 "TLS certs + credential" FAIL \
            "could not parse notAfter ($not_after) — check locale"
        return 0
    fi
    now_epoch="$(date +%s)"
    if (( not_after_epoch <= now_epoch )); then
        record 8 "TLS certs + credential" FAIL \
            "worker.crt is EXPIRED (notAfter=$not_after)"
        return 0
    fi

    # RW-PROD-001 A9: cert CN MUST equal WORKER_ID (identity binding).
    # Without this assertion a stolen worker.key could impersonate any
    # worker_id, defeating the registry's CN-based filter.
    local cert_cn=""
    if ! cert_cn="$(openssl x509 -in "$certs_dir/worker.crt" \
            -noout -subject 2>/dev/null \
            | sed -n 's/.*CN *= *\([^,/]*\).*/\1/p' | tr -d ' ')"; then
        record 8 "TLS certs + credential" FAIL \
            "openssl x509 -subject failed"
        return 0
    fi
    if [[ -z "$cert_cn" ]]; then
        record 8 "TLS certs + credential" FAIL \
            "could not extract CN from worker.crt subject"
        return 0
    fi
    if [[ "$cert_cn" != "$WORKER_ID" ]]; then
        record 8 "TLS certs + credential" FAIL \
            "cert CN=${cert_cn} does NOT match WORKER_ID=${WORKER_ID} (RW-PROD-001 A9 binding)"
        return 0
    fi

    vrb "cert_cn=${cert_cn} matches WORKER_ID"
    record 8 "TLS certs + credential" PASS \
        "chain OK; notAfter=${not_after}; CN=${cert_cn} matches WORKER_ID"
}

# ═════════════════════════════════════════════════════════════════════════════
# Section 9 — Compose config
# ═════════════════════════════════════════════════════════════════════════════
section_9_compose() {
    section_header 9 "Compose config"

    local compose_dir="/opt/velox-worker"
    local compose_yml="$compose_dir/compose.yml"
    if [[ ! -r "$compose_yml" ]]; then
        record 9 "Compose config" FAIL \
            "$compose_yml not readable — has prepare-host.sh run at least once?"
        return 0
    fi

    # compose.yml uses ${VELOX_*}-style substitutions; source env_file in a
    # subshell so the exports do not leak back into the parent verifier env.
    local cfg_json
    if ! cfg_json="$(
        set -a
        # shellcheck disable=SC1090
        source "$ENV_FILE"
        set +a
        docker compose -p "velox-verify-${WORKER_ID}" \
            -f "$compose_yml" config --format json 2>/dev/null
    )"; then
        record 9 "Compose config" FAIL \
            "docker compose config exited non-zero (env vars / yaml may be invalid)"
        return 0
    fi

    local svc="velox-worker"
    local svc_json
    svc_json="$(printf '%s' "$cfg_json" | jq -r --arg s "$svc" '.services[$s]')"
    if [[ -z "$svc_json" || "$svc_json" == "null" ]]; then
        record 9 "Compose config" FAIL "service '$svc' not present in rendered config"
        return 0
    fi

    local failures=()
    local img
    img="$(printf '%s' "$svc_json" | jq -r '.image // ""')"
    [[ "$img" == *@sha256:* ]] || failures+=("image uses digest=false (got: $img)")

    local nname
    nname="$(printf '%s' "$svc_json" | jq -r '.container_name // ""')"
    [[ "$nname" == "velox-worker-$WORKER_ID" ]] \
        || failures+=("container_name='$nname' (want velox-worker-$WORKER_ID)")

    [[ "$(printf '%s' "$svc_json" | jq -r '.read_only // false')" == "true" ]] \
        || failures+=("read_only != true")

    if ! printf '%s' "$svc_json" | jq -r '.cap_drop // [] | .[]' 2>/dev/null | grep -qx 'ALL'; then
        failures+=("cap_drop missing ALL")
    fi
    if ! printf '%s' "$svc_json" | jq -r '.security_opt // [] | .[]' 2>/dev/null | grep -qx 'no-new-privileges:true'; then
        failures+=("security_opt missing no-new-privileges:true")
    fi

    local mem_limit
    mem_limit="$(printf '%s' "$svc_json" | jq -r '.mem_limit // ""')"
    [[ -n "$mem_limit" ]] || failures+=("mem_limit unset")

    local vols ro_mounts
    vols="$(printf '%s' "$svc_json" | jq -r '.volumes // [] | .[]' 2>/dev/null)"
    # Compose renders volume strings as "src:dst[:mode]" with `:ro` (when
    # set) exactly at the end of the string. The `,` alternation in `:ro(,|$)`
    # is dead — composite compose output never contains a `,)`.
    ro_mounts="$(printf '%s\n' "$vols" | grep -E ':ro$' || true)"
    if ! printf '%s' "$ro_mounts" | grep -q '/etc/velox-worker/certs'; then
        failures+=("certs mount not present with :ro")
    fi
    if ! printf '%s' "$ro_mounts" | grep -q '/etc/velox-worker/secrets'; then
        failures+=("secrets mount not present with :ro")
    fi

    # Compose renders healthcheck.test as a JSON array (e.g.
    # ["CMD","/bin/sh","-c","curl … /health/ready || exit 1"]). Stream each
    # element so the substring check is robust to the array form.
    if ! printf '%s' "$svc_json" \
            | jq -r '.healthcheck.test // [] | .[] | tostring' \
            2>/dev/null | grep -qF '/health/ready'; then
        failures+=("healthcheck does not probe /health/ready")
    fi

    if [[ ${#failures[@]} -gt 0 ]]; then
        record 9 "Compose config" FAIL "violations: ${failures[*]}"
        return 0
    fi

    record 9 "Compose config" PASS \
        "image=${img}; ro mounts verified; caps dropped; readiness on /health/ready"
}
