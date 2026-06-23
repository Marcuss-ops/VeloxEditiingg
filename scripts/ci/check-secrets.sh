#!/usr/bin/env bash
# scripts/ci/check-secrets.sh
#
# Heuristic secrets + infrastructure-identifier scan for committed source.
# Patterns are HIGH-signal (real-looking GitHub / AWS / Stripe / Google OAuth
# / private-key markers, infrastructure FQDNs, private IPv4) so we don't
# fire on UUIDs, env var names, or random base64 in test fixtures.
#
# Excluded (intentional):
#   * docs/archive/            historical context
#   * RemoteCodex/native/worker-agent-go/bin/ compiled worker binary
#   * frontend_standalone/     bundled JS artefacts
#   * *_test.go                unit tests deliberately reference fake keys
#   * deploy/*example*         example inventory / env templates are placeholders
#
# PR-5 (P0 security closure): the previous exclusion `DataServer/data/` from
# the secrets regexes has been REMOVED. That exclusion was a bug: the
# directory contains OAuth client_secret JSON files that MUST be scanned.
# Real OAuth credentials under DataServer/data/{youtube,drive}/credentials/
# are gitignored via the .gitignore entries that match the real file names
# (`credentials.json`), so the regex will NOT fire in CI on a clean tree —
# it WILL fire if an operator accidentally drops a freshly-generated
# client_secret.json into the tree without renaming it. Spot-check on
# local-disk files (git check-ignore) is a separate opt-in mode (see
# --include-untracked below).
#
# Exit codes:
#   0   no committed secret-shaped / identifier-shaped strings found
#   1   at least one violation (printed to stderr, with file:line refs)
#   2   usage error (missing arg, etc.)
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

# Default scope: git-tracked source only (CI mode).
# --include-untracked: also scan .gitignore-but-checked-into-active-checkout
# files (operator mode, off by default — fires loudly on local dev creds).
INCLUDE_UNTRACKED=0
if [[ "${1:-}" == "--include-untracked" ]]; then
    INCLUDE_UNTRACKED=1
fi

violations=0

# Helper: print a violation block + increment counter.
report_match() {
    local label="$1"; shift
    local matches="$1"; shift
    if [[ -n "$matches" ]]; then
        printf '%s found:\n%s\n\n' "$label" "$matches" >&2
        violations=$((violations + 1))
    fi
}

# Common git-grep prefix: scan everything that's TRACKED today.
GIT_GREP_BASE=(
    ':!docs/archive/**'
    ':!*_test.go'
    ':!scripts/ci/check-secrets.sh'
    ':!docs/SECURITY_RUNBOOK.md'
)

# ── Private-key blocks (most reliable signal) ───────────────────────────────
m="$(git grep -nE '-----BEGIN ((RSA|EC|DSA|OPENSSH|PGP) )?PRIVATE KEY-----' -- "${GIT_GREP_BASE[@]}" || true)"
report_match "Private key block(s)" "$m"

# ── GitHub tokens ────────────────────────────────────────────────────────────
# `ghp_` PAT · `ghs_` server-to-server · `gho_` OAuth · `ghu_` user-to-server
m="$(git grep -nE 'gh[psoru]_[A-Za-z0-9]{30,}' -- "${GIT_GREP_BASE[@]}" || true)"
report_match "GitHub token(s)" "$m"

# ── AWS access-key IDs (AKIA = long-lived, ASIA = STS) ──────────────────────
m="$(git grep -nE 'AKIA[0-9A-Z]{16}|ASIA[0-9A-Z]{16}' -- "${GIT_GREP_BASE[@]}" || true)"
report_match "AWS access key ID(s)" "$m"

# ── Stripe live keys (sk_/pk_/rk_/whsec_) ────────────────────────────────────
m="$(git grep -nE 'sk_live_[0-9a-zA-Z]{20,}|pk_live_[0-9a-zA-Z]{20,}|whsec_[0-9a-zA-Z]{20,}' -- "${GIT_GREP_BASE[@]}" || true)"
report_match "Stripe live key(s)" "$m"

# ── Google API keys (AIza…) ─────────────────────────────────────────────────
m="$(git grep -nE 'AIza[0-9A-Za-z_-]{35}' -- "${GIT_GREP_BASE[@]}" || true)"
report_match "Google API key(s)" "$m"

# ── Google OAuth client_secret (GOCSPX-) ────────────────────────────────────
# OAuth Web + Desktop client_secrets issued by Google Cloud Console since
# 2021 use the GOCSPX- prefix for the secret field in client_secret.json.
# PR-5 must catch any future leak of these via committed JSON.
m="$(git grep -nE 'GOCSPX-[A-Za-z0-9_-]{20,}' -- "${GIT_GREP_BASE[@]}" || true)"
report_match "Google OAuth client_secret (GOCSPX-)" "$m"

# ── Google OAuth client_id (apps.googleusercontent.com) ─────────────────────
# Real client_ids look like `712843578597-9b2cnrs...apps.googleusercontent.com`.
# We flag any occurrence outside test fixtures and the SECURITY_RUNBOOK
# (which uses literal patterns for training).
m="$(git grep -nE '\b[0-9]{6,}-[A-Za-z0-9_-]{20,}\.apps\.googleusercontent\.com\b' -- "${GIT_GREP_BASE[@]}" || true)"
report_match "Google OAuth client_id (apps.googleusercontent.com)" "$m"

# ── Tailscale tailnet identifiers (vps-XXXX.tailYYY.ts.net) ────────────────
# Tailscale assigns every device a hostname like `vps-334f342f.tail41558e.ts.net`.
# Operators occasionally commit these.
m="$(git grep -nE '\b[a-z][a-z0-9-]*\.tail[a-z0-9]{4,}\.ts\.net\b' -- "${GIT_GREP_BASE[@]}" || true)"
report_match "Tailscale tailnet hostname (*.ts.net)" "$m"

# ── DuckDNS dynamic-DNS hostnames ──────────────────────────────────────────
m="$(git grep -nE '\b[a-z][a-z0-9-]*\.duckdns\.org\b' -- "${GIT_GREP_BASE[@]}" || true)"
report_match "DuckDNS hostname (*.duckdns.org)" "$m"

# ── Private IPv4 in committed source (RFC 1918 + loopback) ──────────────────
# 10.0.0.0/8 · 172.16.0.0/12 · 192.168.0.0/16 · 127.0.0.0/8
# We only flag these in obvious address contexts (URLs, env values, YAML).
# Bare `127.0.0.1` in test fixtures is excluded by the test-file glob above.
m="$(git grep -nE '\b(https?://|tcp://|VELOX_[A-Z_]+=)(10\.[0-9]{1,3}|192\.168\.[0-9]{1,3}|172\.(1[6-9]|2[0-9]|3[01])\.[0-9]{1,3}|127\.[0-9]{1,3})\.[0-9]{1,3}\b' -- "${GIT_GREP_BASE[@]}" || true)"
report_match "Private IPv4 in URL/env value" "$m"

# ── Project IDs (channel-identifier leak: youtube-uploader-safe, …) ───────
# Operators may have committed identifiable GCP project names. PR-5 lists
# the known historical names; new leaks are added in subsequent passes.
m="$(git grep -nE '\b(youtube-uploader-safe|drive-uploading-[0-9]{3,}|veloxmanager)\b' -- "${GIT_GREP_BASE[@]}" || true)"
report_match "GCP project_id / channel identifier" "$m"

# ── Committed .env with real-looking key=value pairs ────────────────────────
# .env.example stays allowed; .env never should.
m="$(git ls-files -- '.env' '*.env' '*/.env' ':!*.env.example' ':!deploy/**.example' || true)"
report_match "Committed .env file(s) (non-template)" "$m"

# ── Bare *.pem / *.key outside deploy/ ─────────────────────────────────────
m="$(git ls-files -- '*.pem' '*.key' ':!deploy/**' ':!docs/**' ':!scripts/ci/check-secrets.sh' || true)"
report_match "Committed key material outside deploy/" "$m"

# ── Bare *.crt outside TLS-tracked paths ────────────────────────────────────
# Caught separately because .crt is an obvious leak candidate but is also
# used inside container-build contexts (debian apt lists) — we allow
# cached apt lists under /var/cache but flag root-level .crt files.
m="$(git grep -lE '\b-----BEGIN CERTIFICATE-----\b' -- "${GIT_GREP_BASE[@]}" || true)"
report_match "Inline PEM-encoded certificate block(s)" "$m"

# ── Operator mode: --include-untracked (local-disk scan) ───────────────────
# When called with --include-untracked, ALSO scan .gitignore'd files that
# exist on the local checkout. This catches the case where an operator
# dropped a real credentials.json into DataServer/data/{youtube,drive}/
# (gitignored, so tree is clean) and wants to verify the regex catches IT
# before committing any helper that processes the file.
if (( INCLUDE_UNTRACKED )); then
    # Walk the two known credential paths. If you add new ones, list them.
    for path in \
        DataServer/data/youtube/credentials/credentials.json \
        DataServer/data/drive/credentials/credentials.json; do
        if [[ -r "$path" ]]; then
            if matches="$(grep -nE 'GOCSPX-[A-Za-z0-9_-]{20,}|\b[0-9]{6,}-[A-Za-z0-9_-]{20,}\.apps\.googleusercontent\.com\b|\b[a-z][a-z0-9-]*\.tail[a-z0-9]{4,}\.ts\.net\b|\b[a-z][a-z0-9-]*\.duckdns\.org\b|\b(youtube-uploader-safe|drive-uploading-[0-9]{3,}|veloxmanager)\b' "$path" 2>/dev/null)"; then
                report_match "LOCAL UNTRACKED in $path" "$matches"
            fi
        fi
    done
fi

if (( violations > 0 )); then
    printf '%d security violation(s) — see above\n' "$violations" >&2
    if (( !INCLUDE_UNTRACKED )); then
        printf 'hint: run with --include-untracked to scan .gitignore-d files on this checkout\n' >&2
    fi
    exit 1
fi

printf 'check-secrets: OK\n'
