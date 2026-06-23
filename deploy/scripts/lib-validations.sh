#!/usr/bin/env bash
# deploy/scripts/lib-validations.sh
#
# Shared validation helpers reused by Velox deploy scripts to prevent drift
# between master and worker config validation. Both callers MUST accumulate
# errors (no fail-fast on first) so the operator sees ALL problems in one
# pass — fixing one and re-running only to discover the next is the worst UX.
#
# Imported from:
#   - deploy/validate-master-env.sh       (config-file driven, env validator)
#   - deploy/scripts/apply-local-worker-config.sh (CLI-arg driven, worker)
#
# API (function_name → return semantics):
#   is_https_url <url>
#     0 → valid https://... (DNS-style OR IPv6 IP-literal bracketed as [::1])
#     1 → valid http://...   (warning-eligible, NOT a hard fail)
#     2 → malformed / empty (e.g. ftp://, unterminated IPv6 bracket, non-hex
#                          inside [..], bare IPv4 with brackets mismatched)
#   is_port <port>
#     0 → 1..65535 numeric
#     1 → empty / non-numeric / out of range
#   worker_id_shape <id>
#     0 → matches canonical production shape (1..63 chars, [A-Za-z0-9._-])
#     1 → empty, '*', or otherwise mis-shaped
# Mirrors DataServer/internal/config/workers_validator.go so a future
# server-side change requires a one-line update here too.

# Don't `set -e` inside this lib; the helpers are called from scripts that
# either already enable errexit (and want to capture non-zero returns) or
# have errexit off (apply-local-worker-config.sh).

# ─── log helpers ───────────────────────────────────────────────────────────
log()  { printf '[lib-validations] %s\n' "$*" >&2; }
warn() { printf '[lib-validations][WARN] %s\n' "$*" >&2; }
die()  { printf '[lib-validations][FAIL] %s\n' "$*" >&2; exit "${2:-1}"; }

# ─── URL ────────────────────────────────────────────────────────────────────
is_https_url() {
    local url="${1:-}"
    [[ -z "$url" ]] && return 2
    # Scheme is case-insensitive per RFC 3986 §3.1; we treat lower-case as
    # canonical. Allow optional port + optional path.
    # Host is either a DNS-style hostname ([A-Za-z0-9._-]+, which also
    # admits bare IPv4 like 192.168.1.1) OR an IPv6 IP-literal bracketed
    # as `[<hex-or-colon>]+` per RFC 3986 §3.2.2 — e.g. `https://[::1]:9000`.
    if [[ "$url" =~ ^https://(\[[0-9a-fA-F:]+\]|[A-Za-z0-9._-]+)(:[0-9]+)?(/.*)?$ ]]; then
        return 0
    fi
    if [[ "$url" =~ ^http://(\[[0-9a-fA-F:]+\]|[A-Za-z0-9._-]+)(:[0-9]+)?(/.*)?$ ]]; then
        return 1   # well-formed but http-only (warning-eligible)
    fi
    return 2       # malformed
}

# ─── Port ───────────────────────────────────────────────────────────────────
is_port() {
    local port="${1:-}"
    [[ "$port" =~ ^[0-9]+$ ]] || return 1
    (( port >= 1 && port <= 65535 )) || return 1
}

# ─── Worker ID shape ────────────────────────────────────────────────────────
# Single source of truth for the production worker-id canonical shape. The
# server validates the same range via ValidateProductionWorkers; both
# sides MUST stay in sync on the regex + length cap.
worker_id_shape() {
    local id="${1:-}"
    [[ -z "$id" ]]               && return 1
    [[ "$id" == "*" ]]           && return 1
    [[ "$id" =~ ^[A-Za-z0-9._-]{1,63}$ ]] || return 1
}
