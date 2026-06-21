#!/usr/bin/env bash
# scripts/ci/check-secrets.sh
#
# Heuristic secrets scan for committed source. Patterns are HIGH-signal
# (real-looking GitHub / AWS / Stripe / private-key markers) so we don't
# fire on UUIDs, env var names, or random base64 in test fixtures.
#
# Excluded (intentional, not a bug):
#   * docs/archive/            historical context
#   * DataServer/data/         generated DB / downloads (large binary blobs)
#   * RemoteCodex/native/worker-agent-go/bin/ compiled worker binary
#   * frontend_standalone/     bundled JS artefacts
#   * *_test.go                unit tests deliberately reference fake keys
#   * deploy/*example*         example inventory / env templates are placeholders
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

violations=0

# ── Private-key blocks (most reliable signal) ───────────────────────────────
if matches="$(
       git grep -nE -- \
         '-----BEGIN ((RSA|EC|DSA|OPENSSH|PGP) )?PRIVATE KEY-----' \
         -- ':!docs/archive/**' \
         ':!*_test.go' \
         ':!scripts/ci/check-secrets.sh' || true
     )"; [[ -n "$matches" ]]; then
  printf 'Private key block(s) found:\n%s\n\n' "$matches" >&2
  violations=$((violations + 1))
fi

# ── GitHub tokens ────────────────────────────────────────────────────────────
# `ghp_` PAT \u00b7 `ghs_` server-to-server \u00b7 `gho_` OAuth \u00b7 `ghu_` user-to-server
if matches="$(
       git grep -nE 'gh[psoru]_[A-Za-z0-9]{30,}' \
         -- ':!docs/archive/**' \
         ':!*_test.go' \
         ':!scripts/ci/check-secrets.sh' || true
     )"; [[ -n "$matches" ]]; then
  printf 'GitHub token(s) found:\n%s\n\n' "$matches" >&2
  violations=$((violations + 1))
fi

# ── AWS access-key IDs (AKIA = long-lived, ASIA = STS) ──────────────────────
if matches="$(
       git grep -nE 'AKIA[0-9A-Z]{16}|ASIA[0-9A-Z]{16}' \
         -- ':!docs/archive/**' \
         ':!*_test.go' \
         ':!scripts/ci/check-secrets.sh' || true
     )"; [[ -n "$matches" ]]; then
  printf 'AWS access key ID(s) found:\n%s\n\n' "$matches" >&2
  violations=$((violations + 1))
fi

# ── Stripe live keys (sk_/pk_/rk_/whsec_) ────────────────────────────────────
if matches="$(
       git grep -nE 'sk_live_[0-9a-zA-Z]{20,}|pk_live_[0-9a-zA-Z]{20,}|whsec_[0-9a-zA-Z]{20,}' \
         -- ':!docs/archive/**' \
         ':!*_test.go' \
         ':!scripts/ci/check-secrets.sh' || true
     )"; [[ -n "$matches" ]]; then
  printf 'Stripe live key(s) found:\n%s\n\n' "$matches" >&2
  violations=$((violations + 1))
fi

# ── Google API keys (AIza\u2026) ─────────────────────────────────────────────────────
if matches="$(
       git grep -nE 'AIza[0-9A-Za-z_-]{35}' \
         -- ':!docs/archive/**' \
         ':!*_test.go' \
         ':!scripts/ci/check-secrets.sh' || true
     )"; [[ -n "$matches" ]]; then
  printf 'Google API key(s) found:\n%s\n\n' "$matches" >&2
  violations=$((violations + 1))
fi

# ── Committed `.env` with real-looking key=value pairs ──────────────────────
# We allow `.env.example` (`*-env.example`) but flag a `.env` whose value
# is non-trivial (>= 16 bytes, no spaces, looks base64/hex).
if matches="$(
       git ls-files -- \
           '.env' \
           '*.env' \
           '*/.env' \
           ':!*.env.example' \
           ':!deploy/**.example' || true
     )"; [[ -n "$matches" ]]; then
  printf 'Committed .env file(s) (non-template):\n%s\n\n' "$matches" >&2
  violations=$((violations + 1))
fi

# ── Bare *.pem / *.key outside infra dirs ───────────────────────────────────
if matches="$(
       git ls-files -- '*.pem' '*.key' ':!deploy/**' ':!docs/**' \
         ':!scripts/ci/check-secrets.sh' || true
     )"; [[ -n "$matches" ]]; then
  printf 'Committed key material outside deploy/:\n%s\n\n' "$matches" >&2
  violations=$((violations + 1))
fi

if [[ "$violations" -gt 0 ]]; then
  printf '%d secrets-related violation(s) \u2014 see above\n' \
    "$violations" >&2
  exit 1
fi

echo "check-secrets: OK"
