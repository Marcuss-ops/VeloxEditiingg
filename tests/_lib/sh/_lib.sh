#!/usr/bin/env bash
# =============================================================================
# tests/_lib/sh/_lib.sh — orchestrator for the cross-test helper library.
# =============================================================================
# Source from any test script via:
#   source "$(dirname "$0")/../_lib/sh/_lib.sh"
#   # or, from arbitrary cwd:
#   source "$(cd "$(dirname "$0")"; cd ../_lib/sh && pwd)/_lib.sh"
#
# Each helper file is IDEMPOTENT (safe to source multiple times in the same
# shell). Sourcing _lib.sh is the recommended entry-point — sourcing the
# individual files directly is allowed but discouraged (increases drift).
#
# Helper inventory:
#   logging.sh              — _ts + log_debug/info/warn/error
#   pid-trap.sh             — lib_push_pid + lib_kill_all + lib_reset_children
#   ensure.sh               — ensure_dir / ensure_clean_tmpdir / ensure_command_available
#   check.sh                — check_file_readable / check_positive_int / check_hex_hash / verify_* helpers
#   retry.sh                — retry_with_backoff (sync + log_warn tail)
#   exitcode-aggregation.sh — aggregate_init / aggregate_record / aggregate_summary_and_exit
#   asset-bootstrap.sh      — bootstrap_workdir + write_stub_binary
# =============================================================================

_LIB_SH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# shellcheck source=tests/_lib/sh/logging.sh
source "${_LIB_SH_DIR}/logging.sh"
# shellcheck source=tests/_lib/sh/pid-trap.sh
source "${_LIB_SH_DIR}/pid-trap.sh"
# shellcheck source=tests/_lib/sh/ensure.sh
source "${_LIB_SH_DIR}/ensure.sh"
# shellcheck source=tests/_lib/sh/check.sh
source "${_LIB_SH_DIR}/check.sh"
# shellcheck source=tests/_lib/sh/retry.sh
source "${_LIB_SH_DIR}/retry.sh"
# shellcheck source=tests/_lib/sh/exitcode-aggregation.sh
source "${_LIB_SH_DIR}/exitcode-aggregation.sh"
# shellcheck source=tests/_lib/sh/asset-bootstrap.sh
source "${_LIB_SH_DIR}/asset-bootstrap.sh"
