#!/usr/bin/env python3
# =============================================================================
# scripts/cert/executor-matrix.py
# =============================================================================
# Phase 3 of the 100% Velox certification plan (cap. 4 — Tutti gli executor).
#
# Walks the worker-agent's executor registry (the canonical source of
# truth for which executors a worker can run), and for EACH registered
# `(executor_id@version)` pair runs the 9-case matrix from the plan:
#
#   case_id             │ expectation (PASS = system refused correctly)
#   ────────────────────┼─────────────────────────────────────────────
#   valid_spec          │ status=SUCCEEDED, artifact READY
#   nonexistent_exe     │ REJECTED  (executor not registered)
#   unsupported_ver     │ REJECTED  (version mismatch)
#   missing_payload     │ FAILED    retryable=false  error=payload_missing
#   corrupt_asset       │ FAILED    retryable=false  error=asset_corrupt;
#                       │           artifact NEVER reaches READY
#   ffmpeg_fail         │ FAILED or retry-per-policy (retryable=true)
#   timeout             │ Attempt status=TIMED_OUT  retryable=true
#   cancel              │ Task + Attempt status=CANCELLED
#   no_output           │ execution completes WITHOUT writing artifact;
#                       │           artifact.status != READY; Job never SUCCEEDED
#
# Subcommands (default = all three steps in sequence):
#   --generate     walk Go source registry (ripgrep over MustRegister),
#                  emit `executor-matrix.json` + `executor-matrix.csv`
#   --run          execute each matrix row against the in-process Python
#                  stub executor + record actual_outcome, em...">