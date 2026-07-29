# =============================================================================
# tests/_lib/sh/exitcode-aggregation.sh — verdict tally + summary line.
# =============================================================================
# Each consumer script owns its own $PASS / $FAIL_COUNT / $CASE_VERDICTS
# globals (declared in the consumer scope, NOT here, to keep cross-script
# state contamination impossible). This file provides:
#   * aggregate_init                — reset the three globals (called from
#                                     consumer's main() before the matrix runs)
#   * aggregate_record <name> <v>   — append "name: v" to CASE_VERDICTS,
#                                     bump PASS or FAIL_COUNT
#   * aggregate_summary_and_exit    — print matrix summary, return 0 if PASS
#                                     count == total, 1 otherwise
# =============================================================================

# aggregate_init — reset all three globals.
aggregate_init() {
  PASS=0
  FAIL_COUNT=0
  CASE_VERDICTS=()
}

# aggregate_record <case_name> <verdict> — verdict must be PASS or FAIL.
aggregate_record() {
  local case_name="$1" verdict="$2"
  CASE_VERDICTS+=("$case_name: $verdict")
  if [[ "$verdict" == "PASS" ]]; then
    (( PASS++ ))
  else
    (( FAIL_COUNT++ ))
  fi
}

# aggregate_summary_and_exit — print summary line, return success iff no FAILs.
aggregate_summary_and_exit() {
  printf "\n==== Matrix summary ====\n"
  local v
  for v in "${CASE_VERDICTS[@]:-}"; do
    [[ -n "$v" ]] && printf "  %s\n" "$v"
  done
  printf "\nResult: %d PASS, %d FAIL\n" "$PASS" "$FAIL_COUNT"
  (( FAIL_COUNT == 0 ))
}
