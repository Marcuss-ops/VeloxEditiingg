#!/usr/bin/env bash
# Sourced catalog helpers for publishing_flow_smoke.sh.

publishing_fetch_inst_catalog() {
# ---- Step 1b: GET /api/v1/integrations/velox/destinations on InstaeditLogin
# Optional, mirrors the PRIVATE_UPLOADED-check pattern below: when
# INSTAEDIT_BASE_URL / INST_USER_TOKEN is unset, skip with a notice
# AND downgrade the step-2 cross-validation to a one-sided shape check
# (destination_id STARTSWITH "instaedit_") — a fully stricter invariant
# BOTH-URL+TURN-OFF would have us fail; partial-degrade is the better
# caller experience and is documented in the exit-code table above.
S_INST=""
S_INST_COUNT=""
INST_CATALOG_STATUS=""
if [[ -n "$INSTAEDIT_BASE_URL" && -n "$INST_USER_TOKEN" ]]; then
  INST_CATALOG_URL="${INSTAEDIT_BASE_URL}/api/v1/integrations/velox/destinations?workspace_id=${PUBLISHING_WORKSPACE_ID}"
  echo "--- GET ${INST_CATALOG_URL} ---"
  CURL_RC=0
  curl -sS -m 15 \
    -H "Authorization: Bearer ${INST_USER_TOKEN}" \
    -H "X-Request-ID: pub-smoke-${EPOCH}-catalog" \
    "${INST_CATALOG_URL}" \
    -D "$TMP_HDRS" -o "$TMP_BODY" 2>"$TMP_TRACE" || CURL_RC=$?
  if [[ $CURL_RC -ne 0 ]]; then
    echo "FATAL: curl could not reach ${INST_CATALOG_URL} (exit=${CURL_RC})" >&2
    exit 3
  fi
  INST_CATALOG_STATUS=$(awk 'NR==1 && $1 ~ /^[Hh][Tt][Tt][Pp]// {print $2; exit}' "$TMP_HDRS")
  INST_CATALOG_BODY=$(cat "$TMP_BODY")
  if [[ "${DEBUG_MODE}" == "1" ]]; then
    echo "DEBUG: /integrations/velox/destinations response:" >&2
    echo "$INST_CATALOG_BODY" | jq . >&2
  fi
  if [[ "$INST_CATALOG_STATUS" != "200" ]]; then
    echo "FATAL: GET ${INST_CATALOG_URL} returned HTTP ${INST_CATALOG_STATUS}" >&2
    echo "  Hint: 401 → USER-JWT missing/invalid (adminIdentityUserID(req)==0);" >&2
    echo "        403 → workspace owned by another user (ownership check);" >&2
    echo "        400 → workspace_id query param missing or non-positive." >&2
    echo "  Response body:" >&2
    echo "$INST_CATALOG_BODY" | sed 's/^/    /' >&2
    exit 4
  fi
  # Capture active+enabled rows into a newline-separated set S_inst.
  # We DO NOT include include_disabled=true — the cross-validation target
  # is "what can the sender actually pick right now?", and disabled rows
  # are not selectable from the perspective of an active can_post target.
  if ! S_INST=$(printf '%s' "$INST_CATALOG_BODY" | jq -er '
    (.destinations // [])
    | map(select(
        (.status // "active") == "active"
        and ((.external_destination_id // "") | length) > 0
      ))
    | map(.external_destination_id)
    | join("\n")
  ' 2>/dev/null); then
    echo "WARN: jq parse of /integrations/velox/destinations body failed; treating S_inst as empty" >&2
    S_INST=""
  fi
  # `wc -l` on empty string yields 0; strip CR defensively.
  S_INST=$(printf '%s' "$S_INST" | tr -d '\r')
  if [[ -z "$S_INST" ]]; then
    S_INST_COUNT="0"
  else
    S_INST_COUNT=$(printf '%s\n' "$S_INST" | grep -c '^' || true)
  fi
  echo "OK: InstaeditLogin catalog returned (active enabled rows): ${S_INST_COUNT}"
  if [[ "${DEBUG_MODE}" == "1" && -n "$S_INST" ]]; then
    echo "DEBUG: S_inst external_destination_ids:" >&2
    printf '%s\n' "$S_INST" | sed 's/^/    /' >&2
  fi
else
  echo "NOTICE: INSTAEDIT_BASE_URL or INSTAEDIT_VELOX_USER_TOKEN unset —" \
       "skipping step 1b (InstaeditLogin catalog GET). Cross-validation" \
       "in step 2 degrades to one-sided shape check (destination_id STARTSWITH" \
       "'instaedit_' per publishing_targets.go::veloxDestinationID); suffix" \
       "membership in S_inst is left as a WARN." >&2
fi
}

publishing_discover_target() {
# ---- POST /api/v1/publishing/targets -------------------------------------
echo "--- POST /api/v1/publishing/targets (workspace_id=${PUBLISHING_WORKSPACE_ID}, platform=${PLATFORM}${PLATFORM_ACCOUNT_ID:+, platform_account_id=${PLATFORM_ACCOUNT_ID}}) ---"
CURL_RC=0
curl -sS -m 15 -X POST \
  -H "Authorization: Bearer ${M2M_BEARER}" \
  -H "Content-Type: application/json" \
  -H "X-Request-ID: pub-smoke-${EPOCH}" \
  --data-raw "$TARGETS_PAYLOAD" \
  "${TARGETS_URL}" \
  -D "$TMP_HDRS" -o "$TMP_BODY" 2>"$TMP_TRACE" || CURL_RC=$?
if [[ $CURL_RC -ne 0 ]]; then
  echo "FATAL: curl could not reach ${TARGETS_URL} (exit=${CURL_RC})" >&2
  exit 3
fi
targets_status=$(awk 'NR==1 && $1 ~ /^[Hh][Tt][Tt][Pp]// {print $2; exit}' "$TMP_HDRS")
targets_body=$(cat "$TMP_BODY")
if [[ "${DEBUG_MODE}" == "1" ]]; then
  echo "DEBUG: /publishing/targets response:" >&2
  echo "$targets_body" | jq . >&2
fi
if [[ "$targets_status" != "200" ]]; then
  echo "FATAL: POST /publishing/targets returned HTTP ${targets_status}" >&2
  echo "  Response body:" >&2
  echo "$targets_body" | sed 's/^/    /' >&2
  exit 4
fi

# Pick the first target satisfying can_post=true AND
# capabilities.upload_video=true. jq -e sets rc=1 when no match +
# .selected/0/null is emitted (depends on jq version), so we explicitly
# default to empty and check.
CHOSEN=$(printf '%s' "$targets_body" | jq -er '
  .targets // [] | map(select(
    (.can_post // false) == true
    and (.capabilities.upload_video // false) == true
  )) | (if length > 0 then .[0] else empty end)
' 2>/dev/null) || CHOSEN=""
if [[ -z "$CHOSEN" || "$CHOSEN" == "null" ]]; then
  echo "FATAL: no target with can_post=true AND capabilities.upload_video=true" >&2
  echo "  workspace_id=${PUBLISHING_WORKSPACE_ID} platform=${PLATFORM}" >&2
  echo "  Inspect the targets array above to find a can_post=false / reauth row." >&2
  exit 5
fi
DESTINATION_ID=$(printf '%s' "$CHOSEN" | jq -er '.destination_id // empty') || true
EXTERNAL_DESTINATION_ID=$(printf '%s' "$CHOSEN" | jq -er '.external_destination_id // empty') || true
CHANNEL_ID=$(printf '%s' "$CHOSEN" | jq -r '.channel_id // "(unset)"')
CHANNEL_NAME=$(printf '%s' "$CHOSEN" | jq -r '.channel_name // "(unset)"')
if [[ -z "$DESTINATION_ID" || "$DESTINATION_ID" == "null" ]]; then
  echo "FATAL: chosen target has empty destination_id — Velox-side catalog is malformed" >&2
  exit 5
fi
echo "OK: target selected"
echo "  channel_id             : ${CHANNEL_ID}"
echo "  channel_name           : ${CHANNEL_NAME}"
echo "  destination_id (Velox) : ${DESTINATION_ID}"
echo "  external_destination_id (InstaEdit): ${EXTERNAL_DESTINATION_ID:-"(unset)"}"

# ---- Cross-validation: chosen target shape + full S_velox-is-subset-S_inst ---
# Per the user spec + the production mapping contract
# (publishing_targets.go::veloxDestinationID), a Velox-side destination_id
# MUST be of the canonical form 'instaedit_<external_destination_id>'.
# We assert two layers:
#   (1) chosen target destination_id STARTSWITH "instaedit_" AND when
#       S_inst is known its suffix is in S_inst (the InstaeditLogin catalog);
#   (2) full-iteration WARN: all S_velox destination_id suffixes vs all
#       S_inst rows, surfacing any drift across the entire response.
#       WARN not FATAL so a transient async-disabled target does not fail.
EXPECTED_DIR_PREFIX="instaedit_"
if [[ "${DESTINATION_ID:0:${#EXPECTED_DIR_PREFIX}}" != "$EXPECTED_DIR_PREFIX" ]]; then
  echo "FATAL: chosen target destination_id='${DESTINATION_ID}' does not start with '${EXPECTED_DIR_PREFIX}' -- Velox mapping contract drift" >&2
  echo "  Expected: instaedit_<external_destination_id> per publishing_targets.go::veloxDestinationID" >&2
  exit 10
fi
S_VELOX_SUFFIX="${DESTINATION_ID#${EXPECTED_DIR_PREFIX}}"
if [[ -n "$S_INST" ]]; then
  if ! printf '%s\n' "$S_INST" | grep -Fxq "$S_VELOX_SUFFIX"; then
    echo "FATAL: chosen destination_id='${DESTINATION_ID}' has suffix '${S_VELOX_SUFFIX}' which is NOT in the InstaeditLogin catalog for workspace_id=${PUBLISHING_WORKSPACE_ID}" >&2
    echo "  S_inst (active enabled rows from /integrations/velox/destinations):" >&2
    printf '%s\n' "$S_INST" | sed 's/^/    /' >&2
    echo "  Hint: Velox is reporting a destination_id not known by InstaEdit -- possible catalog drift or stale snapshot." >&2
    exit 10
  fi
  echo "OK: chosen-target cross-validation passed -- Velox destination_id suffix is in the InstaeditLogin catalog"
elif [[ -n "$INST_CATALOG_STATUS" ]]; then
  echo "WARN: step 1b returned 200 but S_inst was empty -- cross-validation downgrades to one-sided shape check (destination_id STARTSWITH instaedit_)" >&2
else
  echo "WARN: step 1b was skipped -- only enforced destination_id STARTSWITH instaedit_ (single-sided shape check)" >&2
fi
# Full-iteration WARN: surface any external_destination_ids present in Velox
# /publishing/targets but absent from the InstaeditLogin catalog across ALL
# targets. Catches drift even when the can_post filter hides the drifted row.
if [[ -n "$S_INST" ]]; then
  S_VELOX_FULL=$(printf '%s' "$targets_body" | jq -er '
    (.targets // [])
    | map(
        if (.external_destination_id // "") != "" then .external_destination_id
        elif (.destination_id // "") | startswith("instaedit_") then
          .destination_id | sub("^instaedit_"; "")
        else "" end
      )
    | map(select(. != ""))
    | unique
    | join("\n")
  ' 2>/dev/null || echo "")
  S_INST_SORTED=$(printf '%s\n' "$S_INST" | sort -u || true)
  S_VELOX_SORTED=$(printf '%s\n' "$S_VELOX_FULL" | sort -u || true)
  if [[ -n "$S_VELOX_SORTED" ]]; then
    S_DIFF=$(comm -23 <(printf '%s\n' "$S_VELOX_SORTED") <(printf '%s\n' "$S_INST_SORTED") || true)
    if [[ -n "$S_DIFF" ]]; then
      echo "WARN: drift detected -- these external_destination_ids appear in Velox /publishing/targets but NOT in InstaeditLogin catalog:" >&2
      printf '%s\n' "$S_DIFF" | sed 's/^/    /' >&2
      echo "  (WARN not FATAL -- operator should investigate catalog drift)" >&2
    else
      echo "OK: full S_velox-is-subset-S_inst invariant holds (all Velox destination_id suffixes are in S_inst)"
    fi
  fi
fi
}
