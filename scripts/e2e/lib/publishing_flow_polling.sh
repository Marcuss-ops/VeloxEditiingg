#!/usr/bin/env bash
# Sourced polling helpers for publishing_flow_smoke.sh.

poll_job() {
# ---- polling loop ---------------------------------------------------------
echo "--- polling GET /api/v1/jobs/${JOB_ID} (timeout=${POLL_TIMEOUT}s) ---"
elapsed=0
sleep_s=1
last_status=""
last_body=""

while (( elapsed < POLL_TIMEOUT )); do
  sleep "$sleep_s"
  elapsed=$((elapsed + sleep_s))
  sleep_s=$(( sleep_s * 2 ))
  if (( sleep_s > 16 )); then sleep_s=16; fi

  GET_RC=0
  curl -sS -m 10 \
    -H "Authorization: Bearer ${M2M_BEARER}" \
    "${JOBS_URL}/${JOB_ID}" \
    -D "$TMP_HDRS" -o "$TMP_BODY" 2>"$TMP_TRACE" || GET_RC=$?
  if [[ $GET_RC -ne 0 ]]; then
    echo "FATAL: curl could not reach ${JOBS_URL}/${JOB_ID} during poll (exit=${GET_RC})" >&2
    exit 3
  fi
  get_status=$(awk 'NR==1 && $1 ~ /^[Hh][Tt][Tt][Pp]// {print $2; exit}' "$TMP_HDRS")
  get_body=$(cat "$TMP_BODY")
  get_status_value=$(printf '%s' "$get_body" | jq -er '.status // empty') || true
  if [[ -n "$get_status_value" ]]; then
    last_status="$get_status_value"
    last_body="$get_body"
  fi
  if [[ "${DEBUG_MODE}" == "1" ]]; then
    echo "DEBUG: poll t=${elapsed}s status=${get_status_value:-(none)} (HTTP ${get_status})" >&2
  fi
  case "$get_status_value" in
    SUCCEEDED)
      echo "OK: terminal state SUCCEEDED after ${elapsed}s"
      break
      ;;
    FAILED|CANCELLED)
      echo "FATAL: terminal state ${get_status_value} after ${elapsed}s" >&2
      echo "  Last response body:" >&2
      echo "$get_body" | sed 's/^/    /' >&2
      exit 7
      ;;
  esac
  if [[ "$get_status" != "200" ]]; then
    echo "FATAL: GET /api/v1/jobs/${JOB_ID} returned HTTP ${get_status}" >&2
    echo "  Response body:" >&2
    echo "$get_body" | sed 's/^/    /' >&2
    exit 4
  fi
done

if [[ "$last_status" != "SUCCEEDED" ]]; then
  echo "FATAL: polling exhausted after ${POLL_TIMEOUT}s without reaching terminal state" >&2
  echo "  Last observed status: ${last_status:-none}" >&2
  echo "  Hint: raise PUBLISHING_POLL_TIMEOUT_S or check master log." >&2
  exit 8
fi
}

discover_external_delivery_id() {
# ---- best-effort: discover external_delivery_id (remote_id) --------------
# Operator-facing honesty: this smoke does NOT depend on a specific
# Velox enumeration endpoint. We mine the polled job body for any of the
# well-known field names a future API version might use; if none is
# present we log a notice and continue.
EXTERNAL_DELIVERY_ID=""
for field in external_delivery_id social_delivery_id remote_id delivery_id remoteId socialDeliveryId; do
  EXTERNAL_DELIVERY_ID=$(printf '%s' "$last_body" | jq -er --arg f "$field" '
    # Look in: top-level; .deliveries[].<field>; .job_deliveries[].<field>;
    # we use `paths` to scan the whole tree cheaply.
    [.. | objects | select(has($f)) | getpath([$f])] | (map(select(. != null and . != ""))[0] // empty)
  ' 2>/dev/null) || EXTERNAL_DELIVERY_ID=""
  if [[ -n "$EXTERNAL_DELIVERY_ID" && "$EXTERNAL_DELIVERY_ID" != "null" ]]; then
    echo "OK: external_delivery_id discovered via field '$field' : ${EXTERNAL_DELIVERY_ID}"
    break
  fi
done
}

poll_private_delivery() {
# ---- PRIVATE_UPLOADED verification on InstaeditLogin side -----------------
if [[ -n "$INSTAEDIT_BASE_URL" ]]; then
  if [[ -z "$EXTERNAL_DELIVERY_ID" ]]; then
    echo "NOTICE: INSTAEDIT_BASE_URL is set but external_delivery_id cannot be" \
         "discovered from the polled Velox job body — skipping PRIVATE_UPLOADED check." >&2
    echo "  Possible cause: Velox /api/v1/jobs/{id} response shape hides the" \
         "remote_id; an operator may need to add a dedicated enumeration endpoint." >&2
  else
    DELIVERY_URL="${INSTAEDIT_BASE_URL}/api/v1/integrations/velox/deliveries/${EXTERNAL_DELIVERY_ID}"
    echo "--- polling GET ${DELIVERY_URL} (timeout=${PRIVATE_TIMEOUT}s) ---"
    elapsed=0
    sleep_s=1
    last_private_status=""
    last_private_body=""
    while (( elapsed < PRIVATE_TIMEOUT )); do
      sleep "$sleep_s"
      elapsed=$((elapsed + sleep_s))
      sleep_s=$(( sleep_s * 2 ))
      if (( sleep_s > 16 )); then sleep_s=16; fi
      PRIV_RC=0
      curl -sS -m 10 \
        "${DELIVERY_URL}" \
        -D "$TMP_HDRS" -o "$TMP_BODY" 2>"$TMP_TRACE" || PRIV_RC=$?
      if [[ $PRIV_RC -ne 0 ]]; then
        echo "FATAL: curl could not reach ${DELIVERY_URL} (exit=${PRIV_RC})" >&2
        exit 3
      fi
      priv_status=$(awk 'NR==1 && $1 ~ /^[Hh][Tt][Tt][Pp]// {print $2; exit}' "$TMP_HDRS")
      priv_body=$(cat "$TMP_BODY")
      if [[ "${DEBUG_MODE}" == "1" ]]; then
        echo "DEBUG: private poll t=${elapsed}s HTTP=${priv_status}" >&2
      fi
      if [[ "$priv_status" == "200" ]]; then
        priv_status_value=$(printf '%s' "$priv_body" | jq -er '.status // empty') || true
        if [[ -n "$priv_status_value" ]]; then
          last_private_status="$priv_status_value"
          last_private_body="$priv_body"
        fi
        case "$priv_status_value" in
          PRIVATE_UPLOADED)
            echo "OK: PRIVATE_UPLOADED reached on InstaeditLogin side after ${elapsed}s"
            break
            ;;
          PUBLISHED|THUMBNAIL_PENDING)
            # Success-on-the-way states — keep polling.
            ;;
          FAILED|CANCELLED)
            echo "FATAL: delivery state ${priv_status_value} on InstaeditLogin side" >&2
            echo "  Last response body:" >&2
            echo "$priv_body" | sed 's/^/    /' >&2
            exit 9
            ;;
        esac
      elif [[ "$priv_status" == "404" ]]; then
        # The endpoint hasn't surfaced this delivery yet; keep polling.
        :
      else
        echo "FATAL: GET ${DELIVERY_URL} returned HTTP ${priv_status}" >&2
        echo "  Response body:" >&2
        echo "$priv_body" | sed 's/^/    /' >&2
        exit 4
      fi
    done

    if [[ "$last_private_status" != "PRIVATE_UPLOADED" ]]; then
      echo "FATAL: PRIVATE_UPLOADED not reached on InstaeditLogin side after ${PRIVATE_TIMEOUT}s" >&2
      echo "  Last observed private status: ${last_private_status:-none}" >&2
      exit 9
    fi
  fi
else
  echo "NOTICE: INSTAEDIT_BASE_URL unset — skipping step 6 (PRIVATE_UPLOADED on" \
       "InstaeditLogin side). Velox SUCCEEDED is the canonical quality gate" \
       "for this smoke when running in master-only environments." >&2
fi
}
