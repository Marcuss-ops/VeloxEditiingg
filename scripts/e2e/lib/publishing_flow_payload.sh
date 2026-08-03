#!/usr/bin/env bash
# Sourced payload and dry-run helpers for publishing_flow_smoke.sh.

build_targets_payload() {
TARGETS_PAYLOAD=$(jq -nc \
  --argjson ws "${PUBLISHING_WORKSPACE_ID}" \
  --arg platform "${PLATFORM}" \
  --argjson pa "${PLATFORM_ACCOUNT_ID:-null}" \
  '{workspace_id: $ws, platform: $platform}
   + (if $pa != null then {platform_account_id: $pa} else {} end)')
}

build_jobs_payload() {
JOBS_PAYLOAD=$(jq -nc \
  --arg idem "${IDEM_KEY}" \
  --arg ts "publishing_flow_smoke epoch=${EPOCH}" \
  --arg dest "${DESTINATION_ID}" \
  --arg contract "velox.instaedit.publish.v1" \
  --arg title "Velox Publishing Smoke (epoch=${EPOCH})" \
  --arg desc "Automated smoke script for cross-repo publishing flow." \
  '{
    idempotency_key: $idem,
    video_name: $ts,
    script_text: "Smoke script for publishing flow E2E.",
    scenes: [
      {
        text: "Smoke scene",
        duration_seconds: 3,
        clip: {url: "velox-asset://clips/pub-smoke.mp4", duration_ms: 3000},
        voiceover: {url: "velox-asset://voiceovers/pub-smoke.mp3", duration_ms: 3000}
      }
    ],
    delivery_plan: [
      {
        destination_id: $dest,
        priority: 1,
        retry_budget: 1,
        metadata: {
          contract_version: $contract,
          title: $title,
          description: $desc,
          tags: ["velox-smoke", "e2e", "publishing"],
          privacy_status: "private",
          final_privacy: "public",
          require_thumbnail: true
        }
      }
    ]
  }')
}

print_dry_run() {
  echo "[DRY] /api/v1/admin/m2m/keys POST would carry:" >&2
  echo "$ISSUE_REQ" >&2
  if [[ -n "$INSTAEDIT_BASE_URL" && -n "$INST_USER_TOKEN" ]]; then
    cat <<'DRYINNER' >&2
[DRY] /api/v1/integrations/velox/destinations GET would target:
[DRY]   ${INSTAEDIT_BASE_URL}/api/v1/integrations/velox/destinations?workspace_id=${PUBLISHING_WORKSPACE_ID}
[DRY]   Bearer: <USER-JWT, redacted>  (workspace-owner)
[DRY]   expected shape: { destinations: [ { external_destination_id, platform_account_id, status, ... } , ... ] }
[DRY]   step 1b captures: S_inst := [external_destination_id for each enabled active row]
DRYINNER
  else
    echo "[DRY] (skipped) /api/v1/integrations/velox/destinations GET - INSTAEDIT_BASE_URL or INSTAEDIT_VELOX_USER_TOKEN unset" >&2
    echo "[DRY]   cross-validation degrades to one-sided shape check (destination_id STARTSWITH instaedit_)" >&2
  fi
  echo "[DRY] /api/v1/publishing/targets POST would carry:" >&2
  printf '{"workspace_id":%s,"platform":"%s"' \
    "${PUBLISHING_WORKSPACE_ID}" "${PLATFORM}" >&2
  if [[ -n "$PLATFORM_ACCOUNT_ID" ]]; then
    printf ',"platform_account_id":%s' "${PLATFORM_ACCOUNT_ID}" >&2
  fi
  printf '}\n' >&2
  echo "[DRY] step 2 captures: S_velox := [external_destination_id for each target row in response]" >&2
  echo "[DRY] step 2 will assert (invariant S_velox is-subset S_inst):" >&2
  echo "[DRY]   - chosen target destination_id STARTSWITH instaedit_" >&2
  echo "[DRY]   - exit 10 on STARTSWITH failure OR chosen-target suffix not in S_inst" >&2
  echo "[DRY]   - WARN line surfaces S_velox-S_inst diff across ALL targets (not just chosen)" >&2
  echo "[DRY] jobs POST would carry (delivery_plan[0].metadata per spec, jq-built for safety):" >&2
  jq -nc --arg idem "${IDEM_KEY}" --arg epoch "${EPOCH}" --arg dest "<velox-side-instaedit_...>" --arg contract "velox.instaedit.publish.v1" \
    '{
      idempotency_key: $idem,
      video_name: ("publishing_flow_smoke epoch=" + $epoch),
      script_text: "Smoke script for publishing flow E2E.",
      scenes: [ { text: "Smoke scene", duration_seconds: 3, clip: {url: "velox-asset://clips/pub-smoke.mp4", duration_ms: 3000}, voiceover: {url: "velox-asset://voiceovers/pub-smoke.mp3", duration_ms: 3000} } ],
      delivery_plan: [
        {
          destination_id: $dest,
          priority: 1,
          retry_budget: 1,
          metadata: {
            contract_version: $contract,
            title: ("Velox Publishing Smoke (epoch=" + $epoch + ")"),
            description: "Automated smoke script for cross-repo publishing flow.",
            tags: ["velox-smoke", "e2e", "publishing"],
            privacy_status: "private",
            final_privacy: "public",
            require_thumbnail: true
          }
        }
      ]
    }' >&2
  echo "[DRY] no live HTTP calls done; exit 0" >&2
  return 0
}
