# Velox job publishing payload

This document defines the canonical flow used by trusted systems that submit
render jobs and optionally request social delivery.

```text
job sender → Velox Master → render worker → InstaEdit → private upload
```

The sender never calls a render worker directly and never receives OAuth,
Google, YouTube or provider secrets.

## 1. Discover channels through Velox

The trusted sender calls Velox with the same M2M authentication used for job
submission:

```http
POST /api/v1/publishing/targets
Authorization: Bearer <VELOX_M2M_TOKEN>
Content-Type: application/json
```

```json
{
  "workspace_id": 12,
  "platform": "youtube"
}
```

To narrow discovery to one InstaEdit account/channel:

```json
{
  "workspace_id": 12,
  "platform": "youtube",
  "platform_account_id": 381
}
```

Velox calls InstaEdit's internal catalog, synchronizes its own
`delivery_destinations` registry and returns Velox-ready destination IDs:

```json
{
  "workspace_id": 12,
  "platform": "youtube",
  "targets": [
    {
      "destination_id": "instaedit_extdst_01JREADY",
      "external_destination_id": "extdst_01JREADY",
      "platform_account_id": 381,
      "platform": "youtube",
      "channel_id": "UCxxxxxxxx",
      "channel_name": "Wrestling Discovery",
      "status": "active",
      "enabled": true,
      "can_post": true,
      "capabilities": {
        "upload_video": true,
        "set_thumbnail": true,
        "publish": true,
        "schedule": true
      }
    },
    {
      "platform_account_id": 442,
      "platform": "youtube",
      "channel_id": "UCyyyyyyyy",
      "channel_name": "Channel requiring login",
      "status": "reauth_required",
      "enabled": true,
      "can_post": false,
      "block_reason": "channel authentication requires attention",
      "target_error_code": "BLOCKED_AUTH",
      "capabilities": {
        "upload_video": false,
        "set_thumbnail": false,
        "publish": false,
        "schedule": false
      }
    }
  ]
}
```

A sender must only permit selection when all of these conditions are true:

```text
can_post = true
+ destination_id is non-empty
+ capabilities.upload_video = true
```

`channel_name` is display-only. The sender copies `destination_id` exactly as
returned by Velox.

## 2. Submit a render job with social delivery

Publishing metadata uses the existing `delivery_plan[].metadata` field. The
selected channel is represented only by `destination_id`; it is not repeated as
an account name or channel name inside metadata.

```json
{
  "idempotency_key": "pipelinegen-job123-a84f927c",
  "video_name": "Video title",
  "script_text": "Complete script",
  "scenes": [
    {
      "scene_id": "scene-0",
      "index": 0,
      "text": "Scene text",
      "duration_seconds": 7.2,
      "clip": {
        "asset_id": "clip-001",
        "url": "velox-asset://clip-001"
      },
      "voiceover": {
        "asset_id": "voice-001",
        "url": "velox-asset://voice-001",
        "duration_ms": 7200,
        "language": "it"
      }
    }
  ],
  "delivery_plan": [
    {
      "destination_id": "instaedit_extdst_01JREADY",
      "priority": 1,
      "retry_budget": 3,
      "metadata": {
        "contract_version": "velox.instaedit.publish.v1",
        "title": "Video title",
        "description": "Video description",
        "tags": ["wwe", "wrestling"],
        "privacy_status": "private",
        "final_privacy": "public",
        "require_thumbnail": true
      }
    }
  ]
}
```

Optional scheduling metadata:

```json
{
  "publish_at": "2026-07-30T18:00:00Z"
}
```

The initial upload must always remain private. InstaEdit applies the thumbnail
before executing the final public, unlisted or scheduled transition.

## 3. No social delivery

When the caller does not specify a channel, omit the InstaEdit delivery entry.
The render job can still use a Drive or other registered destination:

```json
{
  "delivery_plan": [
    {
      "destination_id": "<explicit-destination-id>",
      "retry_budget": 3
    }
  ]
}
```

Do not invent a default social channel and do not select the first catalog row
automatically.

## 4. Failure rules

- Missing or disabled `destination_id`: Velox rejects the job before enqueue.
  The 422 envelope distinguishes the two failure modes (NIT-2, §0.3.4 item 4):
  - `details[].target_error_code="DESTINATION_NOT_FOUND"` — the `destination_id`
    is not present in `delivery_destinations`. Operator fix: re-pick from
    `/publishing/targets`; never invent a destination.
  - `details[].target_error_code="BLOCKED_VELOX_DISABLED"` — the row exists but
    `delivery_destinations.enabled=0` on Velox. Operator fix: `UPDATE
    delivery_destinations SET enabled = 1 WHERE destination_id = '...'` OR
    re-sync the catalog via `POST /api/v1/admin/destinations/sync`.
- Channel requires reauthorization: catalog returns `can_post=false`.
- Catalog returned one or more rows but none satisfy the publishable predicate
  (`can_post=true AND capabilities.upload_video=true`): the catalog response
  carries a top-level `error.code="BLOCKED_NO_PUBLISHABLE_CHANNEL"`. Inspect
  each entry's `target_error_code` (`BLOCKED_AUTH` / `TARGET_NOT_AVAILABLE`)
  and act per the §0.2.2 cheat-sheet in `SOCIAL_API_MIGRATION_RUNBOOK.md`.
- InstaEdit unavailable: target discovery returns a service/upstream error.
- Same job retry: reuse the original `idempotency_key`.
- Thumbnail or publish failure: the uploaded video remains private.
- Display-name changes never affect routing because delivery uses opaque IDs.

The wire-format codes are canonical and locked at
`DataServer/internal/handlers/server/pipeline/publishing_error_codes.go`.
Adding a new code requires a cross-repo changelog; renaming or removing an
existing code requires bumping `velox.instaedit.publish.v1` (the contract
version stamped in `delivery_plan[].metadata`).
