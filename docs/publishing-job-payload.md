# Velox job publishing payload

This document defines the canonical publishing intent carried by callers of
`POST /api/v1/jobs`.

The render job is still submitted only to the Velox Master. Social OAuth tokens
remain inside InstaEdit and must never be placed in this payload.

## 1. Discover available channels

Before submitting a job, the service calls InstaEdit using the Velox M2M token:

```http
POST /internal/v1/destinations/resolve-target
Authorization: Bearer <VELOX_API_TOKEN>
Content-Type: application/json
```

```json
{
  "workspace_id": 12,
  "platform": "youtube",
  "target": {
    "type": "catalog"
  }
}
```

The response contains every YouTube channel bound to the workspace. Stable
identifiers are authoritative; `channel_name` is display-only.

```json
{
  "valid": true,
  "destination_id": "instaedit_youtube",
  "resolved_targets": [
    {
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

A sender must only allow selection of rows where `can_post=true`.

## 2. Submit one channel target

The publishing intent is stored inside the existing
`delivery_plan[].metadata` boundary. Velox already preserves this object into
the canonical worker payload, so no parallel top-level publishing contract is
introduced.

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
      "destination_id": "instaedit_youtube",
      "priority": 1,
      "retry_budget": 3,
      "metadata": {
        "contract_version": "velox.instaedit.publish.v1",
        "publishing": {
          "platform": "youtube",
          "workspace_id": 12,
          "target": {
            "type": "channel",
            "platform_account_id": 381,
            "channel_id": "UCxxxxxxxx"
          },
          "initial_privacy": "private",
          "final_privacy": "public",
          "require_thumbnail": true
        }
      }
    }
  ]
}
```

Both `platform_account_id` and `channel_id` may be supplied. InstaEdit treats
the numeric account ID as authoritative and uses the provider-native channel ID
as a cross-check against an OAuth grant that may have changed channel.

## 3. Submit a group target

```json
{
  "destination_id": "instaedit_youtube",
  "metadata": {
    "contract_version": "velox.instaedit.publish.v1",
    "publishing": {
      "platform": "youtube",
      "workspace_id": 12,
      "target": {
        "type": "group",
        "group_id": 27
      },
      "initial_privacy": "private",
      "final_privacy": "public",
      "require_thumbnail": true
    }
  }
}
```

InstaEdit expands a group into independent per-channel deliveries. A partial
failure must remain visible per target and must never be collapsed into a false
success.

## 4. Mandatory pre-flight

Before enqueueing a job with publishing metadata, the sender must validate the
selected target:

```json
{
  "workspace_id": 12,
  "platform": "youtube",
  "target": {
    "type": "channel",
    "platform_account_id": 381,
    "channel_id": "UCxxxxxxxx"
  }
}
```

The same endpoint returns `valid=false` with `TARGET_NOT_AVAILABLE`,
`GROUP_EMPTY`, or `BLOCKED_AUTH` when the target cannot be used.

## 5. Safety invariants

- The sender submits only to the Velox Master, never directly to a worker.
- The sender never sends OAuth, refresh, Google, YouTube, or provider API keys.
- Stable IDs are used; display channel names are never identifiers.
- The initial YouTube privacy is always `private`.
- Final publication is blocked until the thumbnail has been applied.
- A replay uses the same idempotency key and must not create a second upload.
