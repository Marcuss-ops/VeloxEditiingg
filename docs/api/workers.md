# Workers API

## GET /api/v1/workers

List all registered workers with status, metadata, and health info.

### Response

```json
{
  "workers": [
    {
      "worker_id": "host_51_91_11_36",
      "status": "online",
      "last_heartbeat": "2026-06-12T10:00:00Z",
      "bundle_version": "v1.0.6",
      "protocol_version": "2026-06-worker-v1",
      "capabilities": ["video_processing", "ffmpeg"],
      "current_job": null
    }
  ],
  "total": 3,
  "online": 2,
  "offline": 1
}
```

## GET /api/v1/workers/status

Dashboard status with master and worker metadata, mismatch warnings.

### Response

```json
{
  "master": { "version": "v1.0.6", "protocol_version": "2026-06-worker-v1" },
  "workers": [...],
  "warnings": []
}
```

### Warning types

- `bundle_mismatch`: worker bundle != master manifest
- `hash_mismatch`: bundle hash != manifest hash
- `code_mismatch`: code_version != master version
- `protocol_mismatch`: worker protocol != master protocol
- `missing_bundle_hash`: manifest missing bundle_hash
- `missing_capabilities`: worker missing capabilities

## POST /api/v1/workers/update_all

Trigger update on all workers.

### Query params
- `exclude_local` (bool, default true) - exclude local worker
- `dry_run` (bool) - preview without executing

## POST /api/v1/workers/rollout_update

Rollout update to a subset of workers.

## POST /api/v1/workers/restart_all

Restart all worker containers.

## POST /api/v1/workers/send_command_bulk

Send a command to multiple workers.

## GET /api/v1/workers/commands

Workers poll for pending commands (heartbeat endpoint).

### Response

```json
{
  "commands": [
    { "command_id": "...", "action": "update", "params": {} }
  ]
}
```

## POST /api/v1/workers/commands/ack

Worker acknowledges a command execution.

### Request body

```json
{
  "command_id": "...",
  "status": "completed",
  "result": "..."
}
```
