# Bundle API

## POST /bundle/manifest/generate

Regenerate `manifest_v2.json` from current config and bundle SHA256.

### Response

```json
{
  "ok": true,
  "message": "Manifest regenerated",
  "path": "/path/to/manifest_v2.json",
  "version": "v1.0.6",
  "code_version": "v1.0.6",
  "build_hash": "da65a9fb...",
  "bundle_path": "/path/to/worker_code_linux_x86_64.zip"
}
```

### Manifest V2 schema

```json
{
  "version": "v1.0.6",
  "code_version": "v1.0.6",
  "bundle_version": "v1.0.6",
  "build_hash": "<sha256>",
  "bundle_hash": "<sha256>",
  "protocol_version": "2026-06-worker-v1",
  "engine_version": "v1.0.6",
  "platform": "linux",
  "arch": "x86_64",
  "timestamp": "2026-06-12T14:00:36Z",
  "generated_at": "2026-06-12T14:00:36Z"
}
```

## GET /api/worker/v2/manifest

Serve `manifest_v2.json` directly.

## GET /api/worker/bundle

Download the worker bundle zip.

### Query params
- `platform` (default: `linux`)
- `arch` (default: `x86_64`)

### Response headers
- `X-Bundle-SHA256`: SHA256 of the bundle

## GET /api/worker/bundle/files

List files inside the bundle zip.

### Query params
- `platform` (default: `linux`)
- `arch` (default: `x86_64`)
- `path` / `prefix` - filter by directory prefix

## GET /install_worker/latest

Get latest bundle info with hash.

### Response

```json
{
  "bundle_hash": "da65a9fb...",
  "manifest_url": "/install_worker/manifest/{hash}?platform=linux&arch=x86_64",
  "updated_at": "2026-06-12T14:00:36Z",
  "filename": "worker_code_linux_x86_64.zip"
}
```

## GET /install_worker/manifest/:bundle_hash

Get bundle file listing for a specific bundle hash.

### Query params
- `platform` (default: `linux`)
- `arch` (default: `x86_64`)

### Response

```json
{
  "bundle_hash": "da65a9fb...",
  "platform": "linux",
  "arch": "x86_64",
  "file_count": 42,
  "files": [...],
  "dir_hash": { "internal/": "...", "cmd/": "..." }
}
```

## GET /bundle_manifest.json

Full manifest with protocol_version, version info, and file inspection.

### Response

```json
{
  "version": "v1.0.6",
  "code_version": "v1.0.6",
  "protocol_version": "2026-06-worker-v1",
  "platform": "linux",
  "arch": "x86_64",
  "sha256": "...",
  "file_count": 42,
  "top_dirs": ["internal/", "cmd/"],
  "runtime": "go1.21"
}
```
