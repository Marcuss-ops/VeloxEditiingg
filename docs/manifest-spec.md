# `velox.render-manifest.v1` — Canonical Schema Spec

> **Source of truth.** This document is the **canonical spec** for the
> `velox.render-manifest.v1` JSON that `POST /api/v1/jobs` accepts via
> the optional `manifest_ref` field. Any drift between this doc, the
> Go types in `DataServer/internal/apiwire/apiwire.go` (the
> `SubmitManifestRef` + `SubmitScene` + nested
> `SubmitClip/SubmitVoiceover/SubmitSubtitles` structs), and the
> `RenderManifestResolver` (yet-to-land) is a contract bug.
>
> The CI guard `scripts/ci/check-manifest-schema-canicality.sh`
> asserts THIS DOCUMENT against a fixture JSON on every push,
> catching drift between this spec and the runtime contract before
> it reaches production.

## 1. Top-level structure

Every manifest is a single JSON object with **exactly nine** top-level
keys (no extras; the CI guard fails on missing/extra keys at the
top level):

```jsonc
{
  "schema_version":  "velox.render-manifest.v1",   // [1] required, closed enum
  "manifest_id":     "pg_20260728_4f82d731a91c",   // [2] required, unique id
  "created_at":      "2026-07-28T14:30:00Z",       // [3] required, RFC 3339 UTC
  "source":          { ... },                      // [4] required, source provider metadata
  "video":           { ... },                      // [5] required, output video params
  "script":          { ... },                      // [6] required, script text + reference
  "scenes":          [ ... ],                      // [7] required, non-empty array
  "delivery_plan":   [ ... ],                      // [8] required, non-empty array
  "integrity":       { ... }                       // [9] required, sha256 + counts
}
```

| # | Field          | Type   | Required | Constraint |
|---|----------------|--------|:--------:|------------|
| 1 | `schema_version` | string |   yes   | MUST equal `"velox.render-manifest.v1"` (closed enum; future versions bump to v2 etc.) |
| 2 | `manifest_id`    | string |   yes   | Canonical client-generated id (e.g. `pg_<UTC-timestamp>_<12-hex>`); unique per producer |
| 3 | `created_at`     | string |   yes   | RFC 3339 timestamp with `Z` suffix (UTC); no local-offset timestamps accepted |
| 4 | `source`         | object |   yes   | See [§5 Source](#5-source-object) |
| 5 | `video`          | object |   yes   | See [§6 Video](#6-video-object) |
| 6 | `script`         | object |   yes   | See [§7 Script](#7-script-object) |
| 7 | `scenes`         | array  |   yes   | Non-empty; per-element shape per [§8 Scene](#8-scene-object) |
| 8 | `delivery_plan`  | array  |   yes   | Non-empty; per-element shape per [§9 DeliveryPlan entry](#9-delivery_plan-entry) |
| 9 | `integrity`      | object |   yes   | Self-consistency hash — see [§10 Integrity](#10-integrity-object) |

> **Reject envelope (HTTP layer).** A manifest that fails ANY
> required-field check, schema_version mismatch, sha256 mismatch, or
> type/format violation returns **HTTP 422 `invalid_payload`** with
> `details[].path` pointing at the offending key (e.g.
> `"integrity.manifest_sha256"`, `"scenes[0].clip.url"`). See
> `DataServer/internal/handlers/server/pipeline/job_submit.go` for
> the canonical validator shape.

---

## 5. Source object

Identifies who produced the manifest. Operators rely on this for
provenance / churn audit.

```jsonc
"source": {
  "provider":            "pipelinegen",         // required, string
  "pipelinegen_job_id":  "job_pipelinegen_123",  // required, string
  "generation_schema":   1                       // required, integer ≥ 1
}
```

| Field                  | Required | Constraint |
|------------------------|:--------:|------------|
| `provider`             |   yes   | Closed enum today (`pipelinegen`); new providers are an additive bump to a future `v2` schema |
| `pipelinegen_job_id`   |   yes   | The PipelineGen-side job id this manifest was rendered from. MUST be unique per producer run |
| `generation_schema`     |   yes   | Integer ≥ 1; the PipelineGen-side rendering schema version (independent of `schema_version` which is the manifest-shape version) |

---

## 6. Video object

Output video parameters used by the master to fill the executor-side
worker payload.

```jsonc
"video": {
  "name":          "Jackie Chan Documentary",   // required, string (≤ 300 bytes)
  "language":      "en",                       // required, string (ISO 639-1: 2 lowercase letters)
  "width":         1920,                       // required, integer > 0 (typical: 1920, 1280, 854)
  "height":        1080,                       // required, integer > 0 (typical: 1080, 720, 480)
  "fps":           30,                         // required, integer > 0, ≤ 120
  "output_format": "mp4"                       // required, string, closed enum ("mp4" only today)
}
```

| Field           | Required | Constraint |
|-----------------|:--------:|------------|
| `name`          |   yes   | Display name; ≤ 300 bytes (mirrors `SubmitJobRequest.video_name`'s `MaxVideoNameBytes = 300`) |
| `language`      |   yes   | ISO 639-1 two-letter code (e.g. `en`, `it`, `zh`) |
| `width`         |   yes   | Integer > 0. Typical: 1920 / 1280 / 854 |
| `height`        |   yes   | Integer > 0. Typical: 1080 / 720 / 480 |
| `fps`           |   yes   | Integer > 0 and ≤ 120 (matches the C++ renderer's nominal ceiling) |
| `output_format` |   yes   | Closed enum today (`mp4`). Additive format bumps require a `v2` schema |

---

## 7. Script object

Plain-text script body and the upstream Google Doc that produced it
(the doc URL is preserved for traceability — operators re-rendering
the same doc later reference the canonical URL).

```jsonc
"script": {
  "text":           "Full narration text...",  // required, string (non-empty, ≤ 200_000 bytes)
  "google_doc_url": "https://docs.google.com/...", // required, string (well-formed http(s) URL)
  "language":       "en"                       // required, string (ISO 639-1)
}
```

| Field            | Required | Constraint |
|------------------|:--------:|------------|
| `text`           |   yes   | Non-empty string. ≤ 200_000 bytes (defensive ceiling; mirrors `data/api/openapi.yaml:ScriptRequest.text.maxLength`) |
| `google_doc_url` |   yes   | Canonical `https://docs.google.com/...` URL pointing at the source-of-truth doc |
| `language`       |   yes   | ISO 639-1 two-letter code |

---

## 8. Scene object

Each scene is a single JSON object inside the `scenes` array. The
array MUST contain **at least one** scene. Per-scene enrichment
(Phase 2 of the render-manifest plan) makes the nested
`clip` / `voiceover` / `subtitles` objects the canonical
source-of-truth for asset URLs — `voiceover_paths[]`-style positional
coupling is explicitly FORBIDDEN at this layer.

```jsonc
{
  "scene_id":     "scene-0",                          // required, string (≤ 64 bytes)
  "index":        0,                                  // required, integer ≥ 0
  "kind":         "intro",                            // required, string (≤ 32 bytes)
  "text":         "First narration line",             // required, string (non-empty, ≤ 2000 bytes)
  "duration_ms":  7200,                               // required, integer ≥ 0

  "clip": {                                          // optional nested (pointer-nil on SubmitScene ⇒ field absent here)
    "asset_id":      "clip_123",                       // optional, string (≤ 128 bytes)
    "drive_file_id": "1cQVbWKK4Tlmf2b9XIoMpLq3DnIblb53t", // optional, string (≤ 128 bytes)
    "url":            "https://drive.google.com/...",  // conditional: required when the parent's clip{} is present
    "sha256":         "<64 lowercase hex chars>",      // optional, but if present MUST match `^[0-9a-f]{64}$`
    "start_ms":      0,                                // optional, integer ≥ 0
    "end_ms":        7200,                             // optional, integer ≥ 0
    "duration_ms":   7200                              // optional, integer ≥ 0
  },
  "voiceover": {                                      // optional nested
    "asset_id":      "voiceover_scene_0",              // optional, string (≤ 128 bytes)
    "drive_file_id": "19m3s1-_guIYqEZE2Ywy77s_mJZMR7686", // optional, string
    "url":            "https://drive.google.com/...",  // conditional: required when the parent's voiceover{} is present
    "sha256":         "<64 lowercase hex chars>",      // optional, format-validated like clip.sha256
    "duration_ms":   7190,                             // optional, integer ≥ 0
    "language":       "en"                             // optional, ISO 639-1
  },
  "subtitles": {                                      // optional nested
    "asset_id":  "subtitle_scene_0",                  // optional, string (≤ 128 bytes)
    "format":    "ass",                               // conditional: required when parent's subtitles{} is present; enum: ass | srt | vtt
    "url":        "https://drive.google.com/...",     // conditional: required when the parent's subtitles{} is present
    "sha256":     "<64 lowercase hex chars>",         // optional, format-validated
    "language":   "en"                                // optional, ISO 639-1
  }
}
```

| Top-level scene field | Required | Constraint |
|-----------------------|:--------:|------------|
| `scene_id`            |   yes   | Client-supplied identifier (`scene-0`, …). ≤ 64 bytes |
| `index`               |   yes   | Integer ≥ 0 (timeline position; the master REORDERS by this field if scenes arrive out-of-order) |
| `kind`                |   yes   | Free-form role tag (`intro`, `clip`, `outro`, …); ≤ 32 bytes |
| `text`                |   yes   | Non-empty narration line. ≤ 2000 bytes (mirrors `SubmitScene.text.validate:"min=1"`) |
| `duration_ms`         |   yes   | Integer ≥ 0; the executor uses this to lay out the timeline (scene.composite.v1 sub-second cuts round-trip via this field, NOT a float seconds field) |
| `clip`                |  no    | Nested object. Pointer-nil on the Go wire struct ⇒ field absent here. When present, MUST be a valid [clip object](#clip-object) |
| `voiceover`           |  no    | Nested object. Same indirection as `clip` |
| `subtitles`           |  no    | Nested object. Same indirection as `clip` |

### clip object

The canonical clip URL for this scene. The asset is fetched by the
master-side asset bridge and passed verbatim to the worker payload.

| Field           | Required | Constraint |
|-----------------|:--------:|------------|
| `asset_id`      |   no    | Producer-supplied id; ≤ 128 bytes |
| `drive_file_id` |   no    | Google Drive file id; ≤ 128 bytes |
| `url`           |   conditional    | **REQUIRED when the parent `clip{}` is present.** MUST be a `https://` or `velox-asset://` URL (matches `manifestRefURLRegexp`); ≤ 2048 bytes after `TrimSpace` |
| `sha256`        |   no    | Lowercase hex, exactly 64 chars (`^[0-9a-f]{64}$`). Used by the asset-bridge to fail-closed on byte-level corruption |
| `start_ms`      |   no    | Integer ≥ 0 |
| `end_ms`        |   no    | Integer ≥ 0 |
| `duration_ms`   |   no    | Integer ≥ 0 |

### voiceover object

The canonical voiceover URL for this scene. Replaces the legacy
position-coupled `voiceover_paths[N] ↔ scenes[N]` contract.

| Field           | Required | Constraint |
|-----------------|:--------:|------------|
| `asset_id`      |   no    | Producer-supplied id; ≤ 128 bytes |
| `drive_file_id` |   no    | Google Drive file id; ≤ 128 bytes |
| `url`           |   conditional    | **REQUIRED when the parent `voiceover{}` is present.** Same scheme allow-list as `clip.url` |
| `sha256`        |   no    | Lowercase hex, exactly 64 chars |
| `duration_ms`   |   no    | Integer ≥ 0 |
| `language`      |   no    | ISO 639-1 |

### subtitles object

The canonical subtitle URL for this scene. One per scene (multiple
subtitle tracks per scene are intentionally NOT supported at this
layer — workers can layer multiple tracks via their own executor-side
config).

| Field      | Required | Constraint |
|------------|:--------:|------------|
| `asset_id` |   no    | Producer-supplied id; ≤ 128 bytes |
| `format`   |   conditional    | **REQUIRED when the parent `subtitles{}` is present.** Closed enum: `ass` (Advanced SubStation Alpha), `srt` (SubRip), `vtt` (WebVTT). The Chronon compositor today supports these three |
| `url`      |   conditional    | **REQUIRED when the parent `subtitles{}` is present.** Same scheme allow-list as `clip.url` |
| `sha256`   |   no    | Lowercase hex, exactly 64 chars |
| `language` |   no    | ISO 639-1 |

---

## 9. `delivery_plan` entry

Each entry is a single JSON object inside the `delivery_plan` array.
The array MUST contain **at least one** entry. Mirrors
`SubmitDeliveryPlanEntry` (Phase 2 wire-shape parity).

```jsonc
{
  "destination_id": "drive",            // required, string (non-empty)
  "priority":       1,                   // optional, integer ≥ 0 (default 0)
  "retry_budget":   3,                   // optional, integer ≥ 0 (default 3; 0 = "no retries" — explicit client opt-out)
  "metadata": {                          // optional, free-form object
    "folder_id": "GOOGLE_DRIVE_FOLDER_ID"
  }
}
```

| Field           | Required | Constraint |
|-----------------|:--------:|------------|
| `destination_id` |   yes   | Non-empty after `TrimSpace`; the master validates the id exists in `delivery_destinations` and is `enabled = 1` BEFORE substituting the manifest into the worker payload. A non-existent or disabled destination triggers the canonical 422 `invalid_payload` with `details[].path = delivery_plan.<i>.destination_id` |
| `priority`       |   no    | Integer ≥ 0; lower number = higher urgency (matches `SubmitDeliveryPlanEntry.priority.validate:"omitempty,gte=0"`) |
| `retry_budget`   |   no    | Integer ≥ 0. **Explicit 0 is allowed** — the worker treats `retry_budget = 0` as the client-chosen "no retries" intent and terminal-fails on the first hard error |
| `metadata`       |   no    | Free-form JSON object; passed verbatim to the worker payload (e.g. Drive `folder_id`, S3 `bucket_name`, …). MUST NOT contain keys that would shadow reserved keys (`destination_id`, `priority`, `retry_budget`) |

---

## 10. Integrity object

Self-describing hash that the master verifies BEFORE substituting
the manifest into the worker payload. The hash is computed over the
canonical JSON serialization of the manifest body **excluding the
`integrity.manifest_sha256` field itself** (so the hash doesn't
include its own value — that would be a cycle).

```jsonc
"integrity": {
  "algorithm":          "sha256",            // required, closed enum ("sha256" today)
  "manifest_sha256":    "<64 lowercase hex>", // required, sha256(body minus this field)
  "scene_count":        1,                   // required, integer ≥ 1 (matches `len(scenes)`)
  "total_duration_ms":  7200                // required, integer ≥ 0 (matches `sum(scenes[*].duration_ms)`)
}
```

| Field                | Required | Constraint |
|----------------------|:--------:|------------|
| `algorithm`          |   yes   | Closed enum today (`sha256`). Future hash algos (e.g. `sha512`) require an additive `v2` schema bump |
| `manifest_sha256`     |   yes   | Lowercase hex, exactly 64 chars. Computed as `sha256(canonical_json_serialization(body minus integrity.manifest_sha256))`. The canonical form is: UTF-8, no whitespace, sorted keys, `","` and `":"` separators (matches Python's `json.dumps(body, sort_keys=True, separators=(',',':'))`) |
| `scene_count`        |   yes   | Integer ≥ 1. MUST equal `len(scenes)`. Mismatch ⇒ 422 invalid_payload |
| `total_duration_ms`   |   yes   | Integer ≥ 0. MUST equal `sum(scenes[*].duration_ms)`. Mismatch ⇒ 422 invalid_payload |

### Canonical form (the only form the master accepts)

```python
import hashlib, json

def manifest_sha256(body: dict) -> str:
    # Strip the integrity.manifest_sha256 field BEFORE serializing
    body_minus_self = {**body}
    body_minus_self.pop("integrity", None)  # entire integrity block omitted
    # (Future note: an alternative form keeps "integrity" but strips
    # only the "manifest_sha256" sub-key. The validator currently
    # strips the whole "integrity" block — pin ONE form and disallow
    # the other via CI to prevent drift.)
    canonical = json.dumps(body_minus_self, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(canonical).hexdigest()
```

> **Fail-closed semantics.** A mismatched
> `integrity.manifest_sha256` returns **422 `invalid_payload`** with
> `details[].path = "integrity.manifest_sha256"`. The worker payload
> is NEVER substituted with a tampered manifest — operator can
> trust the byte-level content delivered to the worker.

---

## 11. Reject envelope (canonical)

When ANY of the above checks fails (missing required field, wrong
type, sha256 mismatch, schema_version mismatch, scene_count /
total_duration_ms drift, …) the master returns:

```http
HTTP/1.1 422 Unprocessable Entity
Content-Type: application/json

{
  "ok":      false,
  "error":   "invalid_payload",
  "message": "request body has N validation failure(s) (see details)",
  "details": [
    { "path": "integrity.manifest_sha256",                 "issue": "mismatch" },
    { "path": "scenes[0].clip.url",                          "issue": "unsupported_scheme", "observed": "file://...", "allowed": ["https://", "http://", "velox-asset://"] },
    { "path": "delivery_plan[1].destination_id",             "issue": "invalid", "observed": "drive_typo" },
    { "path": "integrity.scene_count",                       "issue": "out_of_range", "observed": 3, "expected": "len(scenes)" }
  ]
}
```

The `details[].path` uses dotted-index notation matching the rest of
the `POST /api/v1/jobs` validator envelope; clients can programmatically
react to each violation without parsing the human-readable message.

---

## 12. Acceptance tests

The canonical conformance test is
`scripts/ci/check-manifest-schema-canicality.sh` which asserts:

1. **Spec coverage** — every required field name from §1 (§5, §6,
   §7, §8, §9, §10) appears as a heading or explicit field
   declaration in this document.
2. **Fixture existence + parsing** — the fixture at
   `scripts/ci/fixtures/manifest.v1.fixture.json` parses as valid JSON.
3. **Required-field presence** — `schema_version`, `manifest_id`,
   `created_at`, `source.provider`, `video.name`, `script.text`,
   `scenes`, `delivery_plan`, `integrity.manifest_sha256` all
   present on the fixture.
4. **`schema_version` closed enum** — equals `"velox.render-manifest.v1"`.
5. **`integrity.manifest_sha256` self-consistency** — the
   sha256 of the canonical JSON serialization of the fixture
   (minus the `integrity.manifest_sha256` self) equals the
   stated value. A drifted spec OR a hand-edited fixture ⇒ exit 1.
6. **`integrity.scene_count` and `integrity.total_duration_ms`**
   match the asserted values from `len(scenes)` and
   `sum(scenes[*].duration_ms)`.
7. **Negative-path fixture** — a second fixture at
   `scripts/ci/fixtures/manifest.v1.bad-fixture.json` (with
   sha256 intentionally wrong) produces the validator's
   mismatch exit code — guards that the validator's failure
   path itself is non-trivial (so a future change can't silently
   "fix" a regression by making the validator always pass).

Run on every push:

```bash
bash scripts/ci/check-manifest-schema-canicality.sh
```
