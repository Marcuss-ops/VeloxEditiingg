# External API intake: `POST /api/v1/jobs` (+ `GET /api/v1/jobs/{job_id}`)

> Per il contratto machine-readable (operazione `submitJob`, schema `SubmitJobRequest` / `SubmitScene` / `SubmitDeliveryPlanEntry` / `SubmitJobAcceptedResponse` / `SubmitJobStatusResponse`, security scheme M2M Bearer), vedi `DataServer/api/openapi.yaml` (OpenAPI 3.1.0). Questo file `.md` è la narrativa; lo yaml è il source-of-truth per generatori client / validator OpenAPI / dashboard Swagger. In caso di divergenza, il comportamento testato in `DataServer/internal/handlers/server/pipeline/job_submit_e2e_test.go` è l'autorità finale.
>
> Lo smoke eseguibile end-to-end è `scripts/api/jobs_smoke.sh`: POST → GET con backoff esponenziale fino a stato terminale, con provisioning M2M inline tramite `/api/v1/admin/m2m/keys` (richiede solo `VELOX_ADMIN_TOKEN` come credenziale).

External systems (cron jobs, batch pipelines, downstream automation, third-party integrations) submit video jobs **directly** to the Velox master via a flat, M2M-protected HTTP endpoint. The endpoint reuses the canonical `creatorflow.Resolver` and the atomic `forwarding + Job + TaskSpec` writer; it does not introduce a second queue, a second database writer, or a parallel schema.

This is the SIMPLIFIED intake path intended for **automation**, NOT the operator-grade Creator sync push (`POST /api/v1/creator/jobs`, see [docs/CREATOR-PUSH.md](./CREATOR-PUSH.md)). The two paths converge on the same canonical resolver and produce semantically equivalent rows in `creator_forwardings`; the discriminator is `creator_forwardings.source_provider`:

```text
external_api   → POST /api/v1/jobs        (this document)
creator_*      → POST /api/v1/creator/jobs (operator-grade Creator workflow)
```

## Flow

```text
External system assembles payload (flat)
        ↓
Operator provisions an M2M key via /api/v1/admin/m2m/keys (once)
        ↓
External system GETs the plaintext secret (one-shot)
        ↓
For each job:
        POST /api/v1/jobs    (Authorization: Bearer <plaintext_secret>)
        ↓
        202 Accepted + Location: /api/v1/jobs/job_…
        ↓
        RemoteEngine typed-DTO adapter
        ↓
        creatorflow.Resolver
        ↓
        creator_forwardings + Job + TaskSpec (atomic)
        ↓
        GET /api/v1/jobs/{job_id}    (poll until terminal state)

After production handoff: optionally DELETE the M2M key to revoke
```

The endpoint maps the external payload through the SAME `remoteengine.ParseRemotePipelineResult` adapter the Creator push uses; the resolver sees one canonical shape regardless of the producer (operator Creator workstation vs. external automation).

## M2M client provisioning (operator step, once per client)

Unlike `POST /api/v1/creator/jobs` (which uses the operator-grade `VELOX_ADMIN_TOKEN`), `POST /api/v1/jobs` uses **per-client M2M credentials**. This is intentional: the operator's admin bearer is too coarse to distribute to external systems, and a single leaked operator token would expose the entire admin surface (`/api/v1/admin/m2m/*` is the same adminAuth boundary).

To provision a client:

```bash
curl -sS -X POST "${VELOX_MASTER_URL}/api/v1/admin/m2m/keys" \
  -H "Authorization: Bearer ${VELOX_ADMIN_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{
    "client_id": "client-acme-batch-nightly",
    "description": "Acme nightly batch pipeline",
    "scopes": ["jobs.submit"],
    "rate_limit_rps": 1,
    "rate_limit_burst": 5,
    "quota_max_scenes": 1000,
    "quota_max_total_secs": 14400
  }'
```

The response includes:
- `client_id`: the credential's stable identifier (audit logs + Prometheus labels).
- **`plaintext_secret`**: returned ONCE. The store only keeps SHA-256(plaintext). If lost, the supported remediation is rotate: POST a new key, DELETE the old one.
- `secret_hash`: the SHA-256 stored in `m2m_api_keys.secret_hash` (operational only — never used by clients).
- `scopes`: `["jobs.submit"]` is the only supported scope in v1.

The smoke script `scripts/api/jobs_smoke.sh` does this provisioning inline: pass `VELOX_ADMIN_TOKEN` and the script mints an ephemeral client, runs the smoke, and `DELETE`s the client on exit (best-effort cleanup).

### Revocation

```bash
curl -sS -X DELETE "${VELOX_MASTER_URL}/api/v1/admin/m2m/keys/client-acme-batch-nightly" \
  -H "Authorization: Bearer ${VELOX_ADMIN_TOKEN}"
```

Soft-disable (`is_active=0`) — the middleware rejects subsequent requests with `401 m2m_token_rejected`. Hard deletion is intentionally absent (audit trail preservation).

## Request

`POST /api/v1/jobs`

Headers:

```text
Authorization: Bearer <plaintext_secret>      (M2M bearer — NOT VELOX_ADMIN_TOKEN)
Content-Type:  application/json
X-Request-ID:  <client-supplied correlation ID; optional>
```

Body:

```json
{
  "idempotency_key": "acme-batch-nightly-20260727-001",
  "video_name": "Acme nightly — 2026-07-27",
  "script_text": "The nightly summary script...",
  "voiceover_paths": [
    "velox-asset://voiceovers/acme-nightly.mp3"
  ],
  "scenes": [
    {
      "text": "Opening hook",
      "clip_link": "velox-asset://clips/acme-hook-01.mp4",
      "image_link": "https://cdn.acme.com/hook-bg.png",
      "duration_seconds": 7
    }
  ],
  "delivery_plan": [
    {
      "destination_id": "drive",
      "priority": 1,
      "retry_budget": 3
    }
  ]
}
```

### Field rules (mirrors `ValidateSubmitJobRequest` server-side)

| Field | Required | Constraints |
|---|---|---|
| `idempotency_key` | yes | 1..128 bytes after trim, valid UTF-8, no `:` or `%`, no control chars |
| `video_name` | no | ≤ 300 bytes (matches `MaxVideoNameBytes`) |
| `script_text` | no | unbounded (content) |
| `voiceover_paths[]` | no | items must satisfy egress SSRF policy; either `velox-asset://…` or `https?://…` reachable URL |
| `scenes[]` | yes | 1..10000 entries, each with non-empty `text` and `0.1 ≤ duration_seconds ≤ 86400` |
| `layers[]` | no | independent compositing layers |
| `subtitle_tracks[]` | no | SRT/VTT/Chronon sources |
| `delivery_plan[].destination_id` | yes | non-empty |
| `delivery_plan[].retry_budget` | no | integer; explicit `0` round-trips distinct from omitted (handled via `*int`) |

The HTTP contract is the source of truth, but Go-side validation runs the same constraints (env-constant mirror: `MaxVideoNameBytes=300`, `MinSceneDurationSeconds=0.1`, `MaxSceneDurationSeconds=86400`, `MaxScenes=10000`, `DefaultRetryBudget=3`). On drift, `validate_openapi.py` catches schema vs. specified-ranges mismatches; Go-side constants drift only on a deliberate bump.

### Idempotency contract

The idempotency identity is:

```text
source_provider  = "external_api"        (constant; see pipeline.ExternalAPISourceProvider)
source_job_id    = idempotency_key        (the validated, post-trim form)
target_executor  = "scene.composite.v1"   (constant; see pipeline.JobSubmitTargetExecutorID)
```

Two requests with the same `(idempotency_key)` → same Velox job.

Two requests with the same `(idempotency_key)` but **different payloads** → `409 Conflict` (`idempotency_key_reused`, the resolve layer's SHA-256(payload) hash check on `creator_forwardings.payload_sha256` catches the drift). The hash check is in `creatorflow.Resolver` so cross-intake semantics are uniform.

The stable `source_provider="external_api"` is a deliberate invariant: dashboards can group "all external_api jobs" without scanning per-key labels; cross-job correlation attacks via provider dimension are blocked.

## Response (202 Accepted)

```json
{
  "ok": true,
  "accepted_from": "api_v1_jobs",
  "idempotency_key": "acme-batch-nightly-20260727-001",
  "job_id": "job_a1b2c3d4",
  "dispatch_status": "queued_for_workers",
  "status_url": "/api/v1/jobs/job_a1b2c3d4"
}
```

Headers:

```text
HTTP/1.1 202 Accepted
Location: /api/v1/jobs/job_a1b2c3d4
Content-Type: application/json
```

The `Location` header (RFC 7231 location-of-resource) carries the canonical polling URL for the just-created job. The body mirrors it as `status_url` so RESTless clients (e.g., shell scripts that only parse JSON) have a stable fallback.

The `accepted_from: "api_v1_jobs"` discriminator lets the creator-push / external-api / async-forwarder Prometheus metric `pipeline_creator_intake_accepted_total` split the three intake paths without additional protocol state.

## Polling

`GET /api/v1/jobs/{job_id}`

Headers (same M2M bearer):

```text
Authorization: Bearer <plaintext_secret>
```

Response (200 OK, 4-field envelope):

```json
{
  "ok": true,
  "job_id": "job_a1b2c3d4",
  "status": "PENDING",
  "created": true,
  "status_url": "/api/v1/jobs/job_a1b2c3d4"
}
```

The `status` field is `jobs.Status` (the canonical render state) when the `jobs` row has materialized; it falls back to `creator_forwardings.status` during the pre-FORWARDED race window (resolver commits `forwarding + Job + Task` atomically, but the canonical `jobs.Status` enum arrives slightly later). Clients implementing strict status matching should consult the public `jobs.Status` enum documented in `openapi.yaml`'s `SubmitJobStatusResponse.status.enum`.

`created` is a source discriminator: `true` when the forwarding was produced by `POST /api/v1/jobs` (`source_provider == "external_api"`); `false` for `POST /api/v1/creator/jobs`. A M2M dashboard can filter to its own intake path without a separate join.

Terminal states: `SUCCEEDED`, `FAILED`, `CANCELLED`. The canonical polling loop in `scripts/api/jobs_smoke.sh` does an exponential backoff (1s → 2s → 4s → 8s → 16s, total cap 60s) and exits 0 on terminal success, 6 on the `SUCCEEDED`-but-audit-row-only path (rare; smoke fires after the worker reports back), 7 on terminal failure, 8 on timeout.

### 404 envelope

Unknown `job_id` → 404 with the M2M-style envelope so a single error dispatcher handles auth + lookup misses:

```json
{
  "ok": false,
  "error": "job_not_found",
  "message": "job_id does not match any known creator forwarding"
}
```

## Error codes

The full set is centralised under `ErrorCode` in `openapi.yaml` (validator enforces bidirectional equality). The codes relevant to this endpoint:

| Code | Status | When |
|---|---|---|
| `invalid_json` | 400 | Body failed strict-JSON decode (unknown fields, trailing junk). |
| `invalid_payload` | 422 | Body parsed but failed cross-field validation. `details[]` enumerates offending paths. |
| `payload_incomplete` | 422 | Resolver rejected the worker payload (mostly unreachable for the simplified intake; reserved for the resolver layer). |
| `idempotency_key_reused` | 409 | Same key, different SHA-256(payload). |
| `resolver_failure` | 500 | Resolver-layer failure inside the atomic UoW. No job created. |
| `m2m_token_required` | 401 | Missing `Authorization: Bearer …`. |
| `m2m_token_rejected` | 401 | Bearer present but unknown / disabled / revoked. |
| `m2m_scope_rejected` | 403 | Key present but lacks `jobs.submit` scope. |
| `m2m_rate_limited` | 429 | Token-bucket exhaustion; `Retry-After: 1` header. |
| `m2m_quota_exceeded` | 429 | Per-request `quota_max_scenes` / `quota_max_total_secs` exceeded. |
| `ssrf_rejected` | 422 | External URL egress policy denied (loopback / private / non-allowlisted host). |
| `job_not_found` | 404 | `GET /api/v1/jobs/:id` lookup miss. |

Cross-client authorisation is intentionally SOFT for v1: any valid M2M bearer can `GET /api/v1/jobs/:id` for any `job_id`. The strict boundary (`creator_forwardings.external_client_id == m2m_client_id`) is a documented followup.

## Source-of-truth ladder

In priority order (top authority wins on divergence):

1. **Tested behavior** (final authority): `DataServer/internal/handlers/server/pipeline/job_submit_e2e_test.go` + `creator_push_e2e_test.go` (the latter covers the shared resolver path).
2. **OpenAPI 3.1.0 spec**: `DataServer/api/openapi.yaml` (operation `submitJob` + `getSubmittedJob`, schemas `SubmitJobRequest` / `SubmitScene` / `SubmitDeliveryPlanEntry` / `SubmitJobAcceptedResponse` / `SubmitJobStatusResponse`). CI-enforced by `scripts/api/validate_openapi.py` (correctness-only since [P1] codegen).
3. **M2M admin surface**: `DataServer/internal/handlers/server/api/admin_m2m_keys.go` (CRUD for `client_id` / `plaintext_secret` / scopes / quotas).
4. **M2M middleware**: `DataServer/internal/handlers/server/pipeline/m2m_auth.go` (Bearer → SHA-256 → key row → scope check → token bucket → audit row).
5. **This document** (narrative, treated as the operator onboarding reference).

## Operator runbook

### Smoke

```bash
# local dev
export VELOX_MASTER_URL=http://127.0.0.1:8080
export VELOX_ADMIN_TOKEN=<your admin token>
make jobs-smoke

# observational: pin the smoke to a specific idempotency key
export JOBS_IDEM_KEY=smoke-nightly-RFC-12345
make jobs-smoke
```

The script:
1. Resolves `$VELOX_ADMIN_TOKEN` (env > TOKEN_FILE dotenv).
2. Calls `POST /api/v1/admin/m2m/keys` to mint an ephemeral client (`client_id="smoke-cli-${EPOCHSECONDS}"`, scopes `["jobs.submit"]`).
3. Sends `POST /api/v1/jobs` carrying the **plaintext_secret** as bearer.
4. Polls `GET /api/v1/jobs/{job_id}` with exp backoff until `SUCCEEDED` / `FAILED` / `CANCELLED` (cap 60s).
5. `DELETE`s the ephemeral M2M client (cleanup, on every exit path).
6. Prints a banner with the resolved shape and exits 0 on terminal success.

### Production considerations

- **M2M credential storage**: never commit the `plaintext_secret`. Store in the operator's secret manager. The Velox master only retains the SHA-256.
- **Client_id naming**: avoid employee personal data, customer identifiers, or anything that would survive a credential leak as identity-correlated labels. IDs like `client-acme-batch-nightly` are durable without exposing user data.
- **Idempotency key derivation**: stable per logical call (e.g., content hash + day) so retries converge; never reuse across genuinely-different jobs.
- **Burst budget**: align `rate_limit_burst` with the operator's planned peak submission rate. Bursts above `quota_max_scenes × N` quickly trip `429 m2m_quota_exceeded` even if `rate_limit_rps` looks generous — tune both knobs together.
- **Audit log auditing**: `GET /api/v1/admin/m2m/audit?client_id=…&limit=…` returns the per-request audit rows. The audit row carries the SHA-256 hash prefix of the idempotency key (12 chars), not the raw key — log-grep correlation is preserved without re-identification risk.

---

See also:

- [docs/CREATOR-PUSH.md](./CREATOR-PUSH.md) — the operator-grade Creator sync push (`POST /api/v1/creator/jobs`, `VELOX_ADMIN_TOKEN`).
- [docs/architecture/current-architecture.md §12](./architecture/current-architecture.md) — two intake paths, one resolver.
- [DataServer/api/openapi.yaml](../../DataServer/api/openapi.yaml) — machine-readable contract.
- [scripts/api/jobs_smoke.sh](../../scripts/api/jobs_smoke.sh) — executable smoke.
