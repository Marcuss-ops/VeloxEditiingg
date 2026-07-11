# Cap. 3 evidence pack — operator runbook

This document freezes the canonical evidence-file shape that the Phase 2
certifier family (`scripts/cert/certify-worker-*.sh`) emits. The cap. 11
release gate reads exactly these names — DO NOT rename without updating
both the certifier and the gate consumer.

## Per-worker evidence directory

Every per-worker certifier run writes under:

```
$HOME/evidence/<YYYY-MM-DD>/<worker_id>/
```

`<YYYY-MM-DD>` is the date the certifier was *invoked* (UTC). `<worker_id>`
is the canonical worker name — usually `velox-worker-N`, but any string
the operator chooses works as long as it is consistent with the cert CN
(master-side hard check in `DataServer/internal/grpcserver/handler.go`
~line 766).

## Phase 2A+2B (host + deploy) — `certify-worker-2a-2b.sh`

```
host.json                       # 9 host check payload + PASS/FAIL map
host.txt                        # human-readable summary of host.json
image-digest.txt                # pinned digest (sha256:abc...) — never :latest
image-signature.txt             # cosign verify stdout (when cosign installed)
worker-config.sha256            # sha256 of the worker_config.json on the VPS
certificate.txt                # `openssl x509 -text` dump of the worker cert
verdict.json                    # combined 2A+2B verdict, schema=velox.phase-2a-2b.v1
```

Each host check (cpu_arch, ram, disk, fs_write, ntp, dns, connectivity,
docker, restart_loop) is one row in `host.json` with `{name, status,
detail}`. Each deploy check (digest_pin, container_running, config_mount,
cert_readable, uid_gid, persistent_dirs, health_endpoint) follows the
same row shape.

## Phase 2C+2D (real bootstrap + mTLS handshake) — `certify-worker-2c-2d.sh`

```
bootstrap-report.json           # [BOOTSTRAP_REPORT] JSON, parsed from the worker's stderr
worker.log                      # canonical: 2C container stdout+stderr + 2D handshake stream
master-handshake.log            # dev-hello-client's per-shake stdout+stderr
cert-static.json                # 2D-1 static cert check payload (CN==worker_id, chain, expiry, EKU)
master-state.json               # 2D-3 curl /api/v1/workers payload, raw JSON
handshake-worker-stdout.log     # dev-hello-client's stdout (folded into master-handshake.log + worker.log)
handshake-worker-stderr.log     # dev-hello-client's stderr (folded into master-handshake.log + worker.log)
dump.txt                        # real-bootstrap.sh's debug dump (folded into worker.log on 2C FAIL)
dev-hello-client                # locally-compiled dev-hello-client binary, kept for post-mortem
dev-hello-build.log             # stderr of the local go build
phase-2d-3-master-state.err     # curl stderr (when REST probe fails)
verdict-2c-2d.json              # combined 2C+2D verdict, schema=velox.phase-2c-2d.v1
```

### Canonical schema for `verdict-2c-2d.json`

```json
{
  "schema":               "velox.phase-2c-2d.v1",
  "worker_id":            "<worker_id>",
  "cert_date":            "<YYYY-MM-DD>",
  "worker_image":         "ghcr.io/<owner>/velox-worker@sha256:<64hex>",
  "expected_bundle_hash": "<64hex>",
  "protocol_version":     "v3",
  "phase_status": {
    "2c_bootstrap":         { "status": "PASS|FAIL|SKIP", "detail": "..." },
    "2d_static_cert":       { "status": "PASS|FAIL|SKIP", "detail": "..." },
    "2d_dynamic_handshake": { "status": "PASS|FAIL|SKIP", "detail": "..." },
    "2d_master_state":      { "status": "PASS|FAIL|SKIP", "detail": "..." }
  },
  "overall_verdict":      "CERTIFIED|PARTIAL|FAIL",
  "any_fail":             true|false,
  "any_skip":             true|false,
  "required_passes":      true|false,
  "evidence_dir":         "/home/<user>/evidence/<YYYY-MM-DD>/<worker_id>/",
  "generated_at_utc":     "<iso8601>"
}
```

`overall_verdict == CERTIFIED` requires:

- `2c_bootstrap == PASS` (real-bootstrap run, verdict=OK, 4 step PASS)
- `2d_static_cert == PASS` (CN == worker_id, CA chain valid, not-yet-expired, EKU clientAuth)
- `2d_dynamic_handshake == PASS` (dev-hello-client HelloAck received within timeout)
  — UNLESS `MASTER_URL` was not set AND operator passed `--allow-skip-dynamic`
- `2d_master_state == PASS` (curl `/api/v1/workers` shows the worker in CONNECTED state)
  — UNLESS `MASTER_RESTSERVER` was not set

`PARTIAL` is reserved for diagnostic runs where some non-required phase
is SKIPPED but the required phases all passed. `FAIL` is the
fail-closed verdict for any required phase that did not PASS.

## Exit-code map

The certifier exits non-zero on FAIL so the cap. 11 collector can
short-circuit; the per-phase exit code is also encoded so cap. 11 can
route the failure to the responsible sub-check:

| Exit | Phase that failed                                              |
| :--- | :------------------------------------------------------------- |
| 0    | CERTIFIED or PARTIAL (PARTIAL is informational, see verdict)  |
| 1    | 2C bootstrap fail                                              |
| 2    | 2D-1 static cert fail (or preflight sanity fail)               |
| 3    | 2D-2 dynamic handshake fail                                    |
| 4    | 2D-3 master-state probe fail (REST)                            |

## Mandatory inputs

The certifier refuses to proceed (exit 2) without any of these:

```text
WORKER_ID                       # == cert CN, must match across all surfaces
WORKER_IMAGE                    # ghcr.io/<owner>/velox-worker@sha256:<64hex>  (refuses :latest)
EXPECTED_BUNDLE_HASH            # 64 lowercase hex (sha256 of the published BUNDLE_HASH.txt)
WORKER_CERT_FILE                # worker.pem on the VPS (~/.config/velox/certs/worker.pem)
WORKER_KEY_FILE                 # worker.key on the VPS
WORKER_CA_FILE                  # cluster ca.crt on the VPS
```

Optional inputs (if absent, the corresponding sub-phase is SKIPPED and
overall becomes CERTIFIED only with `--allow-skip-dynamic`):

```text
MASTER_URL                      # host:port of master gRPC endpoint (velox.example.com:50051)
MASTER_RESTSERVER               # https URL base for /api/v1/workers (velox.example.com)
PROTOCOL_VERSION                # default: v3
HANDSHAKE_TIMEOUT_S             # default: 20; floor: 15 (dev-hello-client's hardcoded HelloAckTimeout)
EXPECTED_MAX_CONCURRENCY        # optional cross-check vs /api/v1/workers.max_parallel_jobs
```

## Reference runbook

```bash
# 1. Make sure the worker is deployed and the cert dir is mounted.
ssh velox-worker-01 "ls -la /run/velox/certs/"
# 2. Run the certifier.
ssh velox-worker-01 "\
  sudo WORKER_ID=velox-worker-01 \
       WORKER_IMAGE=ghcr.io/marcuss-ops/velox-worker@sha256:abc... \
       EXPECTED_BUNDLE_HASH=$(cat /opt/velox/BUNDLE_HASH.txt) \
       WORKER_CERT_FILE=/run/velox/certs/worker.pem \
       WORKER_KEY_FILE=/run/velox/certs/worker.key \
       WORKER_CA_FILE=/run/velox/certs/ca.crt \
       MASTER_URL=velox-master.internal:50051 \
       MASTER_RESTSERVER=https://velox-master.internal \
       EXPECTED_MAX_CONCURRENCY=2 \
       make certify-worker-bootstrap-mtls"
# 3. Inspect the verdict + evidence bundle.
ssh velox-worker-01 "cat /home/velox/evidence/<date>/velox-worker-01/verdict-2c-2d.json"
scp -r velox-worker-01:/home/velox/evidence/<date>/velox-worker-01/ evidence/velox-worker-01/

# A diagnostic run when no master is reachable yet:
sudo WORKER_ID=velox-worker-01 \
     WORKER_IMAGE=... \
     EXPECTED_BUNDLE_HASH=... \
     WORKER_CERT_FILE=... WORKER_KEY_FILE=... WORKER_CA_FILE=... \
     make certify-worker-bootstrap-mtls -- --allow-skip-dynamic
```

## Why the names are what they are

- `bootstrap-report.json`: cap. 4/job-state.md requires this exact name
  because the release gate parses it to assert `verdict=OK`.
- `worker.log`: cap. 11 evidence pack layout collector expects one
  canonical worker-side log per worker per day. May be 0 bytes on
  2C FAIL pre-docker; in that case `dump.txt` is folded into it.
- `master-handshake.log`: cap. 3 §D calls for "the master-side slice
  over the handshake window". Real-master integration is expected to
  add a tail of the master's own log via a future logslice tool; the
  current shape is the worker's view of that window.
- `cert-static.json`: per-worker cert attestation. Sum of all live
  workers' `cert-static.json` is the audit-evidence pack for chapter 8
  ("Certificati tutti gli executor").
- `master-state.json`: raw payload from the master's REST surface —
  kept verbatim so downstream dashboards can re-derive CONNECTED-state
  charts without re-querying the master.
