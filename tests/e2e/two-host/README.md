# Two-host Golden E2E

This harness certifies the real topology: the master stages inputs and owns
SQLite/storage on Computer A; the worker on Computer B receives the task,
downloads assets over HTTP, runs the C++ engine/FFmpeg, and uploads the result.

The scripts do not mount or inspect the master's filesystem on B. The worker
certificate filename may be either `<worker-id>.crt`/`.key` (the output of
`gen-production-pki.sh`) or `worker.crt`/`worker.key` via
`VELOX_WORKER_CERT_FILE` and `VELOX_WORKER_KEY_FILE`.

## Computer B

```bash
export MASTER_URL=http://velox-master.test:8180
export GRPC_URL=velox-master.test:51851
export VELOX_BUNDLE_HASH="$(scripts/e2e/write-local-bundle-identity.sh \
  /var/lib/velox/worker /opt/velox/bin/velox-worker-agent \
  /opt/velox/bin/velox_video_engine)"
tests/e2e/two-host/worker-run.sh
```

The worker must have only its leaf certificate, key, and intermediate CA. Do
not copy the root or intermediate private key to either runtime host.

## Computer A

Start `velox-server` with the production PKI and the usual `VELOX_*` paths,
then run:

```bash
export VELOX_ADMIN_TOKEN='test-token'
export VELOX_DB_PATH=/var/lib/velox/master/data/velox.db
export VELOX_STAGING_DIR=/var/lib/velox/master/staging
export VELOX_STORAGE_DIR=/var/lib/velox/master/storage
export MASTER_URL=http://127.0.0.1:8180
export WORKER_ID=worker-pc-b-01
GOLDEN_PROFILE=production-shaped tests/e2e/two-host/master-driver.sh
```

During the run, `pgrep -af 'velox_video_engine|ffmpeg'` must show the process
on B, while the final artifact and SQLite records remain on A. The driver uses
`scripts/e2e/verify-golden-job.sh`, also used by the single-host Golden E2E.
