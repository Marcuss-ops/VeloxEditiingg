#!/usr/bin/env bash
# Focused tests for apply-local-worker-config.sh rendering components.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
RENDERER="$ROOT/deploy/scripts/render-worker-config.py"
FINGERPRINT="$ROOT/deploy/scripts/worker-config-fingerprint.py"
WORK="$(mktemp -d /tmp/velox-worker-config-test.XXXXXX)"
trap 'rm -rf "$WORK"' EXIT

# The entrypoint provisions missing directories with chown. Stub only the
# privileged/system-dependent commands so the integration test is portable.
STUB_BIN="$WORK/bin"
mkdir -p "$STUB_BIN"
cat >"$STUB_BIN/chown" <<'SH'
#!/usr/bin/env bash
exit 0
SH
cat >"$STUB_BIN/docker" <<'SH'
#!/usr/bin/env bash
case "${1:-}" in
  image) exit 1 ;;
  *) exit 0 ;;
esac
SH
chmod +x "$STUB_BIN/chown" "$STUB_BIN/docker"

fail() { printf 'worker-config-components: FAIL: %s\n' "$*" >&2; exit 1; }
pass() { printf 'worker-config-components: %s\n' "$*"; }

cat >"$WORK/template.json" <<'JSON'
{
  "_comment": "operator documentation must not reach the worker",
  "worker_id": "CHANGE_ME_worker",
  "worker_name": "CHANGE_ME_name",
  "bundle_version": "",
  "bundle_hash": "",
  "image_digest": "",
  "log_level": "debug",
  "max_active_jobs": 4,
  "allow_insecure_grpc_dev": false
}
JSON

RENDERER_TOOL="$RENDERER" python3 - "$WORK/template.json" "$WORK/direct.json" <<'PY'
import importlib.util
import json
import os
import sys

spec = importlib.util.spec_from_file_location("render_worker_config", os.environ["RENDERER_TOOL"])
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
module.render(
    sys.argv[1], sys.argv[2], "direct-worker", "Direct Worker",
    "direct.example:7443", "http://direct.example:8000", "/tmp/direct-work",
    8083, "direct-v1", "", "", "", True,
)
with open(sys.argv[2]) as rendered_file:
    cfg = json.load(rendered_file)
assert cfg["worker_id"] == "direct-worker"
assert cfg["allow_insecure_grpc_dev"] is True
PY

python3 "$RENDERER" \
  "$WORK/template.json" "$WORK/rendered.json" \
  "worker-01" "Display Worker" \
  "master.example:7443" "http://master.example:8000" \
  "$WORK/work" "8082" "protocol-v1" \
  "bundle-1" "hash-1" "" "false"

python3 - "$WORK/rendered.json" <<'PY'
import json
import sys

with open(sys.argv[1]) as rendered_file:
    cfg = json.load(rendered_file)

assert all(not key.startswith("_") for key in cfg), cfg
assert cfg["worker_id"] == "worker-01"
assert cfg["worker_name"] == "Display Worker"
assert cfg["control_grpc_url"] == "master.example:7443"
assert cfg["master_url"] == "http://master.example:8000"
assert cfg["health_port"] == 8082
assert cfg["protocol_version"] == "protocol-v1"
assert cfg["bundle_version"] == "bundle-1"
assert cfg["bundle_hash"] == "hash-1"
assert cfg["allow_insecure_grpc_dev"] is False
assert cfg["max_active_jobs"] == 4
assert cfg["prometheus_port"] == 9090
assert cfg["log_level"] == "debug"
assert cfg["image_digest"] == ""
PY

printf 'compose-v1\n' >"$WORK/compose.yml"
fp1="$(python3 "$FINGERPRINT" "$WORK/rendered.json" "$WORK/compose.yml" '')"
fp_direct="$(FINGERPRINT_TOOL="$FINGERPRINT" python3 - "$WORK/rendered.json" "$WORK/compose.yml" <<'PY'
import importlib.util
import os
import sys

spec = importlib.util.spec_from_file_location("worker_config_fingerprint", os.environ["FINGERPRINT_TOOL"])
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
print(module.fingerprint(sys.argv[1], sys.argv[2], ""))
PY
)"
[[ "$fp1" == "$fp_direct" ]] || fail 'CLI and direct fingerprint implementations differ'
fp2="$(python3 "$FINGERPRINT" "$WORK/rendered.json" "$WORK/compose.yml" '')"
[[ "$fp1" == "$fp2" ]] || fail 'fingerprint is not deterministic'
[[ "$fp1" =~ ^[[:xdigit:]]{64}$ ]] || fail 'fingerprint is not a SHA-256 hex digest'

printf 'compose-v2\n' >"$WORK/compose.yml"
fp3="$(python3 "$FINGERPRINT" "$WORK/rendered.json" "$WORK/compose.yml" '')"
[[ "$fp1" != "$fp3" ]] || fail 'compose changes do not affect fingerprint'
fp4="$(python3 "$FINGERPRINT" "$WORK/rendered.json" "$WORK/compose.yml" 'image@sha256:abc')"
[[ "$fp3" != "$fp4" ]] || fail 'image digest changes do not affect fingerprint'

img='ghcr.io/example/worker@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef'
mkdir -p "$WORK/entrypoint"
PATH="$STUB_BIN:$PATH" VELOX_STATE_DIR="$WORK/entrypoint/state" \
  bash "$ROOT/deploy/scripts/apply-local-worker-config.sh" \
  --worker-id worker-smoke --control-grpc-url master.example:7443 --image "$img" \
  --src "$WORK/template.json" --compose-file "$WORK/compose.yml" \
  --skip-validate-config --skip-compose-check >/dev/null
python3 - "$WORK/entrypoint/state/worker_config.json" <<'PY'
import json
import sys

with open(sys.argv[1]) as rendered_file:
    cfg = json.load(rendered_file)
assert cfg["worker_id"] == "worker-smoke"
assert cfg["control_grpc_url"] == "master.example:7443"
PY
entrypoint_output="$(PATH="$STUB_BIN:$PATH" VELOX_STATE_DIR="$WORK/entrypoint/state" \
  bash "$ROOT/deploy/scripts/apply-local-worker-config.sh" \
  --worker-id worker-smoke --control-grpc-url master.example:7443 --image "$img" \
  --src "$WORK/template.json" --compose-file "$WORK/compose.yml" \
  --skip-validate-config --skip-compose-check 2>&1 >/dev/null)"
grep -q 'no-op: deployment_fingerprint and JSON hash unchanged' <<<"$entrypoint_output" \
  || fail 'entrypoint did not take its no-op path on identical input'

pass 'renderer, fingerprint, and entrypoint checks passed'
