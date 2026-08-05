#!/usr/bin/env bash
# probe-worker-facts.sh — streamed over SSH to a worker host (bash -s).
# Collects docker image/container facts + engine binary facts.
# No secrets are emitted.
set -u
CONTAINER="${1:-velox-worker}"

echo "--- images ---"
sudo -n docker images --digests 2>/dev/null | grep -i 'velox-worker' | head -8
echo "--- container ---"
sudo -n docker ps -a --format '{{.Names}} | {{.Image}} | {{.ImageID}} | {{.Status}}' 2>/dev/null | grep -i 'velox' | head -8
echo "--- engine + version inside container ---"
sudo -n docker exec "$CONTAINER" sh -c 'ls -la /usr/local/bin/velox_video_engine 2>/dev/null; sha256sum /usr/local/bin/velox_video_engine 2>/dev/null; echo VERSION=$(cat /app/VERSION.txt 2>/dev/null); echo BUNDLE=$(cat /app/RemoteCodex/BUNDLE_HASH.txt 2>/dev/null)' 2>&1 | head -8
echo "--- workdir bundle ---"
f=/var/lib/velox-worker/work/BUNDLE_HASH.txt
[ -f "$f" ] && echo "$f => $(cat "$f" | tr -d '[:space:]' | cut -c1-40)"
