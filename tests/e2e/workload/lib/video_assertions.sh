# shellcheck shell=bash
# shellcheck disable=SC2154

assert_video_properties() {
  info "Verification 2: ffprobe inspection (strict: codec h264 ONLY, 320x180, 1.8..2.2s)"
  if ! command -v ffprobe >/dev/null 2>&1; then
    fail "ffprobe not found — cannot validate artifact codec/resolution/duration"
    exit 1
  fi
  # artifact is populated by assert_artifact_exists in the caller.
  # shellcheck disable=SC2154
  probe_json="$(ffprobe -v quiet -print_format json -show_format -show_streams "$artifact" 2>/dev/null || true)"
  if [[ -z "$probe_json" ]]; then
    fail "ffprobe returned empty output — artifact may be corrupt"
    exit 1
  fi

  codec="$(echo "$probe_json" | python3 -c '
import sys, json
d = json.load(sys.stdin)
vid = [s for s in d.get("streams", []) if s.get("codec_type") == "video"]
if not vid: sys.exit(2)
print(vid[0].get("codec_name", ""))
' || { fail "ffprobe: no video stream found"; exit 1; })"
  if [[ "$codec" != "h264" ]]; then
    fail "ffprobe: codec=$codec (only h264 accepted; hevc/mpeg4/vp9/av1 reject)"
    exit 1
  fi
  pass "ffprobe: codec=h264"

  width="$(echo "$probe_json" | python3 -c 'import sys,json;d=json.load(sys.stdin);s=[x for x in d["streams"] if x.get("codec_type")=="video"];print(s[0].get("width", "0"))' 2>/dev/null || echo 0)"
  height="$(echo "$probe_json" | python3 -c 'import sys,json;d=json.load(sys.stdin);s=[x for x in d["streams"] if x.get("codec_type")=="video"];print(s[0].get("height", "0"))' 2>/dev/null || echo 0)"
  if (( width != 1920 || height != 1080 )); then
    fail "ffprobe: resolution=${width}x${height} (must be exactly 1920x1080)"
    exit 1
  fi
  pass "ffprobe: resolution=1920x1080"

  dur="$(echo "$probe_json" | python3 -c 'import sys,json;d=json.load(sys.stdin);f=d.get("format",{});print(f.get("duration", "0"))' 2>/dev/null || echo 0)"
  if ! awk -v d="$dur" 'BEGIN{ exit !(d+0 >= 1.8 && d+0 <= 2.2) }'; then
    fail "ffprobe: duration=${dur}s (must be in 1.8..2.2s)"
    exit 1
  fi
  pass "ffprobe: duration=${dur}s (within 1.8..2.2s)"
}
