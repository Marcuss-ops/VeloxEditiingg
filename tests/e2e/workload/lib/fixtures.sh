# shellcheck shell=bash
# fixtures.sh — Phase 2: generate deterministic test fixtures.

phase_fixtures() {
  info "Phase 2: generating test fixtures"
  mkdir -p "$FIXTURE_DIR"

  command -v ffmpeg >/dev/null 2>&1 || { fail "ffmpeg not found — install ffmpeg"; exit 3; }

  # Scene image: pure teal (#008080), 1920x1080, 1 frame PNG.
  # Matches the engine's default canvas so the rendered output is
  # native rather than upscaled from a smaller source.
  info "  → scene.png (teal 1920x1080)"
  ffmpeg -hide_banner -loglevel error -y \
    -f lavfi -i "color=c=0x008080:s=1920x1080:d=0.1" -frames:v 1 \
    -vcodec png "$FIXTURE_DIR/scene.png" 2>/dev/null || {
    fail "scene.png generation failed"; exit 3; }

  # Silent audio: 2 seconds, AAC inside an MP4 container named .mp4. The
  # MP4 container is REQUIRED: inputsecurity sniffs the file header, and
  # raw ADTS AAC (a bare .aac) is not sniffable by http.DetectContentType
  # (rejected with INPUT_MIME_UNSUPPORTED). The extension must ALSO be .mp4:
  # the sniffed MIME for an MP4 container is video/mp4, and an .m4a name
  # declares audio/mp4 → INPUT_MIME_MISMATCH. video/mp4 is accepted for the
  # voiceover/audio role by allowedMIME, and ffprobe validates the stream.
  info "  → silent.mp4 (2s, AAC in MP4 container)"
  ffmpeg -hide_banner -loglevel error -y \
    -f lavfi -i "anullsrc=r=48000:cl=mono" -t 2 \
    -c:a aac -b:a 64k "$FIXTURE_DIR/silent.mp4" 2>/dev/null || {
    # Try MP3 fallback (ID3 tag → audio/mpeg, also accepted)
    ffmpeg -hide_banner -loglevel error -y \
      -f lavfi -i "anullsrc=r=44100:cl=mono" -t 2 \
      -c:a libmp3lame -b:a 64k "$FIXTURE_DIR/silent.mp3" 2>/dev/null || {
      fail "audio fixture generation failed"; exit 3; }
    info "  → silent.mp3 (2s, MP3 fallback)"
  }

  local scene_path="$FIXTURE_DIR/scene.png"
  local audio_path="$FIXTURE_DIR/silent.mp4"
  [[ -f "$audio_path" ]] || audio_path="$FIXTURE_DIR/silent.mp3"

  ls -la "$scene_path" "$audio_path" 2>/dev/null
  pass "fixtures ready"
}
