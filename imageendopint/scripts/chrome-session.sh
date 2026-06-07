#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

CHROME_BIN="${CHROME_BIN:-/opt/google/chrome/google-chrome}"
SESSION_ROOT="${SESSION_ROOT:-$ROOT_DIR/.chrome-session}"
PROFILE_DIR="${PROFILE_DIR:-$SESSION_ROOT/profile}"
STATE_DIR="${STATE_DIR:-$SESSION_ROOT/state}"
SAVE_ROOT="${SAVE_ROOT:-$SESSION_ROOT/saved-sessions}"
PORT="${PORT:-9222}"
SOURCE_PROFILE_DIR="${SOURCE_PROFILE_DIR:-$HOME/.config/google-chrome}"

PID_FILE="$STATE_DIR/chrome.pid"
LOG_FILE="$STATE_DIR/chrome.log"
META_FILE="$STATE_DIR/session.env"

mkdir -p "$SESSION_ROOT" "$STATE_DIR" "$SAVE_ROOT"

copy_source_profile() {
  local source="$1"
  local target="$2"

  if [[ -d "$target/Default" || -d "$target/AutomationProfile" ]]; then
    return 0
  fi

  mkdir -p "$target"
  rsync -a \
    --exclude 'SingletonCookie' \
    --exclude 'SingletonLock' \
    --exclude 'SingletonSocket' \
    --exclude 'CrashpadMetricsActive.pma' \
    "$source/" "$target/"
}

start_chrome() {
  local open_url="${1:-${TARGET_URL:-}}"
  copy_source_profile "$SOURCE_PROFILE_DIR" "$PROFILE_DIR"

  cat >"$META_FILE" <<EOF
CHROME_BIN=$CHROME_BIN
SESSION_ROOT=$SESSION_ROOT
PROFILE_DIR=$PROFILE_DIR
SAVE_ROOT=$SAVE_ROOT
PORT=$PORT
SOURCE_PROFILE_DIR=$SOURCE_PROFILE_DIR
EOF

  if [[ -f "$PID_FILE" ]] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
    echo "Chrome session already running: $(cat "$PID_FILE")"
    echo "CDP URL: http://127.0.0.1:$PORT"
    exit 0
  fi

  local chrome_args=(
    --remote-debugging-port="$PORT"
    --user-data-dir="$PROFILE_DIR"
    --profile-directory=Default
    --ozone-platform=x11
    --new-window
    --start-maximized
  )

  if [[ -n "$open_url" ]]; then
    chrome_args+=("$open_url")
  fi

  setsid "$CHROME_BIN" "${chrome_args[@]}" \
    >/dev/null 2>>"$LOG_FILE" </dev/null &

  echo $! >"$PID_FILE"
  echo "Started Chrome session pid $(cat "$PID_FILE")"
  echo "CDP URL: http://127.0.0.1:$PORT"
}

save_session() {
  local stamp
  stamp="$(date +%Y%m%d-%H%M%S)"
  local backup_dir="$SAVE_ROOT/$stamp"
  mkdir -p "$backup_dir"

  if [[ -d "$PROFILE_DIR" ]]; then
    rsync -a --delete \
      --exclude 'SingletonCookie' \
      --exclude 'SingletonLock' \
      --exclude 'SingletonSocket' \
      --exclude 'CrashpadMetricsActive.pma' \
      "$PROFILE_DIR/" "$backup_dir/profile/"
  fi

  cp -a "$META_FILE" "$backup_dir/session.env" 2>/dev/null || true
  echo "$backup_dir"
}

close_chrome() {
  if [[ -f "$PID_FILE" ]]; then
    local pid
    pid="$(cat "$PID_FILE")"
    if kill -0 "$pid" 2>/dev/null; then
      kill "$pid" || true
      for _ in {1..30}; do
        if ! kill -0 "$pid" 2>/dev/null; then
          break
        fi
        sleep 1
      done
      if kill -0 "$pid" 2>/dev/null; then
        kill -9 "$pid" || true
      fi
    fi
    rm -f "$PID_FILE"
  fi

  local saved_path
  saved_path="$(save_session)"
  echo "Saved session to $saved_path"
}

case "${1:-}" in
  start)
    start_chrome "${2:-}"
    ;;
  close)
    close_chrome
    ;;
  status)
    if [[ -f "$PID_FILE" ]] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
      echo "running $(cat "$PID_FILE")"
    else
      echo "stopped"
    fi
    ;;
  *)
    echo "Usage: $0 {start|close|status}"
    exit 1
    ;;
esac
