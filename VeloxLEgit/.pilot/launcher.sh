#!/usr/bin/env bash
# Velox master pilot launcher. Subcommands: start | status | log | stop | submit | work
# Designed so the basher can invoke ONE short command per step instead of a
# compound shell that gets truncated by the sandbox.

set +e
PILOT=/tmp/velox-pilot
ENV=$PILOT/.env
LOG=$PILOT/master.log
PID=$PILOT/master.pid
DB=$PILOT/velox.db
BIN=/tmp/velox-server
PORT=8080
ADMIN=test-admin-token
WORKER=/home/pierone/Pyt/VeloxLEgit/RemoteCodex/native/worker-agent-go/bin/velox-worker-agent
ENGINE=/home/pierone/Pyt/VeloxLEgit/RemoteCodex/native/video-engine-cpp/build/velox_video_engine

start() {
  pkill -9 -f "$BIN serve" 2>/dev/null
  sleep 2
  rm -f "$LOG" "$PID" "$DB" "$PILOT/health.out" "$PILOT/version.out"
  if [ ! -f "$ENV" ]; then echo "NO_ENV"; return 1; fi
  set -a; . "$ENV"; set +a
  cd "$PILOT"
  setsid nohup "$BIN" serve </dev/null >"$LOG" 2>&1 &
  PIDV=$!
  echo "$PIDV" > "$PID"
  disown $PIDV 2>/dev/null
  echo "BOOT_PID=$PIDV"
  for i in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15; do
    if grep -qE 'Velox master listening|Background goroutines' "$LOG" 2>/dev/null; then echo "BOOT_OK_AT=${i}s"; return 0; fi
    if grep -qE 'panic:|^FATAL|^Error:' "$LOG" 2>/dev/null; then echo "BOOT_FAIL_AT=${i}s"; tail -40 "$LOG"; return 1; fi
    sleep 1
  done
  echo "BOOT_TIMEOUT"; tail -40 "$LOG"
}

status() {
  if [ ! -f "$PID" ]; then echo "NOT_RUNNING"; return 0; fi
  PV=$(cat "$PID")
  if ! ps -p "$PV" >/dev/null 2>&1; then echo "PROCESS_DEAD PID=$PV"; return 0; fi
  echo "PID=$PV"
  ps -p "$PV" -o pid,ppid,user,etime,stat,rss,comm 2>/dev/null
  echo --- LOG_TAIL ---
  tail -50 "$LOG" 2>&1
  echo --- MARKERS ---
  grep -nE 'panic|FATAL|listening on|Background goroutines|Server stopped' "$LOG" 2>/dev/null | head -10 || true
  echo --- HEALTH ---
  curl -sS -m 5 -o "$PILOT/health.out" -w 'HEALTH_HTTP=%{http_code}\n' http://127.0.0.1:$PORT/health 2>&1
  head -3 "$PILOT/health.out" 2>/dev/null
  echo --- VERSION ---
  curl -sS -m 5 -o "$PILOT/version.out" -w 'VERSION_HTTP=%{http_code}\n' http://127.0.0.1:$PORT/version 2>&1
  head -3 "$PILOT/version.out" 2>/dev/null
}

log() { tail -n 200 -F "$LOG"; }

stop() {
  if [ -f "$PID" ]; then
    PV=$(cat "$PID"); kill -TERM "$PV" 2>/dev/null && echo "TERM_SENT=$PV" || true
    sleep 2; kill -KILL "$PV" 2>/dev/null && echo "KILL_SENT" || true
  fi
  pkill -9 -f "$BIN serve" 2>/dev/null
  pkill -9 -f 'velox-worker-agent' 2>/dev/null
  echo "STOPPED"
}

submit() {
  mkdir -p "$PILOT"
  ffmpeg -hide_banner -loglevel error -y -f lavfi -i anullsrc=r=44100:cl=stereo -t 2 -c:a libmp3lame /tmp/vo.mp3 2>/dev/null || true
  ffmpeg -hide_banner -loglevel error -y -f lavfi -i color=c=teal:s=320x240:d=2 -frames:v 1 /tmp/scene1.png 2>/dev/null || true
  ls -la /tmp/vo.mp3 /tmp/scene1.png 2>/dev/null
  cat > "$PILOT/job.json" <<JSON
{
  "video_name": "VeloxPilot",
  "script_text": "Pilot smoke.",
  "scenes_json": "[{\"text\":\"Pilot\",\"image\":\"file:///tmp/scene1.png\"}]",
  "voiceover_path": "/tmp/vo.mp3",
  "render_video": true,
  "save_to_db": true,
  "channel_id": "pilot",
  "audio_language_for_srt": "en"
}
JSON
  curl -sS -m 10 -X POST \
    -H "Authorization: Bearer $ADMIN" \
    -H "Content-Type: application/json" \
    --data-binary @"$PILOT/job.json" \
    http://127.0.0.1:$PORT/api/v1/script/generate-with-images \
    -o "$PILOT/submit.out" -w 'SUBMIT_HTTP=%{http_code}\n' 2>&1
  head -80 "$PILOT/submit.out" 2>/dev/null
  echo --- JOBS_ROWS ---
  sqlite3 "$DB" "SELECT job_id, status, video_name, run_id, job_run_id, created_at, updated_at FROM jobs ORDER BY created_at DESC LIMIT 10;" 2>&1 | head -20
}

work() {
  WORKER_LOG=$PILOT/worker.log
  if [ ! -x "$WORKER" ]; then echo "WORKER_NOT_BUILT"; return 1; fi
  if [ ! -x "$ENGINE" ]; then echo "ENGINE_NOT_BUILT path=$ENGINE"; return 1; fi
  # Worker-side analog of master's VELOX_GRPC_ALLOW_INSECURE_DEV; pilot-scoped loopback only.
  # Transport factory enforces BOTH: JSON field `allow_insecure_grpc_dev` AND env VELOX_ALLOW_INSECURE_GRPC_DEV.
  # Defensive default `true` is acceptable only because this entire launcher file is pilot-only.
  # Pass the env explicitly via `env VAR=val setsid ...` so the var survives into the new session.
  WSEC="${VELOX_ALLOW_INSECURE_GRPC_DEV:-true}"
  cat > "$PILOT/worker.json" <<JSON
{
  "master_url": "http://127.0.0.1:$PORT",
  "worker_id": "pilot-worker-1",
  "work_dir": "$PILOT",
  "control_grpc_url": "http://127.0.0.1:50051",
  "job_delivery": "push",
  "allow_insecure_grpc_dev": true
}
JSON
  setsid nohup env "VELOX_ALLOW_INSECURE_GRPC_DEV=$WSEC" "$WORKER" -config "$PILOT/worker.json" </dev/null >"$WORKER_LOG" 2>&1 &
  WP=$!
  echo "$WP" > "$PILOT/worker.pid"
  disown $WP 2>/dev/null
  echo "WORKER_PID=$WP"
  sleep 10
  ps -p "$WP" -o pid,etime,stat,comm 2>/dev/null || echo "WORKER_DIED"
  echo --- WORKER_LOG ---
  tail -80 "$WORKER_LOG"
  echo --- JOBS_LIFECYCLE ---
  sqlite3 "$DB" "SELECT job_id, status, video_name, job_run_id, updated_at FROM jobs ORDER BY updated_at DESC LIMIT 5;" 2>&1 | head -10
}

case "${1:-}" in
  start)   start ;;
  status)  status ;;
  log)     log ;;
  stop)    stop ;;
  submit)  submit ;;
  work)    work ;;
  *) echo "usage: $0 {start|status|log|stop|submit|work}"; exit 2 ;;
esac
