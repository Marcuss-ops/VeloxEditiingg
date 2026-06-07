#!/bin/bash

# Define project root and environment
PROJECT_ROOT="/home/pierone/Pyt/imageendopint"
export PYTHONPATH="$PROJECT_ROOT:$PYTHONPATH"
export HEADLESS=true
export DEBUG_SCREENSHOTS=true
export STORAGE_STATE_PATH="$PROJECT_ROOT/outputs/flow-storage-state.json"

LOG_DIR="$PROJECT_ROOT/outputs/Log"
mkdir -p "$LOG_DIR"

echo "[STARTUP] Cleaning up existing processes..."
fuser -k 8001/tcp 2>/dev/null
pkill -f "python -m app.worker" 2>/dev/null
pkill -f "python scripts/test_webhook_listener.py" 2>/dev/null
sleep 2

echo "[STARTUP] Starting Webhook Listener on port 9000..."
nohup python "$PROJECT_ROOT/scripts/test_webhook_listener.py" > "$LOG_DIR/webhook.log" 2>&1 &

echo "[STARTUP] Starting API Server on port 8001..."
nohup uvicorn app.main:app --host 0.0.0.0 --port 8001 > "$LOG_DIR/api.log" 2>&1 &

echo "[STARTUP] Starting Image Generation Worker..."
nohup python -m app.worker > "$LOG_DIR/worker.log" 2>&1 &

echo "[STARTUP] Waiting for services to stabilize..."
sleep 5

# Final check
if curl -s http://127.0.0.1:8001/health | grep -q "ok"; then
    echo "[SUCCESS] Image Endpoint is UP and RUNNING on port 8001"
    echo "Logs are available in outputs/*.log"
else
    echo "[ERROR] Failed to start services. Check outputs/*.log"
    exit 1
fi
