#!/bin/bash
# Velox Worker Agent - Deploy Script
#
# Automatizza il deploy del worker agent su tutti gli host remoti:
#   1. Build del binario Go (make agent)
#   2. Build dell'immagine Docker locale
#   3. Salvataggio e compressione dell'immagine
#   4. Copia su ogni host remoto via SCP
#   5. Caricamento dell'immagine Docker su ogni host
#   6. Rimozione del vecchio container e avvio del nuovo
#
# Usage:
#   ./deploy/deploy-worker.sh [options]
#
# Options:
#   --hosts HOST1,HOST2,...    Deploy solo su specifici host (default: tutti dall'inventory)
#   --inventory PATH           Path al file JSON dell'inventory (default: auto-cerca)
#   --image-tag TAG            Tag dell'immagine Docker (default: velox-worker:latest)
#   --master-url URL           Master server URL per i worker (default: http://51.91.11.36:8000)
#   --ssh-user USER            SSH username (default: pierone)
#   --work-dir PATH            Work directory remota (default: /opt/velox/current/refactored)
#   --docker-context PATH      Build context Docker (default: .)
#   --binary-path PATH         Path al binario precompilato (default: bin/velox-worker-agent)
#   --skip-build               Salta make agent e docker build
#   --skip-agent               Salta solo make agent (usa bin/ esistente)
#   --dry-run                  Mostra i comandi senza eseguirli
#   --help                     Mostra questo aiuto
#
# Examples:
#   ./deploy/deploy-worker.sh                              # Deploy su tutti gli host
#   ./deploy/deploy-worker.sh --hosts 51.222.204.158,149.56.131.97  # Solo specifici host
#   ./deploy/deploy-worker.sh --skip-build --dry-run       # Solo preview
#   ./deploy/deploy-worker.sh --hosts 57.129.132.133       # Singolo host

set -euo pipefail

# ─── Configurazione ──────────────────────────────────────────────────────────

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

# Defaults
DEFAULT_IMAGE_TAG="velox-worker:latest"
DEFAULT_MASTER_URL="http://51.91.11.36:8000"
DEFAULT_SSH_USER="pierone"
DEFAULT_WORK_DIR="/opt/velox/current"
DEFAULT_BINARY_PATH="bin/velox-worker-agent"
DEFAULT_DOCKER_CONTEXT="$SCRIPT_DIR"

IMAGE_TAG="$DEFAULT_IMAGE_TAG"
MASTER_URL="$DEFAULT_MASTER_URL"
SSH_USER="$DEFAULT_SSH_USER"
WORK_DIR="$DEFAULT_WORK_DIR"
BINARY_PATH="$DEFAULT_BINARY_PATH"
DOCKER_CONTEXT="$DEFAULT_DOCKER_CONTEXT"
SKIP_BUILD=false
SKIP_AGENT=false
DRY_RUN=false
HOSTS_FILTER=""

# Inventory auto-discovery: cerca ansible_computers.json in percorsi noti
INVENTORY_FILE=""
CANDIDATES=(
    "$PROJECT_ROOT/DataServer/data/ansible/ansible_computers.json"
    "$PROJECT_ROOT/data/ansible/ansible_computers.json"
    "$SCRIPT_DIR/../../DataServer/data/ansible/ansible_computers.json"
)
for c in "${CANDIDATES[@]}"; do
    if [[ -f "$c" ]]; then
        INVENTORY_FILE="$c"
        break
    fi
done

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

# ─── Parse arguments ─────────────────────────────────────────────────────────

while [[ $# -gt 0 ]]; do
    case $1 in
        --hosts)
            HOSTS_FILTER="$2"; shift 2 ;;
        --inventory)
            INVENTORY_FILE="$2"; shift 2 ;;
        --image-tag)
            IMAGE_TAG="$2"; shift 2 ;;
        --master-url)
            MASTER_URL="$2"; shift 2 ;;
        --ssh-user)
            SSH_USER="$2"; shift 2 ;;
        --work-dir)
            WORK_DIR="$2"; shift 2 ;;
        --docker-context)
            DOCKER_CONTEXT="$2"; shift 2 ;;
        --binary-path)
            BINARY_PATH="$2"; shift 2 ;;
        --skip-build)
            SKIP_BUILD=true; shift ;;
        --skip-agent)
            SKIP_AGENT=true; shift ;;
        --dry-run)
            DRY_RUN=true; shift ;;
        --help)
            sed -n '/^# Usage:/,/^$/p' "$0" | sed 's/^# //'
            exit 0 ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            echo "Usage: $0 [options] (see --help)"
            exit 1 ;;
    esac
done

# ─── Funzioni di utilità ────────────────────────────────────────────────────

log_info()  { echo -e "${GREEN}[INFO]${NC}  $*" >&2; }
log_step()  { echo -e "\n${BLUE}━━━ $* ━━━${NC}" >&2; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC}  $*" >&2; }
log_error() { echo -e "${RED}[ERROR]${NC} $*" >&2; }

run_cmd() {
    if $DRY_RUN; then
        echo -e "${YELLOW}[DRY-RUN]${NC} $*" >&2
    else
        echo -e "${CYAN}[EXEC]${NC} $*" >&2
        "$@"
    fi
}

ssh_cmd() {
    local host=$1; shift
    local password=$1; shift
    if [[ -n "$password" ]]; then
        run_cmd sshpass -p "$password" ssh -o StrictHostKeyChecking=no \
            -o ConnectTimeout=10 "$SSH_USER@$host" "$@"
    else
        run_cmd ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 \
            "$SSH_USER@$host" "$@"
    fi
}

scp_cmd() {
    local src=$1; shift
    local dst=$1; shift
    local password=$1; shift
    if [[ -n "$password" ]]; then
        run_cmd sshpass -p "$password" scp -o StrictHostKeyChecking=no "$src" "$dst"
    else
        run_cmd scp "$src" "$dst"
    fi
}


# ─── Inventory parsing ──────────────────────────────────────────────────────

declare -A HOST_PASSWORDS
declare -A HOST_WORKER_IDS
HOST_LIST=()

load_inventory() {
    if [[ -z "$INVENTORY_FILE" ]]; then
        log_warn "Inventory file not found. Use --inventory or place ansible_computers.json in DataServer/data/ansible/"
        log_warn "Falling back to manual host list. Set --hosts or edit this script."
        return 1
    fi

    log_info "Loading inventory from: $INVENTORY_FILE"
    local raw
    raw=$(python3 -c "
import json, sys
with open('$INVENTORY_FILE') as f:
    data = json.load(f)
for key, val in data.items():
    host = val.get('host', key.replace('host_', ''))
    user = val.get('ansible_user', 'pierone')
    password = val.get('ssh_password', '')
    # worker_id: 'host_' prefisso + host con dots -> underscores
    # Es: 149.56.131.97 -> host_149_56_131_97
    # Es: host_57.129.132.133 -> host_host_57_129_132_133
    worker_id = key
    if not worker_id.startswith('host_'):
        worker_id = f'host_{host}'
    worker_id = worker_id.replace('.', '_')
    print(f'{host}|{password}|{worker_id}')
")

    while IFS='|' read -r host password worker_id; do
        HOST_LIST+=("$host")
        HOST_PASSWORDS["$host"]="$password"
        HOST_WORKER_IDS["$host"]="$worker_id"
    done <<< "$raw"

    log_info "Found ${#HOST_LIST[@]} host(s) in inventory"
}

# ─── Docker run command derivation ──────────────────────────────────────────

gen_docker_run_cmd() {
    local worker_id=$1
    local container_name="velox-worker-$worker_id"
    local source_dir="$WORK_DIR"
    local cache_dir="$(dirname "$WORK_DIR")/cache"
    local assets_cache="$WORK_DIR/RemoteCodex/assets_cache"

    # Comando di avvio per il worker (cmake + binario)
    local entrypoint_cmd="ENGINE_BIN=/usr/local/bin/velox_video_engine; "
    entrypoint_cmd+="ENGINE_SRC=/app/RemoteCodex/native/video-engine-cpp; "
    entrypoint_cmd+="if [ ! -x \"\$ENGINE_BIN\" ] || ! ldd \"\$ENGINE_BIN\" >/tmp/velox-worker/engine-ldd.log 2>&1; then "
    entrypoint_cmd+="rm -f \"\$ENGINE_BIN\" && "
    entrypoint_cmd+="mkdir -p \"\$ENGINE_SRC\"/build /tmp/velox-worker && "
    entrypoint_cmd+="cmake -S \"\$ENGINE_SRC\" -B \"\$ENGINE_SRC\"/build && "
    entrypoint_cmd+="cmake --build \"\$ENGINE_SRC\"/build -j\"\$(nproc)\"; "
    entrypoint_cmd+="fi; "
    entrypoint_cmd+="export VELOX_VIDEO_ENGINE_CPP_BIN=\"\$ENGINE_BIN\"; "
    entrypoint_cmd+="CFG=/tmp/velox-worker/worker_config.json; "
    entrypoint_cmd+="mkdir -p /tmp/velox-worker; "
    entrypoint_cmd+="[ -f \"\$CFG\" ] || /usr/local/bin/velox-worker-agent -generate-config -config \"\$CFG\" -work-dir /app/RemoteCodex; "
    entrypoint_cmd+="exec /usr/local/bin/velox-worker-agent -config \"\$CFG\" -master $MASTER_URL -worker-id $worker_id -work-dir /app/RemoteCodex"

    echo "docker run -d \\
  --name $container_name \\
  --rm \\
  --network host \\
  -v $source_dir:/app \\
  -v $cache_dir:/root/.cache \\
  -v $assets_cache:/app/RemoteCodex/assets_cache \\
  --entrypoint /bin/sh \\
  $IMAGE_TAG \\
  -lc '$entrypoint_cmd'"
}

# ─── Step 1: Build Go binary ────────────────────────────────────────────────

build_agent() {
    if $SKIP_AGENT; then
        log_info "Skipping make agent (--skip-agent)"
        return 0
    fi
    log_step "Step 1/6: Building Go binary (make agent)"
    cd "$SCRIPT_DIR"
    if $DRY_RUN; then
        log_info "[DRY-RUN] Would run: make agent"
    else
        make agent
        log_info "Binary built: $SCRIPT_DIR/$BINARY_PATH"
    fi
}

# ─── Step 2: Build Docker image ─────────────────────────────────────────────

build_docker() {
    if $SKIP_BUILD; then
        log_info "Skipping Docker build (--skip-build)"
        return 0
    fi
    log_step "Step 2/6: Building Docker image $IMAGE_TAG"

    # Verifica che il binario esista prima del docker build
    if [[ ! -f "$DOCKER_CONTEXT/$BINARY_PATH" ]]; then
        log_error "Binary not found at $DOCKER_CONTEXT/$BINARY_PATH. Run 'make agent' first or use --skip-agent."
        exit 1
    fi

    if $DRY_RUN; then
        log_info "[DRY-RUN] Would run: docker build -t $IMAGE_TAG $DOCKER_CONTEXT"
    else
        docker build -t "$IMAGE_TAG" "$DOCKER_CONTEXT"
        log_info "Docker image built: $IMAGE_TAG"
    fi
}

# ─── Step 3: Save and compress Docker image ────────────────────────────────

save_image() {
    log_step "Step 3/6: Saving Docker image to /tmp"
    local image_file="/tmp/velox-worker-image.tar.gz"

    if $DRY_RUN; then
        log_info "[DRY-RUN] Would run: docker save $IMAGE_TAG | gzip > $image_file"
        echo "$image_file"
        return
    fi

    docker save "$IMAGE_TAG" | gzip > "$image_file"
    local size
    size=$(du -h "$image_file" | cut -f1)
    log_info "Image saved: $image_file ($size)"
    echo "$image_file"
}

# ─── Step 4-6: Deploy to remote hosts ─────────────────────────────────────

deploy_to_host() {
    local host=$1
    local password="${HOST_PASSWORDS[$host]}"
    local worker_id="${HOST_WORKER_IDS[$host]}"
    local image_file=$2

    log_step "Deploying to $host (worker: $worker_id)"

    # ── Step 4: SCP ──────────────────────────────────────────────────────
    log_info "Step 4/6: Copying image to $host..."
    local remote_path="/tmp/velox-worker-image.tar.gz"
    if [[ -z "$password" ]]; then
        scp_cmd "$image_file" "$SSH_USER@$host:$remote_path" ""
    else
        scp_cmd "$image_file" "$SSH_USER@$host:$remote_path" "$password"
    fi

    # ── Step 5: Docker load ──────────────────────────────────────────────
    log_info "Step 5/6: Loading image on $host..."
    if [[ -z "$password" ]]; then
        run_cmd ssh "$SSH_USER@$host" "docker load -i $remote_path"
    else
        run_cmd sshpass -p "$password" ssh -o StrictHostKeyChecking=no \
            "$SSH_USER@$host" "echo '$password' | sudo -S docker load -i $remote_path"
    fi

    # ── Step 6: Container restart ────────────────────────────────────────
    log_info "Step 6/6: Restarting container on $host..."
    local container_name="velox-worker-$worker_id"
    local docker_run
    docker_run=$(gen_docker_run_cmd "$worker_id")

    if [[ -z "$password" ]]; then
        run_cmd ssh "$SSH_USER@$host" "docker rm -f $container_name 2>/dev/null; $docker_run"
    else
        run_cmd sshpass -p "$password" ssh -o StrictHostKeyChecking=no \
            "$SSH_USER@$host" "echo '$password' | sudo -S sh -c 'exec 0<&-; docker rm -f $container_name 2>/dev/null; $docker_run'"
    fi

    log_info "✅ $host done"
}

# ─── Main ──────────────────────────────────────────────────────────────────

main() {
    echo -e "${BLUE}══════════════════════════════════════════════════════════════${NC}"
    echo -e "${BLUE}  Velox Worker Agent - Deploy${NC}"
    echo -e "${BLUE}══════════════════════════════════════════════════════════════${NC}"
    echo ""
    echo -e "  Image tag:    ${GREEN}$IMAGE_TAG${NC}"
    echo -e "  Master URL:   ${GREEN}$MASTER_URL${NC}"
    echo -e "  Work dir:     ${GREEN}$WORK_DIR${NC}"
    echo -e "  SSH user:     ${GREEN}$SSH_USER${NC}"
    echo ""

    # Load inventory
    load_inventory || true

    if [[ -n "$HOSTS_FILTER" ]]; then
        IFS=',' read -ra FILTERED <<< "$HOSTS_FILTER"
        HOST_LIST=()
        for h in "${FILTERED[@]}"; do
            h=$(echo "$h" | xargs)  # trim
            HOST_LIST+=("$h")
            if [[ -z "${HOST_PASSWORDS[$h]:-}" ]]; then
                HOST_PASSWORDS["$h"]=""
            fi
            if [[ -z "${HOST_WORKER_IDS[$h]:-}" ]]; then
                HOST_WORKER_IDS["$h"]="host_${h//./_}"
            fi
        done
    fi

    if [[ ${#HOST_LIST[@]} -eq 0 ]]; then
        log_error "No hosts to deploy. Use --hosts or provide an inventory file."
        exit 1
    fi

    echo -e "  Targets:"
    for h in "${HOST_LIST[@]}"; do
        echo -e "    - ${CYAN}$h${NC} (${HOST_WORKER_IDS[$h]})"
    done
    echo ""

    if $DRY_RUN; then
        echo -e "${YELLOW}══════════════════════════════════════════════════════════════${NC}"
        echo -e "${YELLOW}  DRY RUN - No commands will be executed${NC}"
        echo -e "${YELLOW}══════════════════════════════════════════════════════════════${NC}"
        echo ""
    fi

    # Step 1-3: Build locale
    build_agent
    build_docker
    local image_file
    image_file=$(save_image)

    # Step 4-6: Deploy remoto
    for host in "${HOST_LIST[@]}"; do
        deploy_to_host "$host" "$image_file"
    done

    # Cleanup
    if [[ -f "$image_file" ]] && ! $DRY_RUN; then
        rm -f "$image_file"
        log_info "Cleaned up local image file: $image_file"
    fi

    echo ""
    echo -e "${GREEN}══════════════════════════════════════════════════════════════${NC}"
    echo -e "${GREEN}  Deploy completato!${NC}"
    echo -e "${GREEN}══════════════════════════════════════════════════════════════${NC}"
    echo ""
    echo "Per verificare:"
    echo "  curl -s http://localhost:8000/api/v1/workers | python3 -m json.tool"
    echo ""
    echo "Per controllare i log di un worker:"
    echo "  ssh pierone@<host> 'sudo docker logs velox-worker-<worker_id> --tail 50'"
}

main "$@"
