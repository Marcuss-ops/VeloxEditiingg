# ============================================================================
# Velox Worker - Single-stage Build
# ============================================================================
# Runtime minimale con ffmpeg + cmake per compilazione C++ nativa.
# Il binario Go va precompilato con 'make agent' prima del docker build.
#
# Build:
#   cd RemoteCodex/native/worker-agent-go
#   make agent        # produce bin/velox-worker-agent
#   docker build -t velox-worker:latest .
#
# ============================================================================

FROM debian:bookworm-slim

ENV DEBIAN_FRONTEND=noninteractive

WORKDIR /app

# Dipendenze essenziali: ffmpeg per video, cmake/g++ per rebuild video engine,
# curl per comunicazioni HTTP, build-essential per compilazione C++ nativa.
RUN apt-get update -qq && apt-get install -y --no-install-recommends \
    build-essential \
    cmake \
    g++ \
    ffmpeg \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copia il binario precompilato (make agent deve essere stato eseguito prima del docker build)
# Nota: Docker mostra giÃ  un errore chiaro se il file non esiste nel build context:
#   COPY failed: file not found in build context: stat bin/velox-worker-agent: file does not exist
COPY bin/velox-worker-agent /usr/local/bin/velox-worker-agent

# Create non-root user for runtime
RUN useradd -r -m -s /usr/sbin/nologin velox && \
    chown -R velox:velox /app /usr/local/bin/velox-worker-agent

USER velox

# ============================================================================
# Worker leggero: Go binary + ffmpeg + cmake/build-essential.
# Nessun Node.js, nessun Remotion, nessuna libreria Chrome/GTK.
# Il motore grafico C++ nativo viene compilato al primo avvio del container
# tramite cmake (presente nell'immagine).
# ============================================================================

# Health check via HTTP endpoint
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Default: Go worker-agent-go
ENTRYPOINT ["/usr/local/bin/velox-worker-agent"]
CMD ["--master", "http://master:8000"]
