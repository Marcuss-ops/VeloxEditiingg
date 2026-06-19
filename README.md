# Velox

Distributed video generation and composition system.

## Documentation

The authoritative documentation lives in [`refactored/README.md`](refactored/README.md).

## Repository layout

```
refactored/                   # Official source
├── DataServer/               # Master server (Go/Gin + gRPC)
├── RemoteCodex/              # Worker agent (Go) + video engine (C++/FFmpeg)
├── shared/                   # Shared Go library
├── frontend_standalone/      # SPA frontend (VELOX_SPA_DIR)
├── docs/                     # Architecture docs, roadmap
├── scripts/                  # Build & deploy scripts
└── VERSION.txt               # Single source of version truth

scripts/                      # Root deploy wrapper
```

## Quick start

```bash
cd refactored/DataServer
export VELOX_ADMIN_TOKEN=velox-dev-token
go run ./cmd/server
# → http://0.0.0.0:8000
```

## License

Proprietary.
