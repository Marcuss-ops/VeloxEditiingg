# Velox

Distributed video generation and composition system.

## Documentation

ADRs, deployment notes, and architecture references live in [`docs/`](docs/).
The legacy pre-gRPC architecture is preserved at
[`docs/archive/architecture-pre-grpc.md`](docs/archive/architecture-pre-grpc.md)
for historical context only — it does NOT describe the current control plane.

## Repository layout

```
deploy/                       # Install scripts, systemd unit, env template
└── install-server.sh         # sudo ./deploy/install-server.sh
└── velox-server.service      # systemd unit
└── velox-server.env.example  # copy to /etc/velox-server.env

docs/                         # ADRs, deploy notes, archived architecture

refactored/                   # Source tree (pending Step 5 promotion to root)
├── DataServer/               # Master server (Go/Gin + gRPC)
├── RemoteCodex/              # Worker agent (Go) + video engine (C++/FFmpeg)
└── frontend_standalone/      # SPA frontend (VELOX_SPA_DIR) — planned split

shared/                       # Shared Go lib (already promoted to root)
VERSION.txt                   # Single source of version truth
```

## Quick start

Run the master server (development):

```bash
cd refactored/DataServer
export VELOX_ADMIN_TOKEN=velox-dev-token
go run ./cmd/server
# → http://0.0.0.0:8000
```

Install on a production host (Debian/Ubuntu, systemd):

```bash
sudo ./deploy/install-server.sh
# then edit /etc/velox-server.env (template in deploy/velox-server.env.example)
sudo systemctl restart velox-server
```

## License

Proprietary.
