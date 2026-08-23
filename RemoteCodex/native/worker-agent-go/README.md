# Velox Worker Agent (Go)

Worker agent Go per il sistema Velox. Comunica con il master Velox per ricevere job di rendering video/audio, eseguirli e riportare i risultati.

## Struttura del Progetto

```
worker-agent-go/
├── cmd/                    # Entrypoint applicativi
│   ├── installer/          # Installer per deploy worker
│   └── velox-worker-agent/ # Worker agent principale
├── internal/               # Logica interna (non esportabile)
│   ├── worker/             # Orchestrazione worker
│   │   ├── worker.go               # Start/Stop lifecycle
│   │   ├── worker_init.go          # Worker struct e New()
│   │   ├── worker_lifecycle.go     # Start/Stop/runSession
│   │   ├── worker_registration.go  # buildHello + capabilityReport
│   │   ├── worker_claimloop.go     # receiveLoop + task dispatch
│   │   ├── worker_artifacts.go     # Artifact Commit Protocol
│   │   ├── worker_commands.go      # Command handling
│   │   ├── worker_config.go        # Configurazione runtime
│   │   ├── worker_types.go         # Helper types
│   │   ├── worker_persistence.go   # Persistenza stato
│   │   ├── heartbeat_loop.go       # Heartbeat loop
│   │   ├── heartbeat_payload.go    # Heartbeat proto construction
│   │   ├── heartbeat_intervals.go  # Interval policy
│   │   ├── lease_renewal.go        # Lease renewal loop
│   │   ├── active_lease_registry.go # Active task leases
│   │   ├── task_execution.go       # executeTask orchestrator
│   │   ├── task_dispatch.go        # dispatch path
│   │   ├── task_result_builder.go  # submitTaskResult
│   │   ├── active_task_lifecycle.go # metriche + upload
│   └── telemetry/          # Metriche Prometheus
│       ├── prometheus.go           # PrometheusMetrics, KPI, server
│       ├── metrics.go              # Metriche runtime
│       ├── metrics_types.go        # HistogramVec, CounterVec, GaugeVec
│       └── gc.go                   # GC stats
├── pkg/                    # Librerie pubbliche
│   ├── api/                # Client HTTP per master Velox
│   │   ├── client.go               # Client HTTP con retry e circuit breaker
│   │   ├── api_types.go            # Tipi (Job, JobResult, HeartbeatPayload)
│   │   ├── circuit_breaker.go      # Circuit breaker pattern
│   │   ├── adapter.go              # Adapter endpoint API
│   │   ├── client_test.go          # Test client
│   │   └── renderplan/             # Contratto RenderPlan v1
│   ├── video/              # Pipeline video generation
│   │   ├── workflow.go             # Orchestrazione generazione video
│   │   ├── native_engine.go        # Bridge al motore C++ (FFmpeg)
│   │   ├── date_number_extraction.go # Estrazione date/numeri
│   │   ├── entity_association.go   # Associazione entità
│   │   ├── entity_resolution.go    # Risoluzione entità
│   │   └── fuzzy_match.go          # Fuzzy matching
│   ├── config/             # Config worker
│   │   └── config.go               # WorkerConfig JSON, LoadConfig, Validate
│   ├── logger/             # Logger strutturato con eventi
│   │   ├── logger.go               # Logger base
│   │   ├── events.go               # EventCode, Event, builder
│   │   ├── events_ratelimit.go     # RateLimiter
│   │   └── events_helpers.go       # Convenience functions
│   └── nlp/                # NLP utilities
│       └── nlp.go                  # Natural language processing
├── deploy/                 # Deploy e runtime
│   ├── install-worker.sh           # Script installazione
│   ├── rollback-worker.sh          # Script rollback
│   ├── velox-worker.service        # Systemd service
│   └── workspace/                  # Dati runtime (workspace versions)
├── bin/                    # Binary compilati
├── Dockerfile              # Build immagine Docker
├── Makefile                # Build system
└── go.mod / go.sum         # Dipendenze Go
```

## Build

```bash
make build        # Build all
make agent        # Solo worker agent
make test         # Test
```

## Esecuzione

```bash
make run-agent    # Esecuzione locale (dev)
# oppure
./bin/velox-worker-agent -master http://master:8000
```

## Variabili d'Ambiente

| Variabile | Default | Descrizione |
|-----------|---------|-------------|
| `WORKER_ID` | hostname | ID univoco worker |
| `MASTER_URL` | — | URL del master server |
| `WORKER_TOKEN` | — | Token autenticazione worker |
| `VELOX_VIDEO_ENGINE_CPP_BIN` | — | Path binary video engine C++ |
| `VELOX_RENDER_BACKEND` | `native` | Set to `chronon` to render through Chronon3D |
| `CHRONON3D_CLI` | — | Path to `chronon3d_cli` when the Chronon backend is enabled |

### Chronon3D backend

The worker can consume Chronon without copying Chronon sources into Velox.
Use the worker image built from `Dockerfile.chronon`, which contains the
Chronon runtime and sets the required environment variables:

```bash
VELOX_RENDER_BACKEND=chronon
CHRONON3D_CLI=/opt/chronon3d/bin/chronon3d_cli
```

In this mode the existing Velox `RenderPlan` is converted to the versioned
`chronon.render-plan.v1` format and executed with `chronon3d_cli render-plan`.
The default `native` backend and its command line remain unchanged.

## Job Types Supportati

| Job Type | Descrizione |
|----------|-------------|
| `process_video` | Composizione video con scene, immagini, voiceover |
| `render` | Pipeline rendering generale |
| `process_audio` | Elaborazione audio standalone |
| `health_check` | Test heartbeat worker |
