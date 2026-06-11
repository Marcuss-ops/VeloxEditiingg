# Velox Worker Agent (Go)

Worker agent Go per il sistema Velox. Comunica con il master Velox per ricevere job di rendering video/audio, eseguirli e riportare i risultati.

## Struttura del Progetto

```
worker-agent-go/
├── cmd/                    # Entrypoint applicativi
│   ├── installer/          # Installer per deploy worker
│   └── velox-worker-agent/ # Worker agent principale
├── internal/               # Logica interna (non esportabile)
│   ├── worker/             # Orchestrazione worker (job loop, heartbeat, stage executor)
│   │   ├── worker.go               # Start/Stop lifecycle
│   │   ├── worker_init.go          # Worker struct e New()
│   │   ├── worker_comms.go         # Heartbeat, register/unregister
│   │   ├── worker_config.go        # Configurazione runtime
│   │   ├── worker_jobs.go          # Job polling
│   │   ├── job_params.go           # Estrazione parametri job
│   │   ├── job_upload.go           # Upload video completati
│   │   ├── job_executor.go         # Esecuzione job
│   │   ├── concurrency.go          # Limitatore concorrenza
│   │   ├── stage_executor.go       # Esecuzione stage
│   │   └── stage_executor_types.go # Tipi stage executor
│   └── telemetry/          # Metriche Prometheus
│       ├── prometheus.go           # PrometheusMetrics, KPI, server
│       └── metrics_types.go        # HistogramVec, CounterVec, GaugeVec
├── pkg/                    # Librerie pubbliche
│   ├── api/                # Client HTTP per master Velox
│   │   ├── client.go               # Client HTTP con retry
│   │   ├── api_types.go            # Tipi (Job, JobResult, ecc.)
│   │   ├── circuit_breaker.go      # Circuit breaker pattern
│   │   └── adapter.go              # Adapter endpoint API
│   │   └── renderplan/             # Contratto RenderPlan v1
│   ├── video/              # Pipeline video generation
│   ├── config/             # Config worker
│   ├── logger/             # Logger strutturato con eventi
│   │   ├── logger.go               # Logger base
│   │   ├── events.go               # EventCode, Event, builder
│   │   ├── events_ratelimit.go     # RateLimiter
│   │   └── events_helpers.go       # Convenience functions
│   └── nlp/                # NLP utilities
├── deploy/                 # Deploy e runtime
│   ├── install-worker.sh           # Script installazione
│   ├── rollback-worker.sh          # Script rollback
│   ├── velox-worker.service        # Systemd service
│   └── workspace/                  # Dati runtime (workspace versions)
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
