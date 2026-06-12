# Velox Worker Agent (Go)

Worker agent Go per il sistema Velox. Comunica con il master Velox per ricevere job di rendering video/audio, eseguirli e riportare i risultati.

## Struttura del Progetto

```
worker-agent-go/
├── cmd/                    # Entrypoint applicativi
│   ├── installer/          # Installer per deploy worker
│   └── velox-worker-agent/ # Worker agent principale
├── internal/               # Logica interna (non esportabile)
│   ├── worker/             # Orchestrazione worker (15 file)
│   │   ├── worker.go               # Start/Stop lifecycle
│   │   ├── worker_init.go          # Worker struct e New()
│   │   ├── worker_comms.go         # Heartbeat, register/unregister
│   │   ├── worker_config.go        # Configurazione runtime
│   │   ├── worker_jobs.go          # Job polling e dispatch
│   │   ├── worker_commands.go      # Command handling
│   │   ├── job_params.go           # Estrazione parametri job
│   │   ├── job_upload.go           # Upload video completati (Drive/S3)
│   │   ├── job_executor.go         # Esecuzione job (process_video, render, audio)
│   │   ├── concurrency.go          # Limitatore concorrenza (max N job paralleli)
│   │   ├── stage_executor.go       # Esecuzione per stage
│   │   └── stage_executor_types.go # Tipi stage executor
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

## Job Types Supportati

| Job Type | Descrizione |
|----------|-------------|
| `process_video` | Composizione video con scene, immagini, voiceover |
| `render` | Pipeline rendering generale |
| `process_audio` | Elaborazione audio standalone |
| `health_check` | Test heartbeat worker |
