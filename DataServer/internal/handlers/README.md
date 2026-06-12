# Handlers – struttura per dominio

La cartella `handlers` è separata per macro-dominio:

- `web`: endpoint e adapter per UI/web layer.
- `server`: logica API/server core.
- `remote`: orchestrazione worker remoti/ansible.

## Struttura attuale

```text
handlers/
├── README.md
├── web/
│   ├── darkeditor/        # Dark Editor web proxy
│   ├── dashboard/         # Worker dashboard
│   ├── explorer/          # File explorer
│   ├── proxy/             # NoRoute handler, compat, landing page
│   └── spa/               # SPA serving (history fallback)
├── server/
│   ├── api/               # Route /api/v1/* (api_v1.go, api_v1_native.go)
│   ├── analytics/         # Dashboard BI, analytics (10 file)
│   ├── auth/              # User auth (register, login, sessions)
│   ├── calendar/          # Calendario produzione video (6 file)
│   ├── collaboration/     # Project collaboration (enterprise)
│   ├── darkeditor/        # Dark Editor - editor immagini AI (16 file)
│   ├── diagnostics/       # Data layout diagnostics
│   ├── drive/             # Google Drive handlers
│   ├── groups/            # YouTube group management
│   ├── health/            # Health check
│   ├── jobs/              # Job CRUD, submission, normalization
│   ├── master/            # Create-master (multi-title video)
│   ├── pipeline/          # Pipeline generazione script
│   ├── script/            # Script con immagini
│   ├── video/             # Scene video, clip+stock, smoke test
│   └── youtube/           # YouTube upload + management (22 file)
└── remote/
    ├── ansible/           # Playbook Ansible per deploy
    ├── install/           # Script installazione worker
    ├── livestream/        # YouTube Live stream management
    ├── submission/        # Multi-clip submission management
    └── workers/           # Registrazione, heartbeat, bundle
```

## Convenzioni

- Ogni subpackage mantiene `package <nome>` (es. `package drive`, `package workers`).
- Gli import usano il nuovo path dominio, ad esempio:
  - `velox-server/internal/handlers/server/drive`
  - `velox-server/internal/handlers/web/spa`
  - `velox-server/internal/handlers/remote/workers`
