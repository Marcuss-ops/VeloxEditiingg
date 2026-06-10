# Handlers – struttura per dominio

La cartella `handlers` e ora separata per macro-dominio:

- `web`: endpoint e adapter per UI/web layer.
- `server`: logica API/server core.
- `remote`: orchestrazione worker remoti/ansible.

## Struttura attuale

```text
handlers/
├── README.md
├── web/
│   ├── dashboard/
│   ├── explorer/
│   ├── proxy/
│   └── spa/
├── server/
│   ├── analytics/
│   ├── api/
│   ├── db/
│   ├── drive/
│   ├── groups/
│   ├── health/
│   ├── jobs/
│   ├── master/
│   ├── pipeline/
│   └── youtube/
└── remote/
    ├── ansible/
    ├── install/
    ├── livestream/
    ├── submission/
    └── workers/
```

## Convenzioni

- Ogni subpackage mantiene `package <nome>` (es. `package drive`, `package workers`).
- Gli import usano il nuovo path dominio, ad esempio:
  - `velox-server/internal/handlers/server/drive`
  - `velox-server/internal/handlers/web/spa`
  - `velox-server/internal/handlers/remote/workers`
