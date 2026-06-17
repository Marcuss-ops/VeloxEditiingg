# `frontend_standalone` — SPA frontend (in transizione verso repo separato)

> **Stato**: in questo repository il frontend è ancora presente come *sorgente*.
> Il `web/dist/` compilato **non** viene più versionato a fianco del backend
> (vedi `.gitignore`). Il master Velox lo consuma via la variabile
> `VELOX_SPA_DIR`.

## Cosa c'è qui

```
frontend_standalone/
├── web/         # SvelteKit (Creator Studio + YouTube Manager + Drive + Livestream + Dark Editor v2)
└── dark_editor/ # Editor immagini AI (build separato, GPU/NVIDIA)
```

## Build locale

```bash
cd frontend_standalone/web
npm install
npm run build           # produce ./build (e ./dist se configurato)
```

```bash
cd frontend_standalone/dark_editor
npm install
npm run build
```

## Consumo da parte del master

Il master Go legge la variabile `VELOX_SPA_DIR` per sapere dove pescare
`index.html` e la cartella `assets/`:

```bash
export VELOX_SPA_DIR=/srv/velox/frontend-velox/build
go run ./DataServer/cmd/server
```

Se `VELOX_SPA_DIR` non è impostata (o punta a una directory senza
`index.html`), il master resta pienamente funzionale come API ma serve una
*landing page* al posto della SPA — vedi
`DataServer/internal/handlers/web/proxy/compat.go::LandingPage` e il log
`[FRONTEND] WARNING` emesso al boot da
`DataServer/internal/modules/frontend/module.go`.

## Piano di separazione

Questa directory è pronta per essere promossa a un repository proprio
(`frontend-velox`). I passi previsti sono:

1. `git mv` del solo contenuto utile (`web/`, `dark_editor/`, `package.json`,
   `README.md`, `.gitignore`) in un nuovo repo Git.
2. Aggiornare i workflow CI per pubblicare un artifact del build (e relativo
   `sha256`) consumato dal master al deploy.
3. Rimuovere `frontend_standalone/` da questo repository una volta che il
   master può raggiungere il frontend via URL/artifact in modo deterministico.

## Compatibilità

- Il bundle JS storico `creator_studio_app/dist/` viene servito dal master
  dalle stesse route di prima (`/assets`, `/creator_studio_app/dist/assets`,
  `/youtube-suite/*`). Nessuna route API è cambiata.
- I link relativi in `docs/` puntano a percorsi del backend; restano validi
  indipendentemente da dove vive la SPA.
