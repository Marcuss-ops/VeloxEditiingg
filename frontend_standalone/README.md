# `frontend-velox` — Velox SPA frontend

> The Velox front-end lives at `refactored/frontend_standalone/` inside the
> Velox master repo. The former external source-of-truth on GitHub
> (`Marcuss-ops/VeloxFrontend`) has been **collapsed into this subtree** —
> VeloxEditing is now the canonical home, and that GitHub URL no longer
> needs to be maintained separately.

## Stack — what the synced lockfile actually pins

The lockfile at `web/package-lock.json` was synced byte-for-byte from
`Marcuss-ops/VeloxFrontend@HEAD`, and it pins a **React 19 + Vite 7 +
Tailwind 3 + Radix UI** stack — *not* SvelteKit (older docs in the wider
repo used to say SvelteKit; those were stale pre-extraction assumptions and
are now superseded).

| Layer                  | Choice                                                    |
|------------------------|-----------------------------------------------------------|
| Framework              | React 19 with `react-router-dom@^7.13.1`                  |
| Bundler / dev server   | Vite 7 with `@vitejs/plugin-react@^5.1.4`                 |
| Styling                | Tailwind 3 + `tailwindcss-animate` + `tailwind-merge`    |
| UI primitives          | `@radix-ui/react-{dialog,dropdown-menu,select,slider,slot,tabs,tooltip}` |
| Data layer             | `@tanstack/react-query@^5.60.0`                          |
| Visualisation          | `chart.js@^4.5.1`                                         |
| Motion                 | `motion@^12.34.3`                                         |
| Icons                  | `lucide-react@^0.575.0`                                   |
| Tests                  | `vitest@^4` + `@testing-library/{dom,jest-dom,react}` + `@playwright/test` |
| Lint                   | `eslint@^9` flat config with `typescript-eslint@^8`       |

`web/package.json` declares every entry above with the same caret ranges as
the lockfile, so `npm ci` reproduces the upstream tree exactly without
re-generating the lockfile (the byte-identical sync evidence is preserved).

## What's in here

```
refactored/frontend_standalone/
├── web/                                ← React 19 + Vite 7 SPA workspace (authored)
│   ├── dark_editor/                    ← editor subdir (lockfile + .gitignore only — awaiting source)
│   ├── src/
│   │   ├── App.tsx                     ← one toy page that confirms the vite build emits a working bundle
│   │   ├── main.tsx                    ← React 19 createRoot mount + StrictMode
│   │   ├── index.css                   ← minimal system-font + light/dark colour-scheme CSS
│   │   └── vite-env.d.ts               ← vite/client ambient types
│   ├── index.html                      ← Vite entry, mounts #root at /src/main.tsx
│   ├── vite.config.ts                  ← Vite 7 + @vitejs/plugin-react (emits to dist/)
│   ├── tsconfig.json                   ← ES2022 + bundler resolution + react-jsx
│   └── package-lock.json               ← byte-identical to upstream VeloxFrontend@HEAD
├── scripts/
│   └── build-and-bundle.sh             ← local build that mirrors .github/workflows/release.yml
├── .github/workflows/release.yml       ← builds + sha256 + GitHub Release on tag v*
├── package.json                        ← root npm scripts (delegate via --prefix web)
├── .gitignore                          ← recursive iglob for dist/, build/, test-results/, node_modules/, …
└── README.md                           ← this file
```

The `src/App.tsx` is intentionally a **toy page** ("Velox frontend boot")
that proves `vite build` emits a working bundle. Replace it with the real
Creator Studio / YouTube Manager / Drive / Livestream UI when you start
fleshing out the application; the Radix primitives, React Router, TanStack
Query, Tailwind utilities, and chart.js are already declared in
`web/package.json` and resolved by the synced `web/package-lock.json` so
imports will Just Work.

## Build locally

```bash
# from refactored/frontend_standalone/
npm install                                       # no-op for scripts (no workspaces yet)
( cd web && npm ci && npm run build )             # the real path — installs + builds the SPA

# OR
npm run build                                     # prints a no-op message until web/package.json is authored

# OR the unified build+sign tarball
VERSION=v0.1.0 ./scripts/build-and-bundle.sh
```

Vite 7 emits the SPA to `web/dist/`. `scripts/build-and-bundle.sh` copies
that directory into `dist/<VERSION>/` and tarballs + sha256-signs it as
`dist/frontend-<VERSION>.tar.gz`. The earlier `web/build/` path used by the
SvelteKit-era scaffold is **not** produced by Vite; if anything ever shows
up there, it's a stale artifact and can be deleted.

### `--prefix web` does **not** auto-install — the node_modules pitfall

`npm --prefix web run <script>` does NOT trigger an install. Two facts
that are easy to get wrong unless you've read npm's docs closely:

1. **`npm run X --prefix <dir>` does NOT trigger an install.** When you
   call `npm --prefix web run build`, npm changes its working directory
   to `web/` and runs the `build` script. It does **not** first reconcile
   `web/node_modules/` against `web/package-lock.json`. Any `--prefix`
   invocation silently assumes node_modules is already in sync.
2. **The auto-install behaviour is reserved for `npm install` /
   `npm ci` — not for `npm run`.** Forgetting this is the most common
   cause of `'vite' is not recognized as an internal or external command`
   on fresh checkouts: `npm run build` from a clean clone finds no
   `vite` because nothing ran `npm ci` first.

Practical rules of thumb:

* **Always precede `npm run` with `( cd web && npm ci )`** (or
  `npm install`). The release workflow and `scripts/build-and-bundle.sh`
  already do this; don't drop it when copy-pasting.

## Pointing the Velox master at the build

```bash
tar -xzf dist/frontend-v0.1.0.tar.gz -C /srv/velox/spa
export VELOX_SPA_DIR=/srv/velox/spa/v0.1.0
go run ./cmd/server
```

The Velox master keeps its existing contract:

| Variable          | Default                                       | Notes                                 |
|-------------------|-----------------------------------------------|---------------------------------------|
| `VELOX_SPA_DIR`   | unset (API-only mode)                         | Path with `index.html`                |
| `VELOX_SPA_DIR`   | `/srv/velox/spa/<tag>`                        | Production mount, after deploy downloads + verifies sha |
| `VELOX_SPA_DIR`   | `<repo>/refactored/frontend_standalone/web/dist` | Offline / dev mode — points at the freshly-built bundle |

## CI release flow

`.github/workflows/release.yml` runs on every push of a tag matching
`v*` (e.g. `v0.1.0`):

1. `npm ci` + `npm run build` in the `web` workspace (produces `web/dist/`)
2. Copy `web/dist/` into `dist/<VERSION>/` under the GHA artifact root
3. Tarball + sha256-sign as `dist/frontend-<VERSION>.tar.gz`
4. Attach both files to the GitHub Release `$VERSION` via
   `softprops/action-gh-release`

Manual dispatches (`workflow_dispatch`) produce the same artifact and upload
it as a workflow artifact, useful for testing the build before tagging.

## Drift detection

`web/package-lock.json` is byte-identical to `Marcuss-ops/VeloxFrontend@HEAD`
today. If upstream ever shifts, the lockfile diff is the canonical signal.
A future enhancement is `scripts/sync-check.mjs` that archives the upstream
tag and diffs it against the local copy; it's not in the tree today to keep
the extraction diff focused.

## dark_editor next steps

`web/dark_editor/` currently carries only a `.gitignore` and a lockfile
(also synced from upstream). The lockfile presumably pins the editor's
separate dependency tree, but no `package.json` was upstream. To bring it
online, add `web/dark_editor/package.json` whose dependencies match the
lockfile, and (optionally) register it as a sibling workspace in
`web/package.json` under `workspaces`.
