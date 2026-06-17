# `frontend-velox` — Velox SPA frontend

> Standalone repository that builds the Single Page Application consumed by
> the Velox master server. This directory is the **extraction-prep layout**
> of the sources that used to live at `frontend_standalone/` inside the
> Velox master repo: once the master stops shipping the bundle, a new
> GitHub repository named `frontend-velox` will be initialized from this
> tree.

## What lives here

```
frontend-standalone/                    ← was frontend_standalone/ inside velox-core
├── web/                                ← SvelteKit app (Creator Studio + YT Manager + Drive + Livestream)
├── scripts/build-and-bundle.sh         ← local build that mirrors the GitHub Actions workflow
├── .github/workflows/release.yml       ← builds + sha256 + GitHub Release on tag v*
├── package.json                        ← npm workspaces (web)
└── README.md                           ← this file
```

The `dark_editor/` editor lives under `web/` today. When it becomes a
standalone workspace member, add `"web/dark_editor"` to `package.json`
`workspaces` and document it here.

## Build locally

```bash
# install workspace dependencies
(cd web && npm ci)

# build the SvelteKit bundle
(cd web && npm run build)

# OR use the unified build+sign script that produces a tarball + sha256
VERSION=v0.1.0 ./scripts/build-and-bundle.sh
```

Output:

```
dist/<VERSION>/                          # raw build output, kept for inspection
dist/frontend-<VERSION>.tar.gz           # portable artifact
dist/frontend-<VERSION>.tar.gz.sha256    # sha256 + human-readable metadata
```

Pointing the Velox master at the build is now a single env var:

```bash
tar -xzf dist/frontend-v0.1.0.tar.gz -C /srv/velox/spa
export VELOX_SPA_DIR=/srv/velox/spa/v0.1.0
go run ./cmd/server
```

## CI release flow

`.github/workflows/release.yml` runs on every push of a tag matching
`v*` (e.g. `v0.1.0`):

1. `npm ci` + `npm run build` in the `web` workspace
2. Pack the `web/build` tree into `dist/frontend-$VERSION.tar.gz`
3. Write `dist/frontend-$VERSION.tar.gz.sha256` (64-char hash + metadata)
4. Attach both files to the GitHub Release `$VERSION` via
   `softprops/action-gh-release`

Manual dispatches (`workflow_dispatch`) produce the same artifact and
upload it as a workflow artifact, useful for testing the build before
tagging.

## Master server consumption

The Velox master keeps its existing contract:

| Variable          | Default                                  | Notes                         |
|-------------------|------------------------------------------|-------------------------------|
| `VELOX_SPA_DIR`   | unset (API-only mode)                    | Path with `index.html`        |
| `VELOX_SPA_DIR`   | `/srv/velox/spa/<tag>`                   | Production mount, after deploy downloads + verifies sha |

Recommended deploy step (operator script — not yet in the master repo):

```bash
SHA="$(curl -sSL https://github.com/.../releases/latest/download/frontend-v0.1.0.tar.gz.sha256 | awk '{print $1}')"
curl -sSL "https://github.com/.../releases/latest/download/frontend-v0.1.0.tar.gz" -o /tmp/spa.tgz
echo "$SHA  /tmp/spa.tgz" | sha256sum -c -
rm -rf /srv/velox/spa/v0.1.0
mkdir -p /srv/velox/spa/v0.1.0
tar -xzf /tmp/spa.tgz -C /srv/velox/spa/v0.1.0
systemctl restart velox-master
```

## Extraction roadmap

When this directory graduates to a standalone GitHub repo:

1. `git init frontend-velox` (or create the empty repo on GitHub first)
2. Copy `web/`, `scripts/`, `.github/`, `package.json`, `README.md`,
   `.gitignore` over.
3. Add the front-end consumers + the deploy step above to the Velox
   master repo's documentation (`docs/frontend_artifact.md` or a
   section in `README.md`).
4. Delete `frontend_standalone/` from the Velox master repo once
   `VELOX_SPA_DIR` consumers are wired to download from releases and
   the legacy `web/dist/` no longer exists.
