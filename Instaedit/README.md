# Velox Editing

Monorepo personale (legacy). Worktree principale per lo sviluppo.

## Struttura

- `InstaeditLogin/` — modulo (Go + React) per il login OAuth federato
  di Instaedit (social-scheduler). **Modulo attivo**.
- (altri worktree sibling su altri path non sono parte integrante di Velox Editing)

## ⚠️ Setup del submodule `InstaeditLogin` per fresh clone

Questo repo **non include il file `.gitmodules`** (configurazione intenzionalmente
minimalista). Di conseguenza, dopo aver clonato `VeloxEditiingg.git`, la
directory `InstaeditLogin/` risulta **vuota** perché Git non conosce l'URL
del modulo.

Per popolarla correttamente, dopo il clone:

```bash
# dentro al worktree appena clonato
rmdir InstaeditLogin                                  # rimuove la cartella vuota
git clone https://github.com/Marcuss-ops/InstaeditLogin.git InstaeditLogin
cd InstaeditLogin
git checkout main
```

## Come aggiornare il submodule in futuro

Quando viene pushato un nuovo commit sul submodule (es. `0751a20`, o successivi):

```bash
cd InstaeditLogin
git pull origin main                                  # scarica nuovi commit del submodule
cd ..
git add InstaeditLogin                                # bump del gitlink nel parent
git commit -m "chore: bump InstaeditLogin submodule"
git push origin main
```

Regole operative (vincolanti):
- Tutti i commit vanno direttamente su `main` (no branch locali)
- Push frequenti e atomici
- Mai `git add .` nel parent (esistono dirty submodules sibling da non bundlare)

## Status dei lavori

- **Active**: InstaeditLogin (OAuth federato → Instagram, TikTok, LinkedIn,
  X/Twitter, YouTube, Facebook)
- **Roadmap architetturale** (in ordine): workspaces table → posts + post_targets
  table → Go structs/repository → REST API `/workspaces`, `/posts` → worker
  pubblicazione asincrona → presigned-URL storage → JWT strict su production →
  split dev/prod database
