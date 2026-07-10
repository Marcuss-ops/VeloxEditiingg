# Velox Editing

Monorepo personale **WIP** di Marcus. Repository principale per lo sviluppo.

**Prerequisiti**: `git` installato + accesso di rete a `github.com` (HTTPS).

## Struttura

- [`InstaeditLogin/`](InstaeditLogin/) — modulo (Go + React) per il login
  OAuth federato di Instaedit (social-scheduler). **Modulo attivo**.
  Vedi [`InstaeditLogin/README.md`](InstaeditLogin/README.md) per i docs del
  modulo e [`HANDOFF-LINUX.md`](InstaeditLogin/HANDOFF-LINUX.md) per le linee
  guida operative.
- (altri sibling worktree su altri path non sono parte integrante di Velox Editing)

## ⚠️ Setup del submodule `InstaeditLogin` per fresh clone

Questo repo **non include il file `.gitmodules`** (configurazione intenzionalmente
minimalista). Di conseguenza, dopo aver clonato `VeloxEditiingg.git`, la
directory `InstaeditLogin/` risulta **vuota** perché Git non conosce l'URL
del modulo.

Per popolarla correttamente, dopo il clone:

```bash
# dentro al repository appena clonato
rmdir InstaeditLogin 2>/dev/null || rm -rf InstaeditLogin    # rimuove la cartella vuota
git clone https://github.com/Marcuss-ops/InstaeditLogin.git InstaeditLogin
cd InstaeditLogin
git checkout main
```

> ⚠️ **Non eseguire `git submodule update --init`** — fallirà con errore
> criptico (`No submodule mapping found in .gitmodules`) perché `.gitmodules`
> è assente per scelta architetturale.

> **Nota**: dopo questo workaround `InstaeditLogin/` diventa un **repo Git
> indipendente** (non un submodule entry). `git diff --submodule=log` non
> funzionerà dal parent; `git submodule update` resta inutilizzabile. È il
> trade-off deliberato: meno automazione, ma il repo resta clona-bile da
> zero senza dipendenze nascoste.

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

## Convenzioni operative personali dell'autore

- Tutti i commit vanno direttamente su `main` (no branch locali permanenti)
- Push frequenti e atomici (un commit logico = un push)
- Mai `git add .` nel parent (esistono sibling worktree dirty da non bundlare;
  fare sempre `git add <path>` esplicito)
- Push verso `VeloxEditiingg/main` da detached HEAD: `git push origin HEAD:main`
  > `HEAD:main` è necessario perché `main` è checked-out in un altro worktree
  > sibling (`VeloxLEgit/`). Da un branch locale basterebbe `git push origin main`.

## Status dei lavori

- **Active**: InstaeditLogin (OAuth federato → Instagram, TikTok, LinkedIn,
  X/Twitter, YouTube, Facebook)
- **Roadmap architetturale** (in ordine): workspaces table → posts + post_targets
  table → Go structs/repository → REST API `/workspaces`, `/posts` → worker
  pubblicazione asincrona → presigned-URL storage → JWT strict su production →
  split dev/prod database
