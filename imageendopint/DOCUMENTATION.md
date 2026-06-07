# Image Endpoint - Google Flow Headless Generator

Questo progetto è un backend avanzato basato su **FastAPI** e **Playwright** progettato per automatizzare la generazione di immagini tramite l'interfaccia **Google Flow (Labs.google)** in modalità completamente headless.

## Caratteristiche Principali

- **Puro Headless**: Funziona senza interfaccia grafica, ideale per server e container.
- **Bypass Anti-Bot**: Integra `playwright-stealth` per mascherare l'automazione e superare i controlli "unusual activity" di Google.
- **Gestione Sessioni**: Utilizza file `storage-state.json` per riutilizzare sessioni Chrome già loggate, evitando di dover rifare il login ad ogni avvio.
- **Estrazione Intelligente**:
    - Scansiona **Shadow DOM** ricorsivamente.
    - Rileva immagini in `srcset`, `currentSrc` e **background-image** CSS.
    - Scarica **solo le nuove immagini** generate, filtrando quelle già presenti nella gallery del progetto.
- **Rendering Parallelo**: Supporta la generazione simultanea su più progetti Flow.
- **Anti-Saturazione**: Applica un jitter randomico (1-5s) all'avvio di ogni task per evitare pattern di bot rilevabili.
- **Layout x4**: Forza automaticamente la selezione del layout a 4 immagini prima di inviare il prompt.

## Architettura del Progetto (Modulare)

Il cuore del sistema è nel pacchetto `app/browser/`:

- `actions.py`: Gestisce le interazioni UI (focus prompt, selezione layout x4, click bottoni).
- `extraction.py`: Logica avanzata per trovare e scaricare i file immagine reali dal DOM.
- `generation.py`: Orchestratore principale del flusso di lavoro.
- `utils.py`: Funzioni di supporto per cookie e gestione profili.

## Requisiti

- Python 3.11+
- Google Chrome o Chromium installato
- Playwright e dipendenze: `pip install -e . && playwright install`

## Guida all'Uso

### 1. Preparazione della Sessione (Una sola volta)

Per funzionare in headless, il sistema ha bisogno di una sessione valida:

1. Avvia Chrome in modalità visibile per fare il login:
   ```bash
   ./scripts/chrome-session.sh open
   ```
2. Effettua il login al tuo account Google su [Labs.google Flow](https://labs.google/fx/tools/flow).
3. Una volta loggato, esporta lo stato della sessione:
   ```bash
   ./scripts/export-storage-state.sh
   ```
   Questo creerà il file `outputs/flow-storage-state.json`.
4. Chiudi Chrome:
   ```bash
   ./scripts/chrome-session.sh close
   ```

### 2. Configurazione Variabili d'Ambiente

Crea un file `.env` (usa `.env.example` come base):
```env
HEADLESS=true
STORAGE_STATE_PATH="/home/pierone/Pyt/imageendopint/outputs/flow-storage-state.json"
CHROME_EXECUTABLE="/usr/bin/google-chrome" # o il tuo percorso
API_TOKEN="tua_chiave_segreta"
```

### 3. Avvio del Server

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

### 4. Utilizzo delle API

#### Generazione Immagine
Invia una richiesta POST a `/v1/generate`:

```bash
curl -X POST http://localhost:8000/v1/generate \
  -H "Authorization: Bearer tua_chiave_segreta" \
  -H "Content-Type: application/json" \
  -d '{
    "project_id": "ID_DEL_TUO_PROGETTO_FLOW",
    "prompt": "A majestic dragon flying over a sunset city, 8k"
  }'
```

#### Controllo Stato e Download
Il comando sopra restituisce un `job_id`. Puoi controllare lo stato qui:

```bash
curl http://localhost:8000/v1/jobs/{job_id}
```

Quando lo stato è `succeeded`, troverai i link alle immagini scaricate localmente nella cartella `outputs/`.

## Debugging

Se hai problemi con l'interfaccia di Flow, puoi usare lo script di debug dedicato:
```bash
python scripts/debug-nano-banana-final.py
```
Questo script scatterà screenshot ad ogni passaggio della selezione layout per verificare che il sistema "veda" correttamente i pulsanti dell'interfaccia.

## Note sulla Sicurezza
Il file `flow-storage-state.json` contiene i tuoi cookie di sessione Google. **Non condividere mai questo file** e assicurati che sia incluso nel tuo `.gitignore`.
