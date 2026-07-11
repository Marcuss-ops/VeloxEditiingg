# RW-PROD-003 — Bootstrap runtime ed executor reale

**Priorità:** P0
**Dipendenze:** RW-PROD-002
**Stato attuale:** fail-closed su `video.NewPipelineRunner` (`cmd/velox-worker-agent/main.go:281`). Mancano: self-test del motore C++, self-test FFmpeg/ffprobe, verifica output dir scrivibile, pubblicazione capability **solo dopo bootstrap**, versione/engine/bundle mismatch abort.

---

## 1. Pain points

1. **Main non chiama il motore C++ per un self-test.** `video.NewPipelineRunner` controlla solo che il wrapper Go possa essere istanziato. Non esegue un render di prova (es. 1-frame minimal) per validare `velox-render-cpp` end-to-end.

2. **FFmpeg/ffprobe non verificati.** Anche se montati nel container, il bin può essere una fake-shim. Niente `ffprobe -version` parse.

3. **`scene.composite.v1@1` registrato ma non esercitato.** `executor.MustRegister(sceneComposite)` conclude la registrazione senza chiamare `Run(ctx, renderOneFrameMinimal)`. Se la pipeline è rotta, fallisce solo al primo TaskOffer reale.

4. **Output dir scrivibile non verificata a boot.** `pkg/video/services/native/render_client.go` scrive in `/tmp/velox/scene-composite` (default `main.go:288`). Una directory read-only o piena viene scoperta solo al primo job.

5. **Capability pubblicate prima del bootstrap.** Nella sessione di Hello: `buildHello` chiama `capabilitiesMap` *prima* che il bootstrap abbia confermato motore+ffmpeg. Un master potrebbe assegnare un task a un worker che in realtà non può eseguire.

6. **Version mismatch binary/engine/bundle non abortito.** `cfg.Version/EngineVersion/BundleVersion/ProtocolVersion` sono valorizzati ma non confrontati con valori attesi del bundle sul disco.

---

## 2. Soluzione

Introdurre **`bootstrap.OK` gate** (post-TLS-verification, pre-Hello) che:

1. **Eseguire self-test del motore C++**:
   - Renderizzare un "black 1×1 frame, 1s" via il wrapper, misurare durata ≤ 5s, verificare output SHA-256 noto.

2. **Eseguire self-test FFmpeg/ffprobe**:
   - `ffprobe -version` major ≥ 4; `ffmpeg -version` major ≥ 4; `ffmpeg -f lavfi -i color=c=black:s=64x64 -frames:v 1 -f null -` deve terminare 0.

3. **Verificare output dir**:
   - `os.MkdirAll(outputDir, 0o755)`, `ioutil.WriteFile(outputDir+"/.write_test", ...)`, `os.Remove`.

4. **Capability pubblicate solo dopo OK**:
   - `telemetry.SetHealthRegistered(true)` chiamato in `worker.Start` *dopo* `bootstrap.Ok()`. Currently è chiamato subito dopo `transport.Connect` → race.

5. **Mismatch check**:
   - Se `cfg.BundleHash` != `readTextFileFirst(workDir, "BUNDLE_HASH.txt")` ⇒ fail-closed + log `bundle_version_mismatch`.

---

## 3. Azioni concrete

| # | File:line | Azione |
|---|-----------|--------|
| A1 | `RemoteCodex/.../pkg/bootstrap/` (nuovo) | Creare `bootstrap.Run(ctx, cfg) error` con sotto-step engine/ffmpeg/output/self-render. |
| A2 | `RemoteCodex/.../pkg/bootstrap/self_render.go` | Render frame 1×1 nero; verificare exit 0; pulire tmp. |
| A3 | `RemoteCodex/.../pkg/bootstrap/ffmpeg.go` | Esegui `ffprobe -version` e `ffmpeg -h encoder=libx264`, parse major. |
| A4 | `RemoteCodex/.../pkg/bootstrap/output_dir.go` | Mkdir + write + remove test. |
| A5 | `RemoteCodex/.../cmd/velox-worker-agent/main.go` (~234, subito dopo `pipelineRunner, pipeErr := video.NewPipelineRunner(bootLog)`) | Dopo `pipelineRunner` istanziato e **prima** di `sceneComposite := executors.NewSceneComposite(...)`, chiamare `bootstrap.Run(ctx, cfg, pipelineRunner)`. Se errore, `os.Exit(1)` con codice logging dedicato. |
| A6 | `RemoteCodex/.../internal/worker/worker.go:105-108` | Spostare `SetHealthRegistered(true)` dopo `bootstrap.Ok()`. |
| A7 | `RemoteCodex/.../internal/worker/worker.go|buildHello` + composition root `cmd/velox-worker-agent/main.go:topLevel` | Decisione finale: posticipare boot. Niente secondo invio "DEFERRED_HELLO" né `MsgCapabilityRefresh`. Si ottiene bloccando `w.Start(ctx)` finché bootstrap non è OK. Master side selector vedrà `capabilities=[]` per la finestra bootstrap (≤ 10s); fino a quando `SetHealthRegistered(true)` non viene chiamato, il master escluderà il worker via `costmodel.Score` (`registered=false`). |
| A8 | `pkg/bundle/` (nuovo) | Funzione `BundleHashMatches(cfg BundleHash) error` che parsa `BUNDLE_HASH.txt` su disco. |
| A9 | `RemoteCodex/.../pkg/video/services/native/render_client.go` | Non mostrare fallback se binary assente: hard-error. |

---

## 4. Criteri di accettazione

- [ ] Worker senza motore C++ NON entra nel registry master (Hello non inviato o subito followed by `MsgGoodbye` reason=`engine_missing`).
- [ ] Worker con executor non inizializzato NON annuncia capability.
- [ ] Output ffprobe valido per self-test.
- [ ] Bundle mismatch (`cfg.BundleHash` != disco) → `os.Exit(1)` + log `bundle_version_mismatch`.
- [ ] Nessun `fallback_count++` o `python_emergency_path++` durante bootstrap.

---

## 5. Test obbligatori

- `bootstrap.Run` con motore assente (path inesistente) ⇒ errore `engine.missing`.
- `bootstrap.Run` con `scene.composite.v1@1` non registrato ⇒ errore `executors.missing`.
- `bootstrap.Run` con `OutputDir` read-only ⇒ errore `output_dir.readonly`.
- `bootstrap.Run` con FFmpeg mancante ⇒ errore `tools.ffprobe` (wrappa anche RW-PROD-002).
- Self-render frame nero su CI (CPU-only, ≤10s) — invocabile come `make bootstrap-smoke`.

---

## 6. Evidenze

- Log strutturato di `bootstrap` con durate step (`step=engine_selftest dur_ms=…`).
- Output JSON di `bootstrap.Run` con tutti gli step.
- Exit code e codice logging su ciascun fallimento.
