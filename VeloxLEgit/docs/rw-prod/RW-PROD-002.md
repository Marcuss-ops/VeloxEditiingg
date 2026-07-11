# RW-PROD-002 — Validazione completa configurazione production

**Priorità:** P0
**Dipendenze:** RW-PROD-001
**Stato attuale:** **gap importante.** Il flag `--validate-config` (`RemoteCodex/.../cmd/velox-worker-agent/main.go:117`) è documentato come *"validate config JSON (transport check) and exit"* — implementazione attuale è solo la validazione tripletta TLS di `pkg/config/config.go`. Mancano tutti i check runtime/IO/FFmpeg/engine.

---

## 1. Pain points

1. **Nessun check filesystem scrivibile.** Il worker può partire e morire al primo task perché `/opt/velox/cache` o `/opt/velox/blobs` sono read-only o piene.
2. **Nessun check FFmpeg / ffprobe.** `RemoteCodex/native/video-engine-cpp/CMakeLists.txt` non viene verificato a runtime; FFmpeg può essere assente.
3. **Nessun check motore C++ binario raggiungibile.** `pkg/video/services/native/render_client.go:135` chiama `syscall.Kill(-pid, SIGTERM)` ma non verifica che il bin esista.
4. **Executor registry vuoto non è errore in `--validate-config`.** `main.go:259` chiama `worker.New(..., WithRegistry(NewRegistry()))` (registry vuoto), e passa. Solo dopo Start() viene loggato il warning.
5. **Porte health/metrics libere non verificate.**
6. **DNS/reachability master non risolta.**
7. **Disk free minima non controllata.**
8. **`--validate-config` è una via separata.** RW-PROD-002 chiede che la stessa logica sia invocata dal `--doctor` futuro.

---

## 2. Soluzione

Refactor di `--validate-config` (e del futuro `doctor`) attorno a un **`Validator` interface** con tanti sotto-validatori componibili nel package `pkg/doctor` (nuovo):

```go
type Validator interface {
    Name() string
    Run(ctx context.Context) Result
}
type Result struct {
    Status  string // PASS | WARN | FAIL
    Detail  string
    Remedy  string
}
```

Validatori da implementare (ordine di esecuzione):

1. `ValidateEnvironment` — `cfg.Environment == "production" && cert/key/CA presenti, allow_insecure_grpc_dev deve essere false`.
2. `ValidateTransportTLS` — delega a `pkg/config/config.go::Validate()`.
3. `ValidateCertExpiry` — `notAfter > now + 14d`.
4. `ValidateDNSReachability` — risoluzione `cfg.MasterURL`, TCP dial.
5. `ValidateDirs` — mkdir/write/delete su `WorkDir`, `CacheDir`, `BlobDir`, `TempDir`, `OutputDir`.
6. `ValidateDiskFree` — `diskFreeBytes > cfg.MinDiskFreeMB * 1024 * 1024`.
7. `ValidateHealthMetricsPorts` — `net.Listen("tcp", :HealthPort)`, `net.Listen("tcp", :PrometheusPort)`, chiusura immediata.
8. `ValidateEngineBinary` — `os.Stat(cfg.VideoEngineCppBin)` ∧ eseguibile.
9. `ValidateFFmpeg` — `exec.LookPath("ffmpeg")`, `exec.LookPath("ffprobe")`, `--version` parsa.
10. `ValidateExecutorRegistry` — `len(registry.Descriptors()) >= 1` ∧ `scene.composite.v1@1` registrato.

Output JSON stabile e versionato (vedi RW-PROD-016):

```json
{ "worker_id": "...", "verdict": "READY|NOT_READY", "checks": [{"id":"mtls","status":"PASS","detail":"..."}] }
```

Exit code: 0 solo se tutti `PASS`/`WARN accettabili`; ≥1 se almeno un `FAIL`.

---

## 3. Azioni concrete

| # | File:line | Azione |
|---|-----------|--------|
| A1 | `RemoteCodex/.../pkg/doctor/validator.go` (nuovo) | Definire `Validator`, `Result`, registry di sotto-validatori. |
| A2 | `RemoteCodex/.../pkg/doctor/tls.go`, `dns.go`, `dirs.go`, `disk.go`, `ports.go`, `engine.go`, `ffmpeg.go`, `registry.go` | Implementare i 10 sotto-validatori con unit test. |
| A3 | `RemoteCodex/.../cmd/velox-worker-agent/main.go` --validate-config branch | Sostituire il blocco attuale (linee 250-263) con `doctor.Run(cfg).Exit()`. Stesso `os.Exit(0/non-zero)`. |
| A4 | `RemoteCodex/.../pkg/config/config.go` | Aggiungere `MinDiskFreeMB int`, `VideoEngineCppBin string`, `OutputDir string`, `TempDir string` con default. |
| A5 | `RemoteCodex/.../pkg/doctor/engine.go` | `os.Stat + access(X_OK)` su `cfg.VideoEngineCppBin` (default `velox-render-cpp`). |
| A6 | `RemoteCodex/.../pkg/doctor/ffmpeg.go` | `exec.CommandContext(ctx, "ffprobe", "-version")`, parse major version >=4.0. |
| A7 | `RemoteCodex/.../pkg/doctor/registry.go` | `executor.Registry.Resolve("scene.composite.v1", 1)` deve restituire non-nil. |
| A8 | `cmd/velox-worker-agent/main.go:117` (doc comment del flag) | Aggiornare la doc-string: "validate production-readiness and exit (RW-PROD-002)" — non più solo transport. |
| A9 | `deploy/scripts/apply-local-worker-config.sh` | Già chiama `--validate-config` (linee 478-499). Verificare che il nuovo exit code (2 = FAIL con JSON output) sia correttamente interpretato dalla shell. |

---

## 4. Criteri di accettazione

- [ ] Tutti i 10 sotto-validatori implementati e testati in isolamento.
- [ ] `--validate-config` su config completa valida → exit `0`, JSON `verdict: READY`.
- [ ] `cache_dir` non scrivibile → exit non-zero, `checks[].id=disk.cache_writable`, `status=FAIL`, `remedy`.
- [ ] Motore assente → exit non-zero, id `engine.binary`.
- [ ] Registry vuoto → exit non-zero, id `executors.registry_empty`.
- [ ] `ffprobe` assente → exit non-zero, id `tools.ffprobe`.
- [ ] Porta occupata → exit non-zero, id `ports.health` o `ports.metrics`.
- [ ] Disk free < soglia → exit non-zero, id `disk.free`.
- [ ] JSON output contiene almeno `{verdict, checks[], errors[]}` con `code + component + remedy`.

---

## 5. Test obbligatori

- Unit test per ogni sotto-validatore (`pkg/doctor/*_test.go`).
- Integrazione con FS temporaneo (uso `t.TempDir()`).
- Integrazione con porta occupata (`net.Listen`, retries).
- Integrazione con `MAXActiveJobs=0` o `Environment`=production senza TLS.
- Golden test su JSON output (snapshot).
- Esecuzione di `--validate-config` con `Env::production` e bin mancante ⇒ exit 1 con id `executors.scene_composite_v1` non trovato.

---

## 6. Evidenze

- Run `--validate-config --json` su 5 scenari (config valida, TLS parziale, disk pieno, engine assente, registry vuoto).
- Diff JSON tra due release (verifica stabilità schema).
- Report di coverage dei 10 sotto-validatori.
