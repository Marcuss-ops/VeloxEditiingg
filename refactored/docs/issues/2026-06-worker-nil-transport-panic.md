# Bug: worker process panics with nil-pointer dereference when transport_factory returns (nil, err)

- **Status**: Open
- **Component**: `RemoteCodex/native/worker-agent-go`
- **Severity**: High — restart-loop, denial of service for one worker
- **Affected version**: `main` (last reproduced 2026-06-19 against `velox-worker-local`)
- **Reproducible**: yes
- **Reporter**: operator / Buffy via deploy/ reproduction (deploy/scripts/apply-local-worker-config.sh path)

## Where

- `internal/worker/worker.go:24` — operator-observed panic stack frame (older source revision);
  the **current** source has the call sites at `worker.go:43` (`w.transport.Connect(ctx, hello)`)
  and `worker.go:48` (`_ = w.transport.Close()`), both call methods on the nil interface value.
- `internal/worker/worker_init.go:62-66` — **root cause**: the `transportFactory` closure
  silently returns `nil` when `newControlTransport` returns `(nil, err)`.

## Reproduction

Using `deploy/scripts/apply-local-worker-config.sh` (introduced together with this bug report):

1. Render a worker config WITHOUT `--allow-insecure-grpc` (script default), which produces
   `allow_insecure_grpc_dev: false` in the JSON. Combine with NO TLS files on disk. This is the
   default for any operator who has not opted into the two-key double-consent
   (`VELOX_ALLOW_INSECURE_GRPC_DEV=true` env + `allow_insecure_grpc_dev: true` JSON).
2. Render + force-recreate the worker container.
3. Inspect `docker logs velox-worker-velox-worker-local`. Observe, in sequence:
   ```
   [INFO]  [velox-worker-local] [CONNECT] Initializing worker…
   [ERROR] [velox-worker-local] [INIT] initial transport setup failed: transport factory: no TLS configured.
   panic: runtime error: invalid memory address or nil pointer dereference [worker.go:24]
   ```
   The container exits with code 2; compose's `restart: unless-stopped` re-spawns it,
   producing an infinite restart loop.

## Expected

- The worker process exits with a meaningful non-zero exit code (rc=1), NOT a Go panic.
- The operator sees the structured error in a single line of stderr, e.g.:
  `[ERROR] [INIT] transport factory: no TLS configured. Set tls_cert_file|tls_key_file|tls_ca_file, or enable allow_insecure_grpc_dev=true only for local development`.
- No subsequent restart loop — `restart: unless-stopped` does not retry because the exit-code-based
  healthcheck fails on the very first attempt, not after a fast panic cycle.

## Actual

1. `worker_init.go:64` — `newControlTransport` returns `(nil, err)`. The closure at lines 62-66
   logs the error and returns `nil`. The nil is assigned to `w.transport`.
2. `worker.go:43` — `if err := w.transport.Connect(ctx, hello); err != nil` — calling a method
   on a nil interface pointer; Go runtime immediately panics with
   `invalid memory address or nil pointer dereference`.
3. Container exits rc=2. Compose retries. Same panic every cycle.

The Go runtime attributes the panic to `worker.go:24`, which is the `Start()` function
declaration. Earlier source revisions had a non-trivial statement at that line; the **current**
source has both Connect and Close as nil-vulnerable call sites.

## Root cause

Two collaborating places:

1. **`worker_init.go:62-66`** — the factory closure silently returns `nil` on err, breaking the
   contract that `newTransport()` returns a usable, non-nil transport. The earlier patch on
   `initialTransport` at lines 71-78 handles only the very first attempt; the closure still
   returns nil silently on every retry, so the reconnect loop reads nil into `w.transport` and
   hits the panic on the very first inner-loop attempt.

2. **`worker.go:41-48`** — `Start()` reads `w.newTransport()` into `w.transport` WITHOUT nil-checking,
   then calls `w.transport.Connect(...)` directly. There is no defensive `if w.transport == nil { return ... }` guard. Calling a method on a nil interface value dereferences the itab/type pointer and
   hard-crashes the goroutine before any defer can recover.

`transport_factory.go:30-37` lists the failure modes that produce `(nil, err)`:
- `cfg.ControlGRPCURL == ""`
- partial TLS triple (cert/key/ca missing some)
- `cfg.AllowInsecureGRPC` set without `VELOX_ALLOW_INSECURE_GRPC_DEV=true`
- no TLS configured at all (the case we just hit on `velox-worker-local`)

`pkg/config/config.go:WorkerConfig.Validate()` only checks `master_url`, `worker_id`, `work_dir`,
`control_grpc_url`, and `log_level`. It does **not** cross-check that `transport_factory` can
actually build a transport from the validated config — a separate cross-check is a reasonable
followup.

## Suggested fix

Two layered changes (worth landing together):

### Option A — preferred: change `New` to return `(*Worker, error)`

Make the constructor propagate the transport-init failure, since a `*Worker` without a transport
is meaningless:

```go
// worker_init.go (signature change)
func New(cfg *config.WorkerConfig, version string) (*Worker, error) {
    ...
    initialTransport, err := newControlTransport(cfg, log)
    if err != nil {
        log.Error("[INIT] initial transport setup failed: %v", err)
        return nil, fmt.Errorf("transport factory: %w", err)
    }
    ...
    return w, nil
}
```

```go
// cmd/velox-worker-agent/main.go (caller update)
w, err := worker.New(cfg, resolvedVersion)
if err != nil {
    logger.LogRegisterFailed("(initial)", cfg.MasterURL, err)
    os.Exit(1)
}
if startErr := w.Start(ctx); startErr != nil {
    logger.LogRegisterFailed(w.config.WorkerID, cfg.MasterURL, startErr)
    os.Exit(1)
}
```

This is the cleaner long-term fix. All call sites get explicit error handling — no silent
panics on bad config.

### Option B — minimal: nil-guard inside `Start()`

If changing the public signature churns a lot of tests, the minimal diff is:

```go
// worker.go:41-48
w.transport = w.newTransport()
if w.transport == nil {
    err := fmt.Errorf("transport factory returned nil; check the [INIT] error line above and fix config before retry")
    w.logger.Error("%v", err)
    return err
}
w.setConnState(ConnConnecting)
w.connFailureCount = 0

hello := w.buildHello()
if err := w.transport.Connect(ctx, hello); err != nil {
    ...
    _ = w.transport.Close()  // still safe (transport is non-nil at this point)
    ...
}
```

Do **not** ship Option B alone — without also addressing the factory closure's silent-nil
behavior, every reconnect iteration re-derives `w.transport = nil` and the operator gets the
same error redundantly. Pair with Option A, or with Option C below.

### Option C — preferred companion: keep the factory closure logging

```go
// worker_init.go:62-66 (no behavior change, comment polish)
transportFactory := func() controltransport.ControlTransport {
    t, err := newControlTransport(cfg, log)
    if err != nil {
        // Operationally distinguishing error reasons helps operators
        // diagnose between TLS / insecure-flag / partial-triple.
        log.Error("[INIT] transport factory rejected config: %v", err)
        return nil
    }
    return t
}
```

Mostly cosmetic — the change here is to keep returning nil with a single, structured error
log line. Combined with Option A or B, the operator sees the error ONCE on construction
rather than per-reconnect.

## Recommended combination

- **Land Option A** (signature change).
- **Land Option C** (single, well-formatted error log).
- **Defer Option B** until proven necessary (it is a safety net layered only if the
  new signature is somehow bypassed).

## Repro test

Add to `internal/worker/worker_init_test.go` (`pkg/config` import via existing helpers). With
Option A landed, the test is:

```go
func TestNewReturnsErrorOnBadTLS(t *testing.T) {
    cfg := &config.WorkerConfig{
        WorkerID:          "test",
        WorkerName:        "test",
        WorkDir:           t.TempDir(),
        LogLevel:          "info",
        HealthPort:        8081,
        MasterURL:         "http://localhost:8000",
        ControlGRPCURL:    "localhost:9000",
        // No TLS files. No insecure flag. Expect newControlTransport to fail.
    }
    _, err := worker.New(cfg, "test")
    require.Error(t, err, "expected New() to surface transport init error")
    require.Contains(t, err.Error(), "transport factory")
}
```

If Option B is shipped without A, the test should be:

```go
func TestStartDoesNotPanicOnNilTransport(t *testing.T) {
    cfg := &config.WorkerConfig{ /* same fields as above */ }
    w := worker.New(cfg, "test")
    require.NotPanics(t, func() {
        _ = w.Start(context.Background())
    })
}
```

## Acceptance criteria

- [ ] Worker process exits with `rc=1` (NOT a Go panic) when transport setup fails.
- [ ] The structured `[INIT]` error line is the LAST log line before exit.
- [ ] Repro test is added and green in CI.
- [ ] `cmd/velox-worker-agent/main.go` updated to handle `(*Worker, error)`.
- [ ] No regression: the existing happy path
      (`allow_insecure_grpc_dev: true` + `VELOX_ALLOW_INSECURE_GRPC_DEV=true` env)
      still produces `[CONNECT] Registration successful — running session`.

## Related context

- `pkg/config/config.go:WorkerConfig.Validate()` — pre-flight transport validation could be
  added as a followup so misconfigurations surface at `Validate()` time rather than transport
  construction time.
- `internal/transport/grpc_stream.go` — would also benefit from an explicit
  `if t == nil { return controltransport.ErrNotConnected }` on `Send`/`Receive`/`Close` so
  future callers cannot panic on a returned-nil transport from any factory.
- `deploy/scripts/apply-local-worker-config.sh` — already runs `docker run --rm ...
  --validate-config` post-fix. Once Option A lands, the `--validate-config` flag can call
  `worker.New` itself (catching transport misconfiguration without spinning a container),
  eliminating the rc=2 WARN-only path the script currently uses.
