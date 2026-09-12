// package_native_test.go is a compile-only stub for
// RemoteCodex/native/worker-agent-go/pkg/video/services/native.
//
// Purpose: prior to this stub, the package had no *_test.go files, so
// `go test -race -count=1 ./pkg/video/services/native/...` returned
// `ok pkg 0.000s`, hiding whether the split files were still in the
// test compile graph. The package-level references below make a future
// split-file rename a compile failure, while the regression report now
// records sub-second durations in milliseconds.
//
// This stub gives the package an explicit compile-time wiring check without
// introducing behavioural coverage:
//
//  1. The package-level `_ = …` references below force every split
//     file (binary_resolver, engine_process, engine_sidecar,
//     engine_progress) to be in the test binary's compile graph. If
//     a future refactor extracts or renames any of those files,
//     this test fails to COMPILE — a much louder signal than a
//     package that silently reports no test coverage.
//
//  2. The single TestSplitWiresExecute function keeps the compile-only
//     invariant explicit without adding artificial wall-clock delay.
//
// The stub is intentionally white-box (`package native`) because the
// 4 splits carry UNEXPORTED symbols (resolveBinary, runEngineProcess,
// engineSidecar, streamEngineOutput, etc.). A black-box
// `package native_test` would not be able to reference them.
package native

import (
	"testing"
)

// Compile-only references: one symbol per split file. Removes the
// stale "no test file matched the default tag set" silent-success
// path and makes future split renames a hard compile error.
var (
	_ = resolveBinary      // binary_resolver.go
	_ = runEngineProcess   // engine_process.go
	_ = engineSidecar{}    // engine_sidecar.go (type)
	_ = streamEngineOutput // engine_progress.go
)

// TestSplitWiresExecute is the only test in this package. Its body is
// intentionally empty: the package-level references above prove that
// all four split files compile together via this test package.
func TestSplitWiresExecute(t *testing.T) {
	_ = t
}
