package outbox

import (
	"context"
	"errors"
	"fmt"
	"os"
	osExec "os/exec"
	"sync"
	"testing"

	"velox-server/internal/alerts"
)

type countingAlertNotifier struct {
	mu    sync.Mutex
	calls int
}

type failingAlertNotifier struct{ err error }

func (n failingAlertNotifier) Notify(_ context.Context, _ alerts.Alert) error {
	return n.err
}

func (n *countingAlertNotifier) Notify(_ context.Context, _ alerts.Alert) error {
	n.mu.Lock()
	n.calls++
	n.mu.Unlock()
	return nil
}

func TestAlertNotifierNilResetPreservesNopDefault(t *testing.T) {
	SetAlertNotifier(nil)
	t.Cleanup(func() { SetAlertNotifier(nil) })

	if _, ok := AlertNotifier().(alerts.NopNotifier); !ok {
		t.Fatalf("AlertNotifier after nil reset = %T, want alerts.NopNotifier", AlertNotifier())
	}
}

func TestRegisterHandlerFactoryAfterProductionRegistry(t *testing.T) {
	if os.Getenv("VELOX_OUTBOX_LATE_FACTORY_HELPER") == "1" {
		_ = ProductionRegistry()
		defer func() {
			if recovered := recover(); recovered == nil {
				t.Fatal("RegisterHandlerFactory after ProductionRegistry did not panic")
			}
		}()
		RegisterHandlerFactory(func(*Registry) {})
		return
	}
	if os.Getenv("VELOX_OUTBOX_EARLY_FACTORY_HELPER") == "1" {
		const eventType = "TEST_EARLY_FACTORY"
		RegisterHandlerFactory(func(reg *Registry) {
			MustRegisterFunc(reg, eventType, func(context.Context, Event) error { return nil })
		})
		reg := ProductionRegistry()
		if _, err := reg.Lookup(eventType); err != nil {
			t.Fatalf("factory registered before ProductionRegistry was not applied: %v", err)
		}
		return
	}

	cmd := osExec.Command(os.Args[0], "-test.run=^TestRegisterHandlerFactoryAfterProductionRegistry$", "-test.v")
	cmd.Env = append(os.Environ(), "VELOX_OUTBOX_LATE_FACTORY_HELPER=1")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("isolated late-factory contract subprocess failed: %v\n%s", err, output)
	}

	cmd = osExec.Command(os.Args[0], "-test.run=^TestRegisterHandlerFactoryAfterProductionRegistry$", "-test.v")
	cmd.Env = append(os.Environ(), "VELOX_OUTBOX_EARLY_FACTORY_HELPER=1")
	output, err = cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("isolated early-factory contract subprocess failed: %v\n%s", err, output)
	}
}

func TestAlertNotifierConcurrentSetAndGet(t *testing.T) {
	SetAlertNotifier(nil)
	t.Cleanup(func() { SetAlertNotifier(nil) })

	const iterations = 128
	configured := &countingAlertNotifier{}
	var wg sync.WaitGroup
	wg.Add(iterations * 2)
	for i := 0; i < iterations; i++ {
		go func() {
			defer wg.Done()
			SetAlertNotifier(configured)
		}()
		go func() {
			defer wg.Done()
			n := AlertNotifier()
			if n == nil {
				t.Errorf("concurrent AlertNotifier returned nil")
			}
		}()
	}
	wg.Wait()

	if got := AlertNotifier(); got != configured {
		t.Fatalf("final AlertNotifier = %T, want configured notifier", got)
	}
}

func TestProductionJOBFailedHandlerRetriesNotifierFailure(t *testing.T) {
	sinkErr := errors.New("alert sink unavailable")
	SetAlertNotifier(failingAlertNotifier{err: sinkErr})
	t.Cleanup(func() { SetAlertNotifier(nil) })

	h, err := ProductionRegistry().Lookup("JOB_FAILED")
	if err != nil {
		t.Fatalf("lookup JOB_FAILED handler: %v", err)
	}

	err = h.Handle(context.Background(), Event{
		EventID:   "evt-job-failed-1",
		EventType: "JOB_FAILED",
		Payload:   []byte(fmt.Sprintf(`{"job_id":%q,"error_code":"JOB_FAILED_GENERIC","error":"render failed"}`, "job-1")),
	})
	if !errors.Is(err, sinkErr) {
		t.Fatalf("JOB_FAILED handler error = %v, want sink error", err)
	}
	var handlerErr *HandlerError
	if !errors.As(err, &handlerErr) || !handlerErr.Transient {
		t.Fatalf("JOB_FAILED handler error = %T/%v, want transient HandlerError", err, err)
	}
}
