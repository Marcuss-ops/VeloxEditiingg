package main

import (
	"context"
	"testing"
	"time"

	"velox-server/internal/fleet/opsalerts"
	"velox-server/internal/store"
	"velox-server/internal/supervisor"
)

type bootstrapOpsAlertsStore struct{}

func (bootstrapOpsAlertsStore) InsertAlertEvent(context.Context, store.AlertEvent) error { return nil }
func (bootstrapOpsAlertsStore) ResolveAlertEvent(context.Context, string, string, string, time.Time) error {
	return nil
}
func (bootstrapOpsAlertsStore) TouchActiveAlertEvent(context.Context, string, string, string, time.Time, string, string) error {
	return nil
}
func (bootstrapOpsAlertsStore) GetActiveAlertEventForWorkerRule(context.Context, string, string, string) (*store.AlertEvent, error) {
	return nil, store.ErrAlertEventNotFound
}

type bootstrapOpsAlertsSource struct{}

func (bootstrapOpsAlertsSource) WorkerIDs(opsalerts.CallCtx) ([]string, error) { return nil, nil }
func (bootstrapOpsAlertsSource) Snapshot(opsalerts.CallCtx, string) (*opsalerts.WorkerSnapshot, error) {
	return nil, nil
}

type typedNilBootstrapOpsAlertsSource struct{}

func (*typedNilBootstrapOpsAlertsSource) WorkerIDs(opsalerts.CallCtx) ([]string, error) {
	return nil, nil
}
func (*typedNilBootstrapOpsAlertsSource) Snapshot(opsalerts.CallCtx, string) (*opsalerts.WorkerSnapshot, error) {
	return nil, nil
}

func TestRegisterOpsAlertsSupervisorOmitsUnconfiguredCapability(t *testing.T) {
	sup := supervisor.New()
	status, err := registerOpsAlertsSupervisor(sup, bootstrapOpsAlertsStore{}, nil, supervisor.RestartPolicy{}, nil)
	if err != nil {
		t.Fatalf("registerOpsAlertsSupervisor with missing datasource: %v", err)
	}
	if status.State != opsalerts.CapabilityDisabled {
		t.Fatalf("status = %q, want %q", status.State, opsalerts.CapabilityDisabled)
	}

	for _, name := range sup.Names() {
		if name == "alerts-supervisor" {
			t.Fatal("alerts-supervisor must not be registered without a datasource")
		}
	}
}

func TestRegisterOpsAlertsSupervisorTreatsTypedNilDatasourceAsDisabled(t *testing.T) {
	sup := supervisor.New()
	var source *typedNilBootstrapOpsAlertsSource
	status, err := registerOpsAlertsSupervisor(sup, bootstrapOpsAlertsStore{}, source, supervisor.RestartPolicy{}, nil)
	if err != nil {
		t.Fatalf("typed-nil datasource: %v", err)
	}
	if status.State != opsalerts.CapabilityDisabled {
		t.Fatalf("status = %q, want %q", status.State, opsalerts.CapabilityDisabled)
	}
	for _, name := range sup.Names() {
		if name == "alerts-supervisor" {
			t.Fatal("alerts-supervisor must not be registered for typed-nil datasource")
		}
	}
}

func TestRegisterOpsAlertsSupervisorReportsMisconfiguredStore(t *testing.T) {
	sup := supervisor.New()
	status, err := registerOpsAlertsSupervisor(sup, nil, bootstrapOpsAlertsSource{}, supervisor.RestartPolicy{}, nil)
	if err == nil {
		t.Fatal("nil alert store must fail composition")
	}
	if status.State != opsalerts.CapabilityMisconfigured {
		t.Fatalf("status = %q, want %q", status.State, opsalerts.CapabilityMisconfigured)
	}
}

func TestRegisterOpsAlertsSupervisorRegistersReadyCapability(t *testing.T) {
	sup := supervisor.New()
	status, err := registerOpsAlertsSupervisor(sup, bootstrapOpsAlertsStore{}, bootstrapOpsAlertsSource{}, supervisor.RestartPolicy{}, nil)
	if err != nil {
		t.Fatalf("registerOpsAlertsSupervisor with ready dependencies: %v", err)
	}
	if status.State != opsalerts.CapabilityReady {
		t.Fatalf("status = %q, want %q", status.State, opsalerts.CapabilityReady)
	}
	found := false
	for _, name := range sup.Names() {
		if name == "alerts-supervisor" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("alerts-supervisor not registered; names=%v", sup.Names())
	}
}
