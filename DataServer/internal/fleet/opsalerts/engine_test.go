package opsalerts

import (
	"context"
	"errors"
	"testing"
	"time"

	"velox-server/internal/store"
)

type engineTestStore struct{}

func (engineTestStore) InsertAlertEvent(context.Context, store.AlertEvent) error { return nil }
func (engineTestStore) ResolveAlertEvent(context.Context, string, string, string, time.Time) error {
	return nil
}
func (engineTestStore) TouchActiveAlertEvent(context.Context, string, string, string, time.Time, string, string) error {
	return nil
}
func (engineTestStore) GetActiveAlertEventForWorkerRule(context.Context, string, string, string) (*store.AlertEvent, error) {
	return nil, store.ErrAlertEventNotFound
}

type typedNilEngineStore struct{}

func (*typedNilEngineStore) InsertAlertEvent(context.Context, store.AlertEvent) error { return nil }
func (*typedNilEngineStore) ResolveAlertEvent(context.Context, string, string, string, time.Time) error {
	return nil
}
func (*typedNilEngineStore) TouchActiveAlertEvent(context.Context, string, string, string, time.Time, string, string) error {
	return nil
}
func (*typedNilEngineStore) GetActiveAlertEventForWorkerRule(context.Context, string, string, string) (*store.AlertEvent, error) {
	return nil, store.ErrAlertEventNotFound
}

type engineTestSource struct{}

func (engineTestSource) WorkerIDs(CallCtx) ([]string, error) { return nil, nil }
func (engineTestSource) Snapshot(CallCtx, string) (*WorkerSnapshot, error) {
	return nil, nil
}

type typedNilEngineSource struct{}

func (*typedNilEngineSource) WorkerIDs(CallCtx) ([]string, error) { return nil, nil }
func (*typedNilEngineSource) Snapshot(CallCtx, string) (*WorkerSnapshot, error) {
	return nil, nil
}

func TestNewEngineRejectsNilDataSource(t *testing.T) {
	engine, err := NewEngine(engineTestStore{}, nil)
	if engine != nil {
		t.Fatal("nil datasource must not produce an engine")
	}
	if !errors.Is(err, ErrDataSourceNotConfigured) {
		t.Fatalf("error = %v, want ErrDataSourceNotConfigured", err)
	}
}

func TestNewEngineRejectsTypedNilDataSource(t *testing.T) {
	var source *typedNilEngineSource
	engine, err := NewEngine(engineTestStore{}, source)
	if engine != nil {
		t.Fatal("typed-nil datasource must not produce an engine")
	}
	if !errors.Is(err, ErrDataSourceNotConfigured) {
		t.Fatalf("error = %v, want ErrDataSourceNotConfigured", err)
	}
}

func TestNewEngineRejectsTypedNilAlertStore(t *testing.T) {
	var alertStore *typedNilEngineStore
	engine, err := NewEngine(alertStore, engineTestSource{})
	if engine != nil {
		t.Fatal("typed-nil alert store must not produce an engine")
	}
	if !errors.Is(err, ErrAlertStoreNotConfigured) {
		t.Fatalf("error = %v, want ErrAlertStoreNotConfigured", err)
	}
}

func TestNewEngineRejectsNilAlertStore(t *testing.T) {
	engine, err := NewEngine(nil, engineTestSource{})
	if engine != nil {
		t.Fatal("nil alert store must not produce an engine")
	}
	if !errors.Is(err, ErrAlertStoreNotConfigured) {
		t.Fatalf("error = %v, want ErrAlertStoreNotConfigured", err)
	}
}

func TestNewEngineWithClockReturnsReadyEngineWithDependencies(t *testing.T) {
	engine, err := NewEngineWithClock(engineTestStore{}, engineTestSource{}, time.Millisecond, 1)
	if err != nil {
		t.Fatalf("NewEngineWithClock: %v", err)
	}
	if engine == nil {
		t.Fatal("ready dependencies must produce an engine")
	}
	if engine.tick != time.Millisecond {
		t.Fatalf("tick = %s, want 1ms", engine.tick)
	}
	if engine.maxBatch != 1 {
		t.Fatalf("maxBatch = %d, want 1", engine.maxBatch)
	}
}
