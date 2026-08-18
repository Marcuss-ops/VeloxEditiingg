package deliveries

// runner_contract_test.go — white-box contract test that pins the
// DeliveryRunner persistence split.
//
// dbStore MUST be the consumer-owned DeliveryStore interface (the runner
// defines the seam it depends on), never *store.SQLiteStore or a concrete
// *deliverystore.SQLiteDeliveryStore. This is what lets the runner be
// tested against a fake DeliveryStore without SQLite. store is the
// deliberately separate *store.SQLiteStore port for publication-state and
// artifact reads, which belong to other domains and are NOT part of the
// delivery persistence contract.
//
// The reflection assertions below fail at test time (not silently at
// compile time) if either field drifts, so a future refactor that widens
// dbStore back to the concrete store is caught in CI.

import (
	"reflect"
	"testing"

	"velox-server/internal/deliverystore"
	"velox-server/internal/store"
)

// Compile-time anchor: the production leaf must keep satisfying the
// consumer-owned seam. If this ever stops compiling, the composition root
// (bootstrap_modules.go) has drifted from the DeliveryStore contract.
var _ DeliveryStore = (*deliverystore.SQLiteDeliveryStore)(nil)

func TestDeliveryRunnerPersistencePortsAreContract(t *testing.T) {
	typ := reflect.TypeOf(DeliveryRunner{})

	// dbStore must remain the consumer-owned DeliveryStore interface.
	dbField, ok := typ.FieldByName("dbStore")
	if !ok {
		t.Fatal("DeliveryRunner.dbStore field not found")
	}
	wantInterface := reflect.TypeOf((*DeliveryStore)(nil)).Elem()
	if dbField.Type != wantInterface {
		t.Fatalf("DeliveryRunner.dbStore type = %v, want the consumer-owned DeliveryStore interface %v (not *store.SQLiteStore or *deliverystore.SQLiteDeliveryStore)", dbField.Type, wantInterface)
	}

	// store must remain the separate *store.SQLiteStore port for
	// publication-state and artifact reads.
	storeField, ok := typ.FieldByName("store")
	if !ok {
		t.Fatal("DeliveryRunner.store field not found")
	}
	if storeField.Type != reflect.TypeOf((*store.SQLiteStore)(nil)) {
		t.Fatalf("DeliveryRunner.store type = %v, want *store.SQLiteStore (the publication/artifact read port)", storeField.Type)
	}
}
