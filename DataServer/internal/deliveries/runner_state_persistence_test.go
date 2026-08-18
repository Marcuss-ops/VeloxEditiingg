package deliveries

import (
	"context"
	"errors"
	"testing"

	"velox-server/internal/deliverystore"
	"velox-server/internal/publicationstate"
	"velox-server/internal/supervisor"
)

func TestPhaseFailurePropagatesStatePersistenceFailure(t *testing.T) {
	db := openDeliveryTestDB(t)
	if err := db.Close(); err != nil {
		t.Fatalf("close test database: %v", err)
	}

	runner := NewDeliveryRunner(nil, nil, db.Delivery(), db, "state-persistence-test")
	providerErr := errors.Join(ErrProviderPermanent, errors.New("provider rejected publication"))

	err := runner.phaseFailure(
		context.Background(),
		deliverystore.DeliveryLease{AttemptNumber: 1},
		"publication-missing-after-db-close",
		publicationstate.Uploading,
		"upload_media:operation",
		providerErr,
	)
	if err == nil {
		t.Fatal("phaseFailure returned nil after every state write failed")
	}
	if !errors.Is(err, ErrProviderPermanent) {
		t.Fatalf("phaseFailure lost provider error: %v", err)
	}
	if !errors.Is(err, errDeliveryStatePersistence) {
		t.Fatalf("phaseFailure lost state persistence error: %v", err)
	}
	if !errors.Is(err, supervisor.ErrInfrastructure) {
		t.Fatalf("phaseFailure did not classify state persistence as infrastructure: %v", err)
	}
}
