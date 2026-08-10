package statusboundary

import (
	"testing"

	"velox-server/internal/deliveries"
	"velox-server/internal/jobs"
	"velox-server/internal/publicationstate"
	"velox-shared/contract"
)

func TestParsersNormalizeOnlyTheirOwnDomain(t *testing.T) {
	if got, ok := ParseInputAssembly(" completed "); !ok || got != contract.InputAssemblyCompleted {
		t.Fatalf("ParseInputAssembly = %q, %v", got, ok)
	}
	if got, ok := ParseJob("succeeded"); !ok || got != jobs.StatusSucceeded {
		t.Fatalf("ParseJob = %q, %v", got, ok)
	}
	if got, ok := ParseDelivery(" succeeded "); !ok || got != deliveries.DeliverySucceeded {
		t.Fatalf("ParseDelivery = %q, %v", got, ok)
	}
	if got, ok := ParsePublication("published"); !ok || got != publicationstate.Published {
		t.Fatalf("ParsePublication = %q, %v", got, ok)
	}
}

func TestParsersRejectCrossDomainValues(t *testing.T) {
	if _, ok := ParseInputAssembly("SUCCEEDED"); ok {
		t.Fatal("SUCCEEDED must not parse as InputAssemblyStatus")
	}
	if _, ok := ParseInputAssembly("PUBLISHED"); ok {
		t.Fatal("PUBLISHED must not parse as InputAssemblyStatus")
	}
	if _, ok := ParseJob("completed"); ok {
		t.Fatal("completed must not parse as JobStatus")
	}
	if _, ok := ParseJob("PUBLISHED"); ok {
		t.Fatal("PUBLISHED must not parse as JobStatus")
	}
	if _, ok := ParseDelivery("PUBLISHED"); ok {
		t.Fatal("PUBLISHED must not parse as DeliveryStatus")
	}
	if _, ok := ParsePublication("SUCCEEDED"); ok {
		t.Fatal("SUCCEEDED must not parse as PublicationStatus")
	}
}

func TestCompletedCannotBeCastIntoLifecycleTerminalDomains(t *testing.T) {
	completed := contract.InputAssemblyCompleted
	if jobs.JobStatus(completed).Valid() {
		t.Fatal("completed must not be a valid JobStatus")
	}
	if deliveries.DeliveryStatus(completed).Valid() {
		t.Fatal("completed must not be a valid DeliveryStatus")
	}
	if publicationstate.PublicationStatus(completed).Valid() {
		t.Fatal("completed must not be a valid PublicationStatus")
	}
	if contract.InputAssemblyStatus("SUCCEEDED").Valid() {
		t.Fatal("SUCCEEDED must not be accepted as InputAssemblyStatus")
	}
	if contract.InputAssemblyStatus("PUBLISHED").Valid() {
		t.Fatal("PUBLISHED must not be accepted as InputAssemblyStatus")
	}
}

func TestWireValuesRemainStrings(t *testing.T) {
	for _, tc := range []struct {
		name string
		got  string
		want string
	}{
		{"input assembly", string(contract.InputAssemblyCompleted), "completed"},
		{"job", string(jobs.StatusSucceeded), "SUCCEEDED"},
		{"delivery", string(deliveries.DeliverySucceeded), "SUCCEEDED"},
		{"publication", string(publicationstate.Published), "PUBLISHED"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.got != tc.want {
				t.Fatalf("wire value = %q, want %q", tc.got, tc.want)
			}
		})
	}
}
