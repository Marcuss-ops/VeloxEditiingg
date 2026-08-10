package creatorflow

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"

	"velox-shared/contract/deliveryplan"
)

func TestWriteResolverError_DeliveryTargetRequired(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)

	WriteResolverError(ctx, deliveryplan.NewDeliveryTargetRequiredError())

	if recorder.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status=%d, want %d", recorder.Code, http.StatusUnprocessableEntity)
	}
	body := recorder.Body.String()
	for _, want := range []string{"DELIVERY_TARGET_REQUIRED", "an explicit Drive destination is required", "delivery_plan", "required"} {
		if !contains(body, want) {
			t.Fatalf("response %q does not contain %q", body, want)
		}
	}
}

func contains(value, want string) bool {
	return len(want) == 0 || (len(value) >= len(want) && indexOf(value, want) >= 0)
}

func indexOf(value, want string) int {
	for i := 0; i+len(want) <= len(value); i++ {
		if value[i:i+len(want)] == want {
			return i
		}
	}
	return -1
}
