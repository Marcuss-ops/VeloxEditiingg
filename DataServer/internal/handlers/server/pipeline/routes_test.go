package pipeline

import (
	"net/http"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestRegisterRoutesRetiresLegacyPipelineSurfaces(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	noopAuth := func(c *gin.Context) { c.Next() }

	(&Handlers{}).RegisterRoutes(r, noopAuth, noopAuth)

	registered := make(map[string]bool)
	for _, route := range r.Routes() {
		registered[route.Method+" "+route.Path] = true
	}

	for _, legacy := range []string{
		http.MethodPost + " /api/script-simple",
		http.MethodPost + " /api/script-multiple",
		http.MethodPost + " /api/remote/pipeline/generate",
		http.MethodGet + " /api/remote/pipeline/status/:trace_id",
		http.MethodDelete + " /api/remote/pipeline/cancel/:trace_id",
	} {
		if registered[legacy] {
			t.Errorf("retired route remains registered: %s", legacy)
		}
	}

	for _, canonical := range []string{
		http.MethodPost + " /api/v1/jobs",
		http.MethodPost + " /api/v1/jobs/batch",
	} {
		if !registered[canonical] {
			t.Errorf("canonical route is not registered: %s", canonical)
		}
	}

	for _, retired := range []string{
		http.MethodPost + " /api/v1/pipeline-runs",
		http.MethodGet + " /api/v1/pipeline-runs/:id",
		http.MethodPost + " /api/v1/pipeline-runs/:id/cancel",
		http.MethodPost + " /api/v1/pipeline-runs/:id/retry",
		http.MethodGet + " /api/v1/pipeline-runs/:id/timeline",
		http.MethodGet + " /api/v1/pipeline-runs/:id/artifacts",
		http.MethodGet + " /api/v1/pipeline-runs/:id/deliveries",
	} {
		if registered[retired] {
			t.Errorf("retired pipeline-run route remains registered: %s", retired)
		}
	}
}
