package telemetry

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"sync/atomic"
	"time"
)

var (
	healthStartTime  = time.Now()
	healthWorkerID   atomic.Value
	healthRegistered atomic.Bool
)

// SetHealthWorkerID sets the worker ID reported by the health endpoint.
func SetHealthWorkerID(id string) {
	healthWorkerID.Store(id)
}

// SetHealthRegistered marks the worker as registered with the master.
func SetHealthRegistered(registered bool) {
	healthRegistered.Store(registered)
}

// HealthResponse is the JSON payload returned by GET /health.
type HealthResponse struct {
	Status     string `json:"status"`
	WorkerID   string `json:"worker_id,omitempty"`
	Registered bool   `json:"registered"`
	UptimeSec  int64  `json:"uptime_sec"`
}

// StartHealthServer starts a minimal HTTP server on the given port that
// responds to GET /health with a JSON status payload.
func StartHealthServer(port int) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		resp := HealthResponse{
			Status:     "ok",
			Registered: healthRegistered.Load(),
			UptimeSec:  int64(time.Since(healthStartTime).Seconds()),
		}
		if id, ok := healthWorkerID.Load().(string); ok {
			resp.WorkerID = id
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	})

	addr := fmt.Sprintf(":%d", port)
	server := &http.Server{Addr: addr, Handler: mux}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", addr, err)
	}

	go func() {
		if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
			fmt.Printf("[HEALTH] Health server error: %v\n", err)
		}
	}()

	return nil
}
