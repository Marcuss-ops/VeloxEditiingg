// Package metrics / handler.go
//
// HTTP surface for the master-side Prometheus exporter. The handler is
// registered onto the master mux at /metrics with the canonical
// text/plain; version=0.0.4 content type.
package metrics

import (
	"net/http"
)

// Handler returns the canonical HTTP handler. Wired into the master
// router inside cmd/server/bootstrap.go as a route on the public
// listen port (operator-scoped scrape; gated by Helm-level auth at
// the ingress).
func (r *Registry) Handler() http.Handler {
	return r.HTTPHandler()
}
