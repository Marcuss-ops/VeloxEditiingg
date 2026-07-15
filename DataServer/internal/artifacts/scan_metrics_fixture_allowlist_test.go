package artifacts_test

import "path/filepath"

func init() {
	// e2e_metrics_worker_flow_test.go directly seeds a terminal Job only so
	// observability read-side metrics include one completed job. It does not
	// implement or exercise a production finalization writer; the canonical
	// runtime transition remains owned by the audited completion/finalization
	// transaction.
	allowedTestFiles[filepath.Join("internal", "store", "e2e_metrics_worker_flow_test.go")] = true
}
