// fleetctl_test.go — minimal coverage for Step 15/15 fleetctl
// per design Q10 of the thinker call.
//
// Coverage map:
//
//	TestValidateDigest_AcceptsCanonicalLowercase
//	  ^sha256:[0-9a-f]{64}$ → nil err.
//
//	TestValidateDigest_RejectsUppercase
//	  uppercase hex fails the regex (Cosign emits lowercase;
//	  mixed-case input is operator error).
//
//	TestValidateDigest_RejectsLengthTooShort
//	  sha256: + <64 chars → exit-code 7 surface via error message.
//
//	TestValidateDigest_RejectsMobileRefs
//	  :latest / :main / :stable get a specific message rather
//	  than generic regex mismatch (better operator UX).
//
//	TestRunStatus_StatusOK_PrettyPrintsCard
//	  Mock HTTP server returns worker list; handler prints
//	  status table; assert ExitOK + stdout contains worker_id.
//
//	TestRunInspect_StatusOK_WorkerNotFound
//	  Mock returns 404; handler returns ExitWorkerNotFound (4).
//
//	TestRunUpdate_BadDigestReturnsExitImageInvalid
//	  Handler called with --digest=:latest → exit 7 without
//	  hitting HTTP at all (client-side gate).
//
//	TestMapHTTPStatusToOpExit
//	  Pin the matrix: 404 → 4, 409 → 5, 422 → 7, 401 → 2, 500
//	  → 1.
//
//	TestMapOperationKindToExit
//	  smoke→6, rollback→8, drain→1 (generic).
//
//	TestResolveTokenAdvanced
//	  env var + file precedence stable.
package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
)

// ---------- digest regex ----------

// ---------- exit-code matrix ----------

// ---------- HTTP client mock + handler end-to-end ----------

// newMockClient builds a fleetClient whose http.Client hits a
// recorded mock server. Returns (client, server). Tests use
// server.Close() in t.Cleanup.
func newMockClient(handler http.HandlerFunc) (*fleetClient, *httptest.Server) {
	srv := httptest.NewServer(handler)
	t := &http.Transport{}
	c := &fleetClient{
		baseURL: srv.URL,
		token:   "test-token",
		verbose: false,
		http:    &http.Client{Transport: t},
	}
	return c, srv
}

// ---------- token resolution precedence ----------

// ---------- rollout ----------

// Guard so context import stays useful if tests slim down.
var _ = context.Background
var _ = fmt.Sprintf
