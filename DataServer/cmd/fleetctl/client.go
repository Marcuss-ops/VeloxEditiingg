// client.go — thin HTTP client fleetctl uses to call the Master
// REST API (step 1/15 + step 6/15 + step 10/15 + step 12/15).
//
// Construction-tolerant of:
//   - nil baseURL     → loadClientConfig flags it as misuse (exit 2).
//   - empty token     → loadClientConfig flags it as misuse (exit 2).
//   - non-2xx HTTP    → Caller's handlers map to canonical exit
//                       codes (4 / 5 / 6 / 7 / 8 / 1) per Q3 of
//                       the design review.
//
// All requests carry Authorization: Bearer <token>. Body shape
// for POST mutations matches the Step 6/15 + Step 12/15 handler
// schemas (controller reads {reason, requested_by} or {asset_id,
// render_plan, timeout_sec, reason}).
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

// clientConfig carries the values resolved by loadClientConfig.
// Construct exclusively via loadClientConfig — direct struct
// literal is reserved for tests.
type clientConfig struct {
	MasterURL string // e.g. https://velox-master.example.com:8000 (no trailing slash)
	Token     string // bare bearer token (Authorization: Bearer <Token>)
	Verbose   bool   // --verbose: dump request/response for debugging
}

// loadClientConfig parses the args AFTER the sub-command (which
// has already been removed by runMain) PLUS env var fallback.
// Resolution precedence: --token-file > $VELOX_ADMIN_TOKEN >
// /opt/velox/secrets/admin-token. --master flag, then $VELOX_MASTER_URL.
//
// On any resolution failure, returns an error + the caller
// surfaces exit code ExitMisuse (2).
func loadClientConfig(args []string) (*clientConfig, error) {
	fs := flag.NewFlagSet("fleetctl-cfg", flag.ContinueOnError)
	masterURL := fs.String("master", "", "Master URL (e.g. https://velox.example.com:8000)")
	tokenFile := fs.String("token-file", "", "Path to chmod-600 file holding bearer token")
	verbose := fs.Bool("verbose", false, "Dump request + response body to stderr (debug)")
	if err := fs.Parse(args); err != nil {
		return nil, fmt.Errorf("flag parse: %w", err)
	}
	// Master URL resolution: --master flag, then $VELOX_MASTER_URL
	// env, then deploy/group_vars/all.yml's default-style
	// placeholder (returns mis-use if all are empty).
	if *masterURL == "" {
		*masterURL = os.Getenv("VELOX_MASTER_URL")
	}
	if *masterURL == "" {
		return nil, errors.New("master URL required: pass --master=https://HOST:8000 or set VELOX_MASTER_URL")
	}
	if strings.HasSuffix(*masterURL, "/") {
		*masterURL = strings.TrimRight(*masterURL, "/")
	}
	// Token resolution: --token-file, then $VELOX_ADMIN_TOKEN env,
	// then canonical Master-side file at /opt/velox/secrets/admin-token.
	tok, err := resolveToken(*tokenFile)
	if err != nil {
		return nil, err
	}
	return &clientConfig{
		MasterURL: *masterURL,
		Token:     tok,
		Verbose:   *verbose,
	}, nil
}

func resolveToken(explicitFile string) (string, error) {
	paths := []string{}
	if explicitFile != "" {
		paths = append(paths, explicitFile)
	}
	if v := os.Getenv("VELOX_ADMIN_TOKEN"); v != "" {
		return strings.TrimSpace(v), nil
	}
	paths = append(paths, "/opt/velox/secrets/admin-token")
	for _, p := range paths {
		raw, err := os.ReadFile(p)
		if err != nil {
			continue
		}
		tok := strings.TrimSpace(string(raw))
		if tok != "" {
			return tok, nil
		}
	}
	return "", errors.New("admin token not found: provide --token-file=PATH, $VELOX_ADMIN_TOKEN env, or /opt/velox/secrets/admin-token")
}

// fleetClient is the typed HTTP surface. The HTTP transport is
// injectable via the RoundTripper field for tests (see
// fleetctl_test.go).
type fleetClient struct {
	baseURL string
	token   string
	verbose bool
	http    *http.Client
}

func newFleetClient(cfg *clientConfig) (*fleetClient, error) {
	return &fleetClient{
		baseURL: cfg.MasterURL,
		token:   cfg.Token,
		verbose: cfg.Verbose,
		http:    &http.Client{Timeout: 30 * time.Second},
	}, nil
}

// doJSON sends a request + decodes the response body into `out`.
// Returns:
//
//   (status, err)
//
//     status=HTTP code, err=nil on success AND on non-2xx
//       (caller maps status via MapHTTPStatusToOpExit)
//     status=HTTP code, err=non-nil on decode failure (2xx with bad JSON)
//     status=0, err=non-nil on request build / transport failure
//
// Callers map non-2xx to canonical exit codes per exit_codes.go.
func (c *fleetClient) doJSON(ctx context.Context, method, path string, body any, out any) (int, error) {
	var reqBody io.Reader
	if body != nil {
		bs, err := json.Marshal(body)
		if err != nil {
			return 0, fmt.Errorf("marshal request body: %w", err)
		}
		reqBody = bytes.NewReader(bs)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, reqBody)
	if err != nil {
		return 0, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.verbose {
		fmt.Fprintf(os.Stderr, "[fleetctl] -> %s %s\n", method, path)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, fmt.Errorf("transport: %w", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if c.verbose {
		fmt.Fprintf(os.Stderr, "[fleetctl] <- %d %s\n", resp.StatusCode, string(raw))
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		// Return only the status code — no error. Callers check
		// status != 200 and map to canonical exit codes via
		// MapHTTPStatusToOpExit. Returning an error here causes
		// runInspect/runStatus/runMutation to short-circuit to
		// ExitUnexpected before ever evaluating the status code.
		return resp.StatusCode, nil
	}
	if out != nil {
		if err := json.Unmarshal(raw, out); err != nil {
			return resp.StatusCode, fmt.Errorf("decode response: %w (body=%s)", err, snippet(raw, 256))
		}
	}
	return resp.StatusCode, nil
}

// snippet returns the first n bytes of s, ellipsizing if
// truncated. Used in error messages so a 4KB HTML error page
// doesn't blow up the operator's terminal.
func snippet(s []byte, n int) string {
	if len(s) <= n {
		return string(s)
	}
	return string(s[:n]) + "…"
}

// Guard for unused imports when the file is trimmed down. Kept
// as a no-op expression so the import set stays stable.
var _ = context.Background
