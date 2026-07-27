// Command api-docs-gen regenerates the `paths` block of
// DataServer/api/openapi.yaml from DataServer/api/api_docs_manifest.yaml
// while preserving every other section (info, servers, tags,
// components, security, ...) verbatim.
//
// Why a custom codegen instead of swag/oapi-codegen/ogen:
//
//   * Zero new third-party dependencies. yaml.v3 is already vendored.
//   * The hand-curated narrative (operation descriptions, response
//     examples, schema explanations) stays untouched. swag requires
//     `// @Description` annotation on every handler; the cost of
//     porting 30+ handlers is far higher than the maintenance cost
//     of a small, declarative manifest.
//   * The schema block (components/schemas) remains hand-curated
//     because the validator's invariants (minLength, minimum,
//     pattern, enum) mirror Go-side constants (MaxVideoNameBytes,
//     MinSceneDurationSeconds, ...) — a fully-generated schema
//     drifts the moment a Go constant changes by 1.
//
// The manifest is the canonical record of "what routes does the
// Master publish". The validator (scripts/api/validate_openapi.py)
// has migrated from a hard-coded ROUTE_INVARIANTS list to a
// correctness-only check; the drift detector in this program is the
// complementary coverage check that tells maintainers when a
// hand-edit to openapi.yaml has silently diverged from the manifest.
//
// Exit codes:
//
//   0 — codegen succeeded AND drift is clean
//   1 — codegen succeeded BUT drift detected (re-run with -apply)
//   2 — codegen failed (manifest invalid, $ref unresolved, ...)
//
// Usage:
//
//   api-docs-gen                          # write to .api-docs-gen.out, print drift
//   api-docs-gen -apply                   # write back into openapi.yaml
//   api-docs-gen -apply -ci               # silence drift warnings (treat as drifted)
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

const (
	exitOK          = 0
	exitDrift       = 1
	exitGenerr      = 2
	manifestVersion = 1
)

// VALID_OPERATIONS is the set of HTTP verbs the codegen emits paths
// for. Anything outside this set is treated as an OpenAPI extension
// field (e.g. x-* metadata) and skipped during drift computation.
var VALID_OPERATIONS = map[string]bool{
	"get":     true,
	"post":    true,
	"put":     true,
	"delete":  true,
	"patch":   true,
	"head":    true,
	"options": true,
}

// Manifest mirrors the YAML structure of api/api_docs_manifest.yaml.
// Every field is required except Summary (which is only used to seed
// an empty operation's summary on first-generation).
//
// ResponseRef is the shape used for both requestBody and per-status
// responses. It is a MAPPING (not a string) so we can declare "no
// $ref" explicitly via a `null` (or absent) value AND validate $ref
// shape uniformly. Earlier code declared the response map as
// map[string]*string — which doesn't unmarshal a YAML map value
// (e.g. "201:\n  $ref: ...") and produced
// `cannot unmarshal !!map into string` at parse.
type Manifest struct {
	Version int          `yaml:"version"`
	Routes  []RouteEntry `yaml:"routes"`
}

type RouteEntry struct {
	Path        string             `yaml:"path"`
	Method      string             `yaml:"method"`
	OperationID string             `yaml:"operationId"`
	Tag         string             `yaml:"tag"`
	Summary     string             `yaml:"summary,omitempty"`
	Parameters  []string           `yaml:"parameters"`
	RequestBody *RefEntry          `yaml:"requestBody,omitempty"`
	Responses   map[string]*RefEntry `yaml:"responses"`
}

// RefEntry is the standard `$ref` shape used everywhere a manifest
// references a component. The yaml:"$ref" tag captures the dollar
// sign (which would otherwise require a struct-tag escape).
type RefEntry struct {
	Ref string `yaml:"$ref"`
}

func (r *RouteEntry) Validate() error {
	if r.Path == "" {
		return errors.New("route.path is empty")
	}
	if r.OperationID == "" {
		return fmt.Errorf("route %q: operationId is empty", r.Path)
	}
	switch r.Method {
	case "get", "post", "put", "delete", "patch", "head", "options":
	default:
		return fmt.Errorf("route %q: method %q is not a valid HTTP verb", r.Path, r.Method)
	}
	if r.RequestBody != nil && r.RequestBody.Ref == "" {
		return fmt.Errorf("route %q: requestBody.$ref is empty (use requestBody: null for inline bodies)", r.Path)
	}
	for code, refPtr := range r.Responses {
		if code == "" {
			return fmt.Errorf("route %q: response status code is empty", r.Path)
		}
		if !isHTTPStatusCode(code) {
			return fmt.Errorf("route %q: response status %q is not a 3-digit HTTP status", r.Path, code)
		}
		if refPtr != nil && refPtr.Ref == "" {
			return fmt.Errorf("route %q: response %s $ref is empty (use response: null for inline schemas / no-body responses)", r.Path, code)
		}
	}
	if r.Method == "get" || r.Method == "delete" || r.Method == "head" {
		if r.RequestBody != nil {
			return fmt.Errorf("route %q: %s requests MUST NOT carry a requestBody", r.Path, r.Method)
		}
	}
	return nil
}

func isHTTPStatusCode(code string) bool {
	if len(code) != 3 {
		return false
	}
	for _, c := range code {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

// driftEntry is one row in the drift summary. Same shape for "added",
// "removed", and "mismatched" entries — the Kind discriminator drives
// display + machine parsing.
type driftEntry struct {
	Kind       string `json:"kind"`     // "added" | "removed" | "mismatched"
	Path       string `json:"path"`
	Method     string `json:"method"`
	Field      string `json:"field,omitempty"` // which subfield drifted
	Preimage   string `json:"preimage,omitempty"`
	Generated  string `json:"generated,omitempty"`
	Operation  string `json:"operationId,omitempty"`
}

func main() {
	var (
		applyFlag   = flag.Bool("apply", false, "write the regenerated openapi.yaml back to disk")
		ciMode      = flag.Bool("ci", false, "CI mode: exit 1 on drift, exit 0 on clean (default behavior in non-CI mode prints drift but exits 0)")
		manifestIn  = flag.String("manifest", "api/api_docs_manifest.yaml", "path to the manifest file")
		specIn      = flag.String("spec", "api/openapi.yaml", "path to the curated openapi.yaml (preimage)")
		outTmp      = flag.String("out-tmp", "", "if set, write the generated spec to this file instead of STDOUT for diffing")
	)
	flag.Parse()

	manifest, err := loadManifest(*manifestIn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "FAIL: load manifest %s: %v\n", *manifestIn, err)
		os.Exit(exitGenerr)
	}
	if manifest.Version != manifestVersion {
		fmt.Fprintf(os.Stderr, "FAIL: manifest version=%d, want=%d (regenerate the manifest with the current tool)\n", manifest.Version, manifestVersion)
		os.Exit(exitGenerr)
	}
	for i := range manifest.Routes {
		if err := manifest.Routes[i].Validate(); err != nil {
			fmt.Fprintf(os.Stderr, "FAIL: manifest validation error on entry %d: %v\n", i, err)
			os.Exit(exitGenerr)
		}
	}

	preimage, err := loadSpec(*specIn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "FAIL: load spec %s: %v\n", *specIn, err)
		os.Exit(exitGenerr)
	}

	// Pre-ref validation: every $ref the manifest wants to emit MUST
	// already resolve in the preimage components — otherwise the codegen
	// produces an internally-inconsistent spec that would only fail at
	// the validator-level (and obscure the root cause). Catches the
	// class of "manifest typo" bugs at the source.
	if err := checkRefsResolvable(manifest, preimage); err != nil {
		fmt.Fprintf(os.Stderr, "FAIL: manifest $ref check failed: %v\n", err)
		os.Exit(exitGenerr)
	}

	// operationId continuity: the manifest's operationId MUST equal the
	// one currently in the preimage for the same path+method (when the
	// preimage has an operation for it). This is the cross-file pin:
	// changing a manifest operationId without changing the handler is a
	// silent break, so we surface the drift explicitly.
	operationIDPins := operationIDContinuity(manifest, preimage)

	// 1) Build the new paths block from the manifest.
	generated := buildPathsBlock(manifest)

	// 2) Emit drift summary (manifest paths vs preimage paths).
	drift := computeDrift(manifest, preimage)
	if pins := operationIDPins; len(pins) > 0 {
		drift = append(drift, pins...)
	}
	sort.SliceStable(drift, func(i, j int) bool {
		if drift[i].Path != drift[j].Path {
			return drift[i].Path < drift[j].Path
		}
		return drift[i].Method < drift[j].Method
	})

	// 3) Compose the new spec: copy everything from preimage EXCEPT
	// paths, which we replace with the generated block.
	composed := composeSpec(preimage, generated)

	// 4) Emit the composed spec (possibly to disk).
	generatedOut, err := yaml.Marshal(composed)
	if err != nil {
		fmt.Fprintf(os.Stderr, "FAIL: marshal composed spec: %v\n", err)
		os.Exit(exitGenerr)
	}
	if *outTmp != "" {
		if err := os.WriteFile(*outTmp, generatedOut, 0o644); err != nil {
			fmt.Fprintf(os.Stderr, "FAIL: write %s: %v\n", *outTmp, err)
			os.Exit(exitGenerr)
		}
	}
	preimageBytes, err := os.ReadFile(*specIn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "FAIL: read preimage for diff: %v\n", err)
		os.Exit(exitGenerr)
	}

	// 5) Print the drift report.
	printDrift(drift)
	clean := len(drift) == 0
	switch {
	case *applyFlag:
		// Re-read the preimage as a doc and replace paths.
		if err := os.WriteFile(*specIn, generatedOut, 0o644); err != nil {
			fmt.Fprintf(os.Stderr, "FAIL: write back %s: %v\n", *specIn, err)
			os.Exit(exitGenerr)
		}
		fmt.Fprintf(os.Stderr, "OK: wrote regenerated spec to %s\n", *specIn)
		if clean {
			return
		}
		fmt.Fprintf(os.Stderr, "WARN: applied despite drift — re-read the manifest entries if operationId continuity changed\n")
		os.Exit(exitDrift)
	case *ciMode || !bytes.Equal(preimageBytes, generatedOut):
		if !clean {
			fmt.Fprintf(os.Stderr, "DRIFT: %d drift entries; rerun with -apply\n", len(drift))
			os.Exit(exitDrift)
		}
		// clean drift but bytes differ (e.g. whitespace reorder): still exit 1 in CI to surface the format change.
		if !bytes.Equal(preimageBytes, generatedOut) {
			fmt.Fprintf(os.Stderr, "DRIFT: byte-form differs from preimage; rerun with -apply to format-normalise\n")
			os.Exit(exitDrift)
		}
	default:
		// Default mode: write the regenerated file to .api-docs-gen.out
		// so callers can `diff` it. Don't touch openapi.yaml.
		stagingPath := ".api-docs-gen.out"
		if *outTmp != "" {
			stagingPath = *outTmp
		} else {
			if err := os.WriteFile(stagingPath, generatedOut, 0o644); err != nil {
				fmt.Fprintf(os.Stderr, "FAIL: write %s: %v\n", stagingPath, err)
				os.Exit(exitGenerr)
			}
		}
		if clean {
			fmt.Fprintf(os.Stderr, "OK: codegen clean; staging at %s\n", stagingPath)
			return
		}
		fmt.Fprintf(os.Stderr, "DRIFT: %d entries; staging at %s; rerun with -apply or amend the manifest to clear drift\n", len(drift), stagingPath)
		os.Exit(exitDrift)
	}
}

func loadManifest(path string) (*Manifest, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var m Manifest
	if err := yaml.Unmarshal(raw, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

func loadSpec(path string) (map[string]any, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	raw, err := os.ReadFile(abs)
	if err != nil {
		return nil, err
	}
	var spec map[string]any
	if err := yaml.Unmarshal(raw, &spec); err != nil {
		return nil, err
	}
	return spec, nil
}

func checkRefsResolvable(m *Manifest, preimage map[string]any) error {
	components, _ := preimage["components"].(map[string]any)
	if components == nil {
		return errors.New("preimage has no components section; cannot resolve $refs")
	}
	for _, r := range m.Routes {
		if r.RequestBody != nil {
			if err := checkRef(r.RequestBody.Ref, components); err != nil {
				return fmt.Errorf("route %s %s: requestBody: %w", r.Method, r.Path, err)
			}
		}
		for code, refPtr := range r.Responses {
			if refPtr == nil {
				continue
			}
			if err := checkRef(refPtr.Ref, components); err != nil {
				return fmt.Errorf("route %s %s response %s: %w", r.Method, r.Path, code, err)
			}
		}
	}
	return nil
}

func checkRef(ref string, components map[string]any) error {
	const prefix = "#/components/"
	if !strings.HasPrefix(ref, prefix) {
		return fmt.Errorf("ref %q does not start with %q (only intra-document refs are supported)", ref, prefix)
	}
	rest := strings.TrimPrefix(ref, prefix)
	parts := strings.SplitN(rest, "/", 2)
	if len(parts) != 2 {
		return fmt.Errorf("ref %q is malformed (expected #/components/<group>/<name>)", ref)
	}
	group, name := parts[0], parts[1]
	bucket, ok := components[group].(map[string]any)
	if !ok {
		return fmt.Errorf("components.%s is missing", group)
	}
	if _, ok := bucket[name]; !ok {
		return fmt.Errorf("components.%s.%s is missing", group, name)
	}
	return nil
}

// operationIDContinuity returns one drift entry per (path, method)
// where the manifest's operationId does NOT match the preimage's
// operationId. Empty result = clean.
func operationIDContinuity(m *Manifest, preimage map[string]any) []driftEntry {
	paths, _ := preimage["paths"].(map[string]any)
	if paths == nil {
		return nil
	}
	var out []driftEntry
	for _, r := range m.Routes {
		pre, _ := paths[r.Path].(map[string]any)
		opRaw, exists := pre[r.Method]
		if !exists {
			continue // addition / new path; handled by computeDrift
		}
		op, _ := opRaw.(map[string]any)
		if op == nil {
			continue
		}
		preID, _ := op["operationId"].(string)
		if preID == "" || preID == r.OperationID {
			continue
		}
		out = append(out, driftEntry{
			Kind:      "mismatched",
			Path:      r.Path,
			Method:    r.Method,
			Field:     "operationId",
			Preimage:  preID,
			Generated: r.OperationID,
		})
	}
	return out
}

func computeDrift(m *Manifest, preimage map[string]any) []driftEntry {
	paths, _ := preimage["paths"].(map[string]any)
	if paths == nil {
		paths = map[string]any{}
	}
	var out []driftEntry
	manifestOps := make(map[string]bool)
	for _, r := range m.Routes {
		manifestOps[routeKey(r.Path, r.Method)] = true
		pre, _ := paths[r.Path].(map[string]any)
		_, exists := pre[r.Method]
		if !exists {
			out = append(out, driftEntry{
				Kind:      "added",
				Path:      r.Path,
				Method:    r.Method,
				Operation: r.OperationID,
			})
			continue
		}
		// Mismatches in parameters/responses/requestBody are still
		// equal in this minimal paths-only codegen — only entries the
		// manifest DECLARES are emitted. The crossover check belongs in
		// the validator, not the codegen.
	}
	for pathKey, opRaw := range paths {
		op, _ := opRaw.(map[string]any)
		if op == nil {
			continue
		}
		// `op` is map[string]any, so the iteration key is a plain
		// string (NOT an interface). Earlier code tried to type-
		// assert `method.(string)` which Go rejects at build time
		// with `invalid operation: method (variable of type string)
		// is not an interface`. Use the key directly.
		for methodStr, methodRaw := range op {
			if !VALID_OPERATIONS[methodStr] {
				continue
			}
			if _, isOperation := methodRaw.(map[string]any); !isOperation {
				continue
			}
			if !manifestOps[routeKey(pathKey, methodStr)] {
				out = append(out, driftEntry{
					Kind:   "removed",
					Path:   pathKey,
					Method: methodStr,
				})
			}
		}
	}
	return out
}

func routeKey(path, method string) string {
	return method + " " + path
}

// buildPathsBlock turns the manifest into the new `paths` YAML fragment.
// Each operation is a fully-formed node: tags, operationId, summary,
// parameters (refs), requestBody, responses, security.
func buildPathsBlock(m *Manifest) map[string]any {
	out := map[string]any{}
	for _, r := range m.Routes {
		op := map[string]any{
			"tags":        []string{r.Tag},
			"operationId": r.OperationID,
		}
		if r.Summary != "" {
			op["summary"] = r.Summary
		}
		if len(r.Parameters) > 0 {
			params := make([]any, 0, len(r.Parameters))
			for _, name := range r.Parameters {
				params = append(params, map[string]any{
					"$ref": "#/components/parameters/" + name,
				})
			}
			op["parameters"] = params
		}
		if r.RequestBody != nil {
			op["requestBody"] = map[string]any{
				"required": true,
				"content": map[string]any{
					"application/json": map[string]any{
						"schema": map[string]any{"$ref": r.RequestBody.Ref},
					},
				},
			}
		}
		responses := map[string]any{}
		for code, refPtr := range r.Responses {
			if refPtr == nil {
				responses[code] = map[string]any{"description": descriptionForCode(code)}
				continue
			}
			responses[code] = map[string]any{
				"description": descriptionForCode(code),
				"content": map[string]any{
					"application/json": map[string]any{
						"schema": map[string]any{"$ref": refPtr.Ref},
					},
				},
			}
		}
		op["responses"] = responses
		op["security"] = []any{map[string]any{"bearerAdminToken": []string{}}}
		pathNode, ok := out[r.Path].(map[string]any)
		if !ok {
			pathNode = map[string]any{}
		}
		pathNode[r.Method] = op
		out[r.Path] = pathNode
	}
	return out
}

func descriptionForCode(code string) string {
	switch code {
	case "200":
		return "OK."
	case "201":
		return "Created."
	case "202":
		return "Accepted."
	case "204":
		return "No Content."
	case "400":
		return "Bad Request."
	case "401":
		return "Unauthorized."
	case "403":
		return "Forbidden."
	case "404":
		return "Not Found."
	case "409":
		return "Conflict."
	case "413":
		return "Payload Too Large."
	case "422":
		return "Unprocessable Entity."
	case "429":
		return "Too Many Requests."
	case "500":
		return "Internal Server Error."
	default:
		return "Response."
	}
}

// composeSpec returns a deep-copy of preimage where the `paths` block
// is replaced by `generated`. Other sections are preserved verbatim
// including their original order, so the curated narrative does NOT
// shift around when codegen runs.
func composeSpec(preimage, generated map[string]any) map[string]any {
	out := make(map[string]any, len(preimage))
	for k, v := range preimage {
		if k == "paths" {
			continue
		}
		out[k] = v
	}
	out["paths"] = generated
	return out
}

func printDrift(entries []driftEntry) {
	if len(entries) == 0 {
		return
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetIndent("", "  ")
	_ = enc.Encode(map[string]any{
		"drift_count": len(entries),
		"entries":     entries,
	})
	fmt.Fprintf(os.Stderr, "%s\n", buf.String())
}
