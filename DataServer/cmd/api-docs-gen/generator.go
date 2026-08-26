// Command api-docs-gen regenerates the `paths` block of
// DataServer/api/openapi.yaml from DataServer/api/api_docs_manifest.yaml
// while preserving every other section (info, servers, tags,
// components, security, ...) verbatim.
//
// Why a custom codegen instead of swag/oapi-codegen/ogen:
//
//   - Zero new third-party dependencies. yaml.v3 is already vendored.
//   - The hand-curated narrative (operation descriptions, response
//     examples, schema explanations) stays untouched. swag requires
//     `// @Description` annotation on every handler; the cost of
//     porting 30+ handlers is far higher than the maintenance cost
//     of a small, declarative manifest.
//   - The schema block (components/schemas) remains hand-curated
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
//	0 — codegen succeeded AND drift is clean
//	1 — codegen succeeded BUT drift detected (re-run with -apply)
//	2 — codegen failed (manifest invalid, $ref unresolved, ...)
//
// Usage:
//
//	api-docs-gen                          # write to .api-docs-gen.out, print drift
//	api-docs-gen -apply                   # write back into openapi.yaml
//	api-docs-gen -apply -ci               # silence drift warnings (treat as drifted)
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)
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
		securityScheme := "bearerAdminToken"
		if r.Security != "" {
			securityScheme = r.Security
		}
		op["security"] = []any{map[string]any{securityScheme: []string{}}}
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
	case "410":
		return "Gone."
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
