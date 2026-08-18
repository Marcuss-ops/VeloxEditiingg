// Command contractgen regenerates the Go and C++ field-name constants from
// the canonical wire schemas in shared/contract/schema/. The schemas are the
// single source of truth for field names; the generated bindings exist so
// production code never hardcodes wire keys like "job_id" or
// "render_manifest" as bare string literals.
//
// Generated outputs:
//
//	shared/contract/payloadfield/payloadfield_gen.go
//	RemoteCodex/native/video-engine-cpp/include/velox/contract/payload_fields_generated.hpp
//
// Freshness is verified by scripts/ci/check-contract-schema.sh, which runs
// the generator with -check (fail if the checked-in bindings are stale) and
// compiles a tiny C++ consumer of the generated header.
//
// Parity pins (fail at generation time, not review time):
//
//   - job_payload_v2.schema.json top-level keys == contract.CanonicalTopLevelKeys
//   - render_manifest_v1.schema.json properties.schema.const == rendermanifest.Schema
//
// Run:
//
//	go run ./contract/cmd/contractgen \
//	    -schema-dir contract/schema \
//	    -go-output contract/payloadfield/payloadfield_gen.go \
//	    -cpp-output ../RemoteCodex/native/video-engine-cpp/include/velox/contract/payload_fields_generated.hpp
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"go/format"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"velox-shared/contract"
	"velox-shared/contract/rendermanifest"
)

// schemaSpec maps one authoritative schema file to the identifier prefixes
// used for its generated constants. job_payload_v2 uses no prefix because its
// field names already produce unambiguous Go identifiers (job_id → JobID,
// render_manifest → RenderManifest); the other two are prefixed so their
// nested field names cannot collide with the job payload's top-level keys
// (e.g. output → RenderManifestOutput, destination_id → DeliveryPlanDestinationID).
type schemaSpec struct {
	file      string // filename under -schema-dir
	goPrefix  string // "" for the unprefixed job payload
	cppPrefix string // "" for the unprefixed job payload
}

var schemaSpecs = []schemaSpec{
	{file: "job_payload_v2.schema.json"},
	{file: "render_manifest_v1.schema.json", goPrefix: "RenderManifest", cppPrefix: "RENDER_MANIFEST"},
	{file: "delivery_plan_v1.schema.json", goPrefix: "DeliveryPlan", cppPrefix: "DELIVERY_PLAN"},
}

// field is one generated constant: the JSON field name (value) plus the Go
// and C++ identifiers derived from its schema path.
type field struct {
	path    []string // JSON field-name path segments, e.g. ["tracks","events","asset_id"]
	goName  string   // Go const identifier
	cppName string   // C++ constexpr identifier
	value   string   // the leaf JSON field name, e.g. "asset_id"
}

func main() {
	schemaDir := flag.String("schema-dir", "schema", "directory holding the three canonical *.schema.json files")
	goOutput := flag.String("go-output", "", "generated Go binding path")
	cppOutput := flag.String("cpp-output", "", "generated C++ header path")
	check := flag.Bool("check", false, "check that outputs are up to date without writing them")
	flag.Parse()

	if *goOutput == "" && *cppOutput == "" {
		fmt.Fprintln(os.Stderr, "contractgen: at least one of -go-output or -cpp-output is required")
		os.Exit(2)
	}

	schemas, err := loadSchemas(*schemaDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "contractgen: %v\n", err)
		os.Exit(1)
	}
	if err := checkParity(schemas); err != nil {
		fmt.Fprintf(os.Stderr, "contractgen: %v\n", err)
		os.Exit(1)
	}

	fields := make(map[string][]field, len(schemaSpecs))
	for _, spec := range schemaSpecs {
		fields[spec.file] = collectFields(schemas[spec.file], spec)
	}

	outputs := []struct {
		path string
		data []byte
	}{
		{path: *goOutput, data: []byte(renderGo(fields))},
		{path: *cppOutput, data: []byte(renderHeader(fields))},
	}
	for _, generated := range outputs {
		if generated.path == "" {
			continue
		}
		if *check {
			existing, err := os.ReadFile(generated.path)
			if err != nil {
				fmt.Fprintf(os.Stderr, "contractgen: read %s: %v\n", generated.path, err)
				os.Exit(1)
			}
			if string(existing) != string(generated.data) {
				fmt.Fprintf(os.Stderr, "contractgen: %s is stale; run contractgen without -check and commit the result\n", generated.path)
				os.Exit(1)
			}
			continue
		}
		if err := os.MkdirAll(filepath.Dir(generated.path), 0o755); err != nil {
			fmt.Fprintf(os.Stderr, "contractgen: create output directory: %v\n", err)
			os.Exit(1)
		}
		if err := os.WriteFile(generated.path, generated.data, 0o644); err != nil {
			fmt.Fprintf(os.Stderr, "contractgen: write %s: %v\n", generated.path, err)
			os.Exit(1)
		}
	}
}

// loadSchemas reads the three canonical schema files into generic maps.
func loadSchemas(dir string) (map[string]map[string]interface{}, error) {
	out := make(map[string]map[string]interface{}, len(schemaSpecs))
	for _, spec := range schemaSpecs {
		path := filepath.Join(dir, spec.file)
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read schema %s: %w", path, err)
		}
		var doc map[string]interface{}
		if err := json.Unmarshal(data, &doc); err != nil {
			return nil, fmt.Errorf("parse schema %s: %w", path, err)
		}
		out[spec.file] = doc
	}
	return out, nil
}

// checkParity enforces the two architectural pins that keep the schemas from
// drifting away from the typed Go contracts.
func checkParity(schemas map[string]map[string]interface{}) error {
	// Pin 1: job_payload_v2 top-level keys == contract.CanonicalTopLevelKeys.
	jobProps := topLevelProperties(schemas["job_payload_v2.schema.json"])
	if !sameStringSet(jobProps, contract.CanonicalTopLevelKeys) {
		onlySchema := setDifference(jobProps, contract.CanonicalTopLevelKeys)
		onlyGo := setDifference(contract.CanonicalTopLevelKeys, jobProps)
		return fmt.Errorf("job_payload_v2.schema.json top-level keys drifted from contract.CanonicalTopLevelKeys: only-in-schema=%v only-in-go=%v", onlySchema, onlyGo)
	}

	// Pin 2: render_manifest_v1 properties.schema.const == rendermanifest.Schema.
	constValue, ok := nestedConst(schemas["render_manifest_v1.schema.json"], []string{"schema"})
	if !ok {
		return fmt.Errorf("render_manifest_v1.schema.json is missing properties.schema.const (expected %q)", rendermanifest.Schema)
	}
	if constValue != rendermanifest.Schema {
		return fmt.Errorf("render_manifest_v1.schema.json properties.schema.const = %q, want %q", constValue, rendermanifest.Schema)
	}
	return nil
}

// topLevelProperties returns the keys of the schema root's `properties` map.
func topLevelProperties(doc map[string]interface{}) []string {
	props, ok := doc["properties"].(map[string]interface{})
	if !ok {
		return nil
	}
	keys := make([]string, 0, len(props))
	for k := range props {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// nestedConst resolves properties.<seg[0]>.const (single-level, used for the
// render_manifest `schema` field pin).
func nestedConst(doc map[string]interface{}, path []string) (string, bool) {
	props, ok := doc["properties"].(map[string]interface{})
	if !ok || len(path) != 1 {
		return "", false
	}
	sub, ok := props[path[0]].(map[string]interface{})
	if !ok {
		return "", false
	}
	value, ok := sub["const"].(string)
	return value, ok
}

func sameStringSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	set := make(map[string]bool, len(a))
	for _, s := range a {
		set[s] = true
	}
	for _, s := range b {
		if !set[s] {
			return false
		}
	}
	return true
}

func setDifference(a, b []string) []string {
	other := make(map[string]bool, len(b))
	for _, s := range b {
		other[s] = true
	}
	var out []string
	for _, s := range a {
		if !other[s] {
			out = append(out, s)
		}
	}
	sort.Strings(out)
	return out
}

// collectFields walks a schema document and emits one constant per JSON field
// name, flattened so nested arrays do not add a path segment (items is not a
// field name). Results are sorted by path for deterministic output.
func collectFields(doc map[string]interface{}, spec schemaSpec) []field {
	var fields []field
	seen := make(map[string]bool)
	walk(doc, nil, spec, &fields, seen)
	sort.Slice(fields, func(i, j int) bool {
		return strings.Join(fields[i].path, ".") < strings.Join(fields[j].path, ".")
	})
	return fields
}

func walk(node interface{}, path []string, spec schemaSpec, fields *[]field, seen map[string]bool) {
	switch n := node.(type) {
	case map[string]interface{}:
		if props, ok := n["properties"].(map[string]interface{}); ok {
			for name, sub := range props {
				register(name, sub, path, spec, fields, seen)
			}
		}
		if items, ok := n["items"].(map[string]interface{}); ok {
			walk(items, path, spec, fields, seen)
		}
		for _, key := range []string{"oneOf", "anyOf", "allOf"} {
			if list, ok := n[key].([]interface{}); ok {
				for _, sub := range list {
					walk(sub, path, spec, fields, seen)
				}
			}
		}
		if ap, ok := n["additionalProperties"].(map[string]interface{}); ok {
			walk(ap, path, spec, fields, seen)
		}
	case []interface{}:
		for _, sub := range n {
			walk(sub, path, spec, fields, seen)
		}
	}
}

func register(name string, sub interface{}, path []string, spec schemaSpec, fields *[]field, seen map[string]bool) {
	newPath := append(append([]string{}, path...), name)
	key := strings.Join(newPath, ".")
	if !seen[key] {
		seen[key] = true
		*fields = append(*fields, field{
			path:    newPath,
			goName:  goIdent(spec.goPrefix, newPath),
			cppName: cppIdent(spec.cppPrefix, newPath),
			value:   name,
		})
	}
	walk(sub, newPath, spec, fields, seen)
}

// renderGo renders the generated Go package (string constants only — no
// imports, so the generated file is dependency-free).
func renderGo(fields map[string][]field) string {
	var b strings.Builder
	b.WriteString("// Code generated by shared/contract/cmd/contractgen; DO NOT EDIT.\n")
	b.WriteString("// Source: shared/contract/schema/*.schema.json\n")
	b.WriteString("package payloadfield\n\n")
	for _, spec := range schemaSpecs {
		b.WriteString(fmt.Sprintf("// %s\n", spec.file))
		for _, f := range fields[spec.file] {
			fmt.Fprintf(&b, "const %s = %s\n", f.goName, strconv.Quote(f.value))
		}
		if spec.file == "job_payload_v2.schema.json" {
			b.WriteString("\n// JobPayloadV2Keys returns the canonical top-level key set in\n")
			b.WriteString("// deterministic (sorted) order. It is a fresh slice on every call.\n")
			b.WriteString("func JobPayloadV2Keys() []string {\n")
			b.WriteString("    return []string{\n")
			for _, f := range fields[spec.file] {
				fmt.Fprintf(&b, "        %s,\n", f.goName)
			}
			b.WriteString("    }\n")
			b.WriteString("}\n")
		}
		b.WriteString("\n")
	}
	formatted, err := format.Source([]byte(b.String()))
	if err != nil {
		panic(fmt.Sprintf("contractgen: format generated Go binding: %v", err))
	}
	return string(formatted)
}

// renderHeader renders the generated C++ header (string_view constants only).
func renderHeader(fields map[string][]field) string {
	var b strings.Builder
	b.WriteString("// Code generated by shared/contract/cmd/contractgen; DO NOT EDIT.\n")
	b.WriteString("// Source: shared/contract/schema/*.schema.json\n")
	b.WriteString("#pragma once\n\n")
	b.WriteString("#include <string_view>\n\n")
	b.WriteString("namespace velox::contract::fields {\n\n")
	for _, spec := range schemaSpecs {
		b.WriteString(fmt.Sprintf("// %s\n", spec.file))
		for _, f := range fields[spec.file] {
			fmt.Fprintf(&b, "inline constexpr std::string_view %s = %s;\n", f.cppName, cppString(f.value))
		}
		b.WriteString("\n")
	}
	b.WriteString("}  // namespace velox::contract::fields\n")
	return b.String()
}

// acronyms maps whole underscore-separated tokens to their canonical Go
// identifier capitalization, matching the field naming used by the typed
// contracts (JobPayloadV2, rendermanifest.Manifest).
var acronyms = map[string]string{
	"id":     "ID",
	"ids":    "IDs",
	"uri":    "URI",
	"url":    "URL",
	"sha":    "SHA",
	"sha256": "SHA256",
	"fps":    "FPS",
	"ms":     "MS",
	"db":     "DB",
	"json":   "JSON",
	"srt":    "SRT",
	"gif":    "GIF",
}

func goIdent(prefix string, path []string) string {
	parts := make([]string, 0, len(path)+1)
	if prefix != "" {
		parts = append(parts, prefix)
	}
	for _, seg := range path {
		for _, token := range strings.Split(seg, "_") {
			parts = append(parts, goWord(token))
		}
	}
	return strings.Join(parts, "")
}

func goWord(token string) string {
	if token == "" {
		return ""
	}
	if up, ok := acronyms[strings.ToLower(token)]; ok {
		return up
	}
	r := []rune(token)
	return strings.ToUpper(string(r[:1])) + strings.ToLower(string(r[1:]))
}

func cppIdent(prefix string, path []string) string {
	parts := make([]string, 0, len(path)+1)
	if prefix != "" {
		parts = append(parts, prefix)
	}
	for _, seg := range path {
		parts = append(parts, strings.ToUpper(seg))
	}
	return strings.Join(parts, "_")
}

func cppString(value string) string {
	return `"` + strings.ReplaceAll(strings.ReplaceAll(value, `\`, `\\`), `"`, `\"`) + `"`
}
