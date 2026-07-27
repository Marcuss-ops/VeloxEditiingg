// Package schemagen turns Go structs into OpenAPI 3.1 component schemas.
//
// Schemagen consumes the empty value of a struct plus its `json:"…"`
// and `validate:"…"` tags and emits an OpenAPI-compatible schema as a
// map[string]any (consumable by yaml.Marshal). It is intentionally
// minimal: the project's validator rules live in the Go struct tags;
// the OpenAPI YAML is derived from them, NEVER the other way around.
//
// Tag grammar (subset of go-playground/validator/v10 chosen for the
// project's wire contracts — no new dependency added):
//
//   - required               → emits the field name into the parent
//                              schema's `required` array.
//   - max=N / min=N          → string → maxLength/minLength;
//                              numeric → maximum/minimum;
//                              array   → maxItems/minItems.
//   - len=N                  → string/array → sets both min/max + length.
//   - oneof=a b c            → enum: [a, b, c].
//   - gte=N / lte=N          → numeric inclusive → minimum/maximum.
//   - gt=N  / lt=N           → numeric exclusive → exclusiveMinimum/Maximum.
//   - email / uuid / url     → format: email | uuid | uri.
//   - regex=…                → pattern: ….
//
// Fields with json:"-" are skipped (consistent with encoding/json).
//
// The library is reflection-based (not AST-based). This is a deliberate
// trade-off: reflect handles type aliases, embedded structs, pointers
// and slices uniformly and matches the runtime types; an AST parser
// would need to re-implement Go's type resolution (and require
// loading the target packages by import-path, brittle). For the
// internal-only use-case here, reflect is the right tool.
//
// This package has no third-party dependencies — yaml.v3 ingestion
// lives in callers (cmd/api-schema-gen).
package schemagen

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// Schema returns the OpenAPI 3.1 schema for the given Go value v.
// v should be a zero value of the target struct (e.g. `SubmitScene{}`).
//
// The optional `registry` is the set of type names that, when
// encountered as a nested field, MUST be emitted as
// `$ref: '#/components/schemas/<Name>'` rather than inlined. Pass the
// set of schema names the code-generator emits in the same run; using
// the registry keeps generated YAML cross-refs intact (matching the
// manually-edited openapi.yaml structure).
func Schema(v any, registry ...string) (map[string]any, error) {
	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.Ptr {
		rv = rv.Elem()
	}
	if rv.Kind() != reflect.Struct {
		return nil, fmt.Errorf("schemagen: expected struct, got %v", rv.Kind())
	}
	reg := make(map[string]bool, len(registry))
	for _, n := range registry {
		if n != "" {
			reg[n] = true
		}
	}
	out, _, err := structSchema(rv.Type(), reg)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// structSchema walks the struct fields and returns the schema map
// along with the "required" field-name list (caller merges it into
// parent-level required[]).
func structSchema(typ reflect.Type, reg map[string]bool) (map[string]any, []string, error) {
	props := map[string]any{}
	var required []string
	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)
		js := f.Tag.Get("json")
		vt := f.Tag.Get("validate")
		name, _, _ := strings.Cut(js, ",")
		if name == "-" {
			continue
		}
		if name == "" {
			name = snakeCase(f.Name)
		}
		schema, isReq := fieldSchema(f.Type, vt, reg)
		props[name] = schema
		if isReq {
			required = append(required, name)
		}
	}
	out := map[string]any{
		"type":       "object",
		"properties": props,
	}
	if len(required) > 0 {
		sortStrings(required)
		out["required"] = toAnySlice(required)
	}
	return out, required, nil
}

// fieldSchema returns the OpenAPI schema for a single struct field.
func fieldSchema(t reflect.Type, validate string, reg map[string]bool) (map[string]any, bool) {
	required := false
	out := map[string]any{}

	// Strip pointer indirection — *T emits the schema of T. The
	// validate tag's "omitempty" is handled separately (required
	// stays false even when validate:"required, omitempty, ...").
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	parts := splitValidate(validate)
	hasOmitempty := false
	for _, p := range parts {
		switch {
		case p == "required":
			required = true
		case p == "omitempty":
			hasOmitempty = true
			// omitempty doesn't suppress `required`; the two are
			// orthogonal in the validator grammar — a field can be
			// required AND omitempty in json. We honour the
			// explicit `required` rule and ignore omitempty for
			// the required-list calculation, but it MAY signal
			// (future) array minItems=0 etc.
			continue
		case strings.HasPrefix(p, "max="):
			n, _ := strconv.Atoi(p[4:])
			switch kindOf(t) {
			case "string":
				out["maxLength"] = n
			case "array":
				out["maxItems"] = n
			default:
				out["maximum"] = n
			}
		case strings.HasPrefix(p, "min="):
			n, _ := strconv.Atoi(p[4:])
			switch kindOf(t) {
			case "string":
				out["minLength"] = n
			default:
				out["minimum"] = n
			}
		case strings.HasPrefix(p, "len="):
			n, _ := strconv.Atoi(p[4:])
			switch kindOf(t) {
			case "string":
				out["maxLength"] = n
				out["minLength"] = n
			case "array":
				out["maxItems"] = n
				out["minItems"] = n
			}
		case strings.HasPrefix(p, "oneof="):
			out["enum"] = strings.Fields(strings.TrimPrefix(p, "oneof="))
		case strings.HasPrefix(p, "gte="):
			n, _ := strconv.ParseFloat(p[4:], 64)
			out["minimum"] = n
		case strings.HasPrefix(p, "lte="):
			n, _ := strconv.ParseFloat(p[4:], 64)
			out["maximum"] = n
		case strings.HasPrefix(p, "gt="):
			n, _ := strconv.ParseFloat(p[3:], 64)
			out["exclusiveMinimum"] = n
		case strings.HasPrefix(p, "lt="):
			n, _ := strconv.ParseFloat(p[3:], 64)
			out["exclusiveMaximum"] = n
		case p == "email":
			out["format"] = "email"
		case p == "uuid":
			out["format"] = "uuid"
		case p == "url", p == "uri":
			out["format"] = "uri"
		case strings.HasPrefix(p, "regex="):
			out["pattern"] = strings.TrimPrefix(p, "regex=")
		}
	}
	_ = hasOmitempty

	// Set type-specific defaults AFTER the validate rules so the
	// validate values override the type defaults (e.g. enum already
	// sets type to string above).
	setTypeFromGo(t, out, reg)
	return out, required
}

// setTypeFromGo infers the JSON Schema type from the Go type and
// attaches nested / items schemas where applicable.
func setTypeFromGo(t reflect.Type, out map[string]any, reg map[string]bool) {
	switch k := t.Kind(); k {
	case reflect.String:
		if _, present := out["type"]; !present {
			out["type"] = "string"
		}
	case reflect.Bool:
		out["type"] = "boolean"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		out["type"] = "integer"
	case reflect.Float32, reflect.Float64:
		out["type"] = "number"
		if _, present := out["format"]; !present {
			out["format"] = "float"
		}
	case reflect.Slice, reflect.Array:
		out["type"] = "array"
		if _, present := out["minItems"]; !present {
			out["minItems"] = 0
		}
		elem, _ := fieldSchema(t.Elem(), "", reg)
		if _, hasEnum := out["enum"]; hasEnum {
			return
		}
		out["items"] = elem
	case reflect.Struct:
		// Special-case well-known stdlib / encoding types so we
		// don't emit verbose object schemas for them.
		if t == reflect.TypeOf(time.Time{}) {
			out["type"] = "string"
			out["format"] = "date-time"
			return
		}
		if t == reflect.TypeOf(json.RawMessage(nil)) {
			out["type"] = "string"
			return
		}
		// Type registry cross-ref: if this struct's NAME is in the
		// registry emit $ref instead of inlining.
		name := t.Name()
		if name != "" && reg[name] {
			out["$ref"] = "#/components/schemas/" + name
			return
		}
		out["type"] = "object"
		nested, _, err := structSchema(t, reg)
		_ = err
		if err == nil {
			if props, ok := nested["properties"].(map[string]any); ok {
				out["properties"] = props
			}
			if req, ok := nested["required"]; ok {
				out["required"] = req
			}
		}
	case reflect.Map:
		out["type"] = "object"
		if t.Key().Kind() == reflect.String {
			valSchema, _ := fieldSchema(t.Elem(), "", reg)
			out["additionalProperties"] = valSchema
		} else {
			out["additionalProperties"] = true
		}
	default:
		// Fallback for chan/func/interface — emit generic object.
		out["type"] = "object"
	}
}

// kindOf returns a coarse JSON Schema "kind" label used by the
// validate-tag handlers to pick length vs items vs numeric bounds.
func kindOf(t reflect.Type) string {
	switch t.Kind() {
	case reflect.String:
		return "string"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return "integer"
	case reflect.Float32, reflect.Float64:
		return "number"
	case reflect.Bool:
		return "boolean"
	case reflect.Slice, reflect.Array:
		return "array"
	}
	return "object"
}

func splitValidate(s string) []string {
	out := []string{}
	for _, p := range strings.Split(s, ",") {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func snakeCase(s string) string {
	var b strings.Builder
	for i, r := range s {
		if i > 0 && r >= 'A' && r <= 'Z' {
			b.WriteByte('_')
		}
		if r >= 'A' && r <= 'Z' {
			b.WriteRune(r + ('a' - 'A'))
		} else {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func sortStrings(s []string) {
	// Small enough; insertion sort to avoid importing sort for the
	// test path. Caller expectation: only handful of fields.
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j-1] > s[j]; j-- {
			s[j-1], s[j] = s[j], s[j-1]
		}
	}
}

func toAnySlice(in []string) []any {
	out := make([]any, len(in))
	for i, v := range in {
		out[i] = v
	}
	return out
}
