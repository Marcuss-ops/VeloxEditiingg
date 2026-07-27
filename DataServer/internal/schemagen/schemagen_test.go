package schemagen

import (
	"reflect"
	"testing"
)

// Each test below pins a specific shape of Go struct → OpenAPI schema.
// Together they cover the four validation concerns called out in the
// project's MVP: primitives+required, string boundaries, numeric
// bounds, and enum/slices. The fifth case verifies nested-struct
// $ref cross-references via the registry path.

func TestSchema_PrimitivesRequired(t *testing.T) {
	type req struct {
		Name  string `json:"name" validate:"required,min=1,max=64"`
		Count int    `json:"count" validate:"required"`
	}
	got, err := Schema(req{})
	if err != nil {
		t.Fatal(err)
	}
	if got["type"] != "object" {
		t.Errorf("type: %v", got["type"])
	}
	reqSlice, ok := got["required"].([]any)
	if !ok {
		t.Fatalf("required missing or wrong type: %v", got["required"])
	}
	if len(reqSlice) != 2 {
		t.Errorf("required list len: %d", len(reqSlice))
	}
	props, ok := got["properties"].(map[string]any)
	if !ok {
		t.Fatal("properties not a map")
	}
	name, ok := props["name"].(map[string]any)
	if !ok {
		t.Fatal("name missing")
	}
	if name["type"] != "string" {
		t.Errorf("name.type = %v", name["type"])
	}
	if name["minLength"] != 1 || name["maxLength"] != 64 {
		t.Errorf("name bounds: %v", name)
	}
	count, ok := props["count"].(map[string]any)
	if !ok {
		t.Fatal("count missing")
	}
	if count["type"] != "integer" {
		t.Errorf("count.type = %v", count["type"])
	}
}

func TestSchema_StringBoundaries(t *testing.T) {
	type req struct {
		Email string `json:"email" validate:"required,email"`
		ID    string `json:"id" validate:"required,uuid"`
		Link  string `json:"link" validate:"omitempty,url"`
		Code  string `json:"code" validate:"required,regex=^[A-Z]{3}$"`
	}
	got, err := Schema(req{})
	if err != nil {
		t.Fatal(err)
	}
	props, ok := got["properties"].(map[string]any)
	if !ok {
		t.Fatal("properties not a map")
	}
	if props["email"].(map[string]any)["format"] != "email" {
		t.Errorf("email.format: %v", props["email"])
	}
	if props["id"].(map[string]any)["format"] != "uuid" {
		t.Errorf("id.format: %v", props["id"])
	}
	if props["link"].(map[string]any)["format"] != "uri" {
		t.Errorf("link.format: %v", props["link"])
	}
	if props["code"].(map[string]any)["pattern"] != "^[A-Z]{3}$" {
		t.Errorf("code.pattern: %v", props["code"])
	}
	// All four are required EXCEPT Link (validate omitempty).
	reqList, _ := got["required"].([]any)
	foundLink := false
	for _, r := range reqList {
		if r == "link" {
			foundLink = true
		}
	}
	if foundLink {
		t.Errorf("link must NOT be required when validate contains omitempty")
	}
}

func TestSchema_NumericBounds(t *testing.T) {
	type req struct {
		Score  float64 `json:"score" validate:"required,gte=0.1,lte=86400"`
		Strict int     `json:"strict" validate:"required,gt=0,lt=100"`
	}
	got, err := Schema(req{})
	if err != nil {
		t.Fatal(err)
	}
	props, ok := got["properties"].(map[string]any)
	if !ok {
		t.Fatal("properties not a map")
	}
	score := props["score"].(map[string]any)
	if score["type"] != "number" {
		t.Errorf("score.type: %v", score["type"])
	}
	if v, _ := score["minimum"].(float64); v != 0.1 {
		t.Errorf("score.minimum: %v (type %T)", score["minimum"], score["minimum"])
	}
	if v, _ := score["maximum"].(float64); v != 86400.0 {
		t.Errorf("score.maximum: %v (type %T)", score["maximum"], score["maximum"])
	}
	strict := props["strict"].(map[string]any)
	if strict["type"] != "integer" {
		t.Errorf("strict.type: %v", strict["type"])
	}
	if v, _ := strict["exclusiveMinimum"].(float64); v != 0 {
		t.Errorf("strict.exclusiveMinimum: %v (type %T)", strict["exclusiveMinimum"], strict["exclusiveMinimum"])
	}
	if v, _ := strict["exclusiveMaximum"].(float64); v != 100 {
		t.Errorf("strict.exclusiveMaximum: %v (type %T)", strict["exclusiveMaximum"], strict["exclusiveMaximum"])
	}
}

func TestSchema_EnumAndSlices(t *testing.T) {
	type scene struct {
		Kind string   `json:"kind" validate:"required,oneof=public unlisted private"`
		Tags []string `json:"tags,omitempty" validate:"max=10"`
	}
	got, err := Schema(scene{})
	if err != nil {
		t.Fatal(err)
	}
	props, ok := got["properties"].(map[string]any)
	if !ok {
		t.Fatal("properties not a map")
	}
	kind, ok := props["kind"].(map[string]any)
	if !ok {
		t.Fatal("kind missing")
	}
	enumVals, ok := kind["enum"].([]string)
	if !ok {
		t.Fatalf("kind.enum not a []string: %v", kind["enum"])
	}
	want := []string{"public", "unlisted", "private"}
	if !reflect.DeepEqual(enumVals, want) {
		t.Errorf("kind.enum = %v, want %v", enumVals, want)
	}
	tags, ok := props["tags"].(map[string]any)
	if !ok {
		t.Fatal("tags missing")
	}
	if tags["type"] != "array" {
		t.Errorf("tags.type: %v", tags["type"])
	}
	if tags["maxItems"] != 10 {
		t.Errorf("tags.maxItems: %v", tags["maxItems"])
	}
	items := tags["items"].(map[string]any)
	if items["type"] != "string" {
		t.Errorf("tags.items.type: %v", items["type"])
	}
}

func TestSchema_NestedStructRef(t *testing.T) {
	type Link struct {
		URL string `json:"url" validate:"required,url"`
	}
	type Pool struct {
		Name  string `json:"name" validate:"required"`
		Links []Link `json:"links" validate:"required"`
	}
	got, err := Schema(Pool{}, "Link")
	if err != nil {
		t.Fatal(err)
	}
	props := got["properties"].(map[string]any)
	links := props["links"].(map[string]any)
	items := links["items"].(map[string]any)
	if items["$ref"] != "#/components/schemas/Link" {
		t.Errorf("links.items should be $ref to Link, got %v", items)
	}
	// The cross-ref means the inline "properties" must NOT be set.
	if _, present := items["properties"]; present {
		t.Errorf("$ref schema must not also have inline properties: %v", items)
	}
}
