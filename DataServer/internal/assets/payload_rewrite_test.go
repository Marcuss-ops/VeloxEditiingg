package assets

import (
	"context"
	"strings"
	"testing"

	"velox-shared/assetref"
)

func TestApplyRewriteUsesTypedParserForCanonicalLocalReference(t *testing.T) {
	var rewritten []string
	input := strings.ToUpper(assetref.SchemeVeloxAsset) + "://asset-1"

	err := (&AssetService{}).applyRewrite(
		context.Background(),
		map[string]interface{}{},
		RoleVoiceover,
		func(map[string]interface{}) []string { return []string{input} },
		func(_ map[string]interface{}, refs []string) { rewritten = append(rewritten, refs...) },
	)
	if err != nil {
		t.Fatalf("applyRewrite: %v", err)
	}
	if got, want := strings.Join(rewritten, ","), assetref.SchemeVeloxAsset+"://asset-1"; got != want {
		t.Fatalf("rewritten = %q, want %q", got, want)
	}
}

func TestApplyRewriteRejectsEmptyCanonicalLocalReference(t *testing.T) {
	called := false
	err := (&AssetService{}).applyRewrite(
		context.Background(),
		map[string]interface{}{},
		RoleVoiceover,
		func(map[string]interface{}) []string { return []string{assetref.SchemeVeloxAsset + "://"} },
		func(_ map[string]interface{}, _ []string) { called = true },
	)
	if err == nil {
		t.Fatal("applyRewrite accepted an empty canonical local reference")
	}
	if called {
		t.Fatal("applyRewrite applied an empty canonical local reference")
	}
}
