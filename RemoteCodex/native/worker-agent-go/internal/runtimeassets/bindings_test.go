package runtimeassets

import (
	"context"
	"testing"
)

func TestBindingsContext_DefensiveCopies(t *testing.T) {
	original := Bindings{
		"video-1": {AssetID: "video-1", Path: "/var/cache/video-1.mp4", SHA256: "abc", Size: 12},
	}
	ctx := WithBindings(context.Background(), original)
	original["video-1"] = Binding{AssetID: "video-1", Path: "/tampered"}
	original["audio-1"] = Binding{AssetID: "audio-1", Path: "/unexpected"}

	got, ok := FromContext(ctx)
	if !ok {
		t.Fatal("FromContext returned !ok")
	}
	if got["video-1"].Path != "/var/cache/video-1.mp4" {
		t.Fatalf("stored binding mutated through input map: %+v", got["video-1"])
	}
	if _, exists := got["audio-1"]; exists {
		t.Fatal("stored bindings unexpectedly contain later input mutation")
	}

	got["video-1"] = Binding{AssetID: "video-1", Path: "/mutated-read"}
	got["audio-2"] = Binding{AssetID: "audio-2", Path: "/mutated-read"}
	again, ok := FromContext(ctx)
	if !ok {
		t.Fatal("second FromContext returned !ok")
	}
	if again["video-1"].Path != "/var/cache/video-1.mp4" {
		t.Fatalf("stored binding mutated through output map: %+v", again["video-1"])
	}
	if _, exists := again["audio-2"]; exists {
		t.Fatal("stored bindings unexpectedly contain output-map mutation")
	}
}

func TestBindingsContext_NilAndEmptyAreFailClosed(t *testing.T) {
	if _, ok := FromContext(nil); ok {
		t.Fatal("FromContext(nil) returned ok")
	}
	ctx := WithBindings(context.Background(), nil)
	if _, ok := FromContext(ctx); ok {
		t.Fatal("WithBindings(nil) returned a usable binding set")
	}
}
