package translation

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// startFakeTranslationServer spins up a provider stub that echoes the scene
// text back prefixed with "TRANSLATED:". sceneText parses the scene out of the
// "user" chat message that Client.Translate builds. failText (when non-empty)
// makes the server return 500 for that scene, exercising the error path.
func startFakeTranslationServer(t *testing.T, failText string) (*httptest.Server, func() (maxInFlight int64)) {
	t.Helper()
	var (
		mu          sync.Mutex
		inFlight    int64
		maxInFlight int64
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req struct {
			Messages []struct {
				Role    string `json:"role"`
				Content string `json:"content"`
			} `json:"messages"`
		}
		_ = json.Unmarshal(body, &req)
		text := ""
		for _, m := range req.Messages {
			if m.Role == "user" {
				parts := strings.SplitN(m.Content, "\n\n", 2)
				text = parts[len(parts)-1]
			}
		}

		mu.Lock()
		inFlight++
		if inFlight > maxInFlight {
			maxInFlight = inFlight
		}
		mu.Unlock()
		time.Sleep(50 * time.Millisecond) // force overlap so concurrency is observable
		mu.Lock()
		inFlight--
		mu.Unlock()

		if text == failText {
			http.Error(w, "boom", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"message":{"role":"assistant","content":"TRANSLATED:%s"}}`, text)
	}))
	return srv, func() int64 {
		mu.Lock()
		defer mu.Unlock()
		return maxInFlight
	}
}

func TestTranslateScenesPreservesOrderAndBoundsConcurrency(t *testing.T) {
	srv, maxInFlight := startFakeTranslationServer(t, "")
	defer srv.Close()

	client := Client{BaseURL: srv.URL, Model: "test", HTTP: srv.Client()}
	scenes := make([]interface{}, 8)
	for i := range scenes {
		scenes[i] = map[string]interface{}{"text": fmt.Sprintf("scene-%d", i)}
	}
	out, err := TranslateScenes(context.Background(), map[string]interface{}{
		"translate_to": "en",
		"scenes":       scenes,
	}, client)
	if err != nil {
		t.Fatalf("TranslateScenes: %v", err)
	}

	outScenes, ok := out["scenes"].([]interface{})
	if !ok || len(outScenes) != 8 {
		t.Fatalf("scenes = %#v, want 8 entries", out["scenes"])
	}
	for i, s := range outScenes {
		m, ok := s.(map[string]interface{})
		if !ok {
			t.Fatalf("scene %d is %T, want map", i, s)
		}
		want := fmt.Sprintf("TRANSLATED:scene-%d", i)
		if m["translated_text"] != want {
			t.Fatalf("scene %d translated_text = %q, want %q", i, m["translated_text"], want)
		}
	}

	if got := maxInFlight(); got > maxTranslationConcurrency {
		t.Fatalf("max in-flight = %d, want <= %d", got, maxTranslationConcurrency)
	}
	if got := maxInFlight(); got < 2 {
		t.Fatalf("max in-flight = %d, want >= 2 (scenes should translate concurrently)", got)
	}
}

func TestTranslateScenesReturnsLowestIndexFailure(t *testing.T) {
	srv, _ := startFakeTranslationServer(t, "scene-1")
	defer srv.Close()

	client := Client{BaseURL: srv.URL, Model: "test", HTTP: srv.Client()}
	scenes := make([]interface{}, 3)
	for i := range scenes {
		scenes[i] = map[string]interface{}{"text": fmt.Sprintf("scene-%d", i)}
	}
	_, err := TranslateScenes(context.Background(), map[string]interface{}{
		"translate_to": "en",
		"scenes":       scenes,
	}, client)
	if err == nil {
		t.Fatal("TranslateScenes returned nil error, want scene 1 failure")
	}
	if !strings.Contains(err.Error(), "translate scene 1") {
		t.Fatalf("error = %v, want it to reference scene 1", err)
	}
}
