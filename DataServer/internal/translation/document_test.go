package translation

import "testing"

func TestRenderGoogleDocContentKeepsSceneBoundaries(t *testing.T) {
	got, err := RenderGoogleDocContent(map[string]interface{}{
		"video_name": "Jackie Chan",
		"scenes": []interface{}{
			map[string]interface{}{"text": "Martial arts and comedy.", "translated_text": "Arti marziali e comicità."},
			map[string]interface{}{"text": "Rhythm drives the performance.", "translated_text": "Il ritmo guida la performance."},
		},
	})
	if err != nil {
		t.Fatalf("RenderGoogleDocContent: %v", err)
	}
	want := "Jackie Chan\n\nScene 1\nOriginal: Martial arts and comedy.\nTranslation: Arti marziali e comicità.\n\nScene 2\nOriginal: Rhythm drives the performance.\nTranslation: Il ritmo guida la performance."
	if got != want {
		t.Fatalf("content = %q, want %q", got, want)
	}
}
