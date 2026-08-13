package translation

import (
	"context"
	"fmt"
	"strings"
	"sync"
)

// maxTranslationConcurrency bounds the number of in-flight scene translation
// calls. Each scene is independent, so translating them concurrently removes
// the sequential N×provider-latency from a long script without hammering the
// provider with an unbounded fan-out.
const maxTranslationConcurrency = 4

// TranslateScenes translates each scene independently and keeps the source
// text and clip bindings intact. A requested translation failure is returned
// to the API; the job is never reported as successfully translated when it is
// not. Scene order and the translations array are preserved regardless of the
// completion order of the concurrent provider calls.
func TranslateScenes(ctx context.Context, raw map[string]interface{}, client Client) (map[string]interface{}, error) {
	target := targetLanguage(raw["translate_to"])
	if target == "" {
		return raw, nil
	}
	input := sceneList(raw["scenes"])
	if len(input) == 0 {
		return nil, fmt.Errorf("translation requires a non-empty scenes array")
	}
	result := cloneMap(raw)
	scenes := make([]interface{}, len(input))
	translations := make([]map[string]interface{}, len(input))

	var (
		wg   sync.WaitGroup
		sem  = make(chan struct{}, maxTranslationConcurrency)
		mu   sync.Mutex
		errs = make([]error, len(input)) // indexed; nil = ok
	)
	for i, value := range input {
		wg.Add(1)
		go func(i int, scene map[string]interface{}) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			text, _ := scene["text"].(string)
			translated, err := client.Translate(ctx, text, target)
			if err != nil {
				mu.Lock()
				errs[i] = fmt.Errorf("translate scene %d: %w", i, err)
				mu.Unlock()
				return
			}
			copyScene := cloneMap(scene)
			byLanguage := map[string]interface{}{}
			if existing, ok := copyScene["translations"].(map[string]interface{}); ok {
				for key, value := range existing {
					byLanguage[key] = value
				}
			}
			byLanguage[target] = translated
			copyScene["translations"] = byLanguage
			copyScene["translated_text"] = translated
			scenes[i] = copyScene
			translations[i] = map[string]interface{}{
				"index":    i,
				"language": target,
				"text":     translated,
			}
		}(i, value)
	}
	wg.Wait()
	// Surface the lowest-index failure deterministically, matching the old
	// sequential fail-fast semantics (first failing scene in order).
	for _, e := range errs {
		if e != nil {
			return nil, e
		}
	}
	result["scenes"] = scenes
	result["translations"] = translations
	result["translation_status"] = "completed"
	result["translation_language"] = target
	return result, nil
}

func sceneList(value interface{}) []map[string]interface{} {
	switch scenes := value.(type) {
	case []interface{}:
		out := make([]map[string]interface{}, 0, len(scenes))
		for _, value := range scenes {
			if scene, ok := value.(map[string]interface{}); ok {
				out = append(out, scene)
			}
		}
		return out
	case []map[string]interface{}:
		return scenes
	default:
		return nil
	}
}

func targetLanguage(value interface{}) string {
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed)
	case []interface{}:
		if len(typed) > 0 {
			if first, ok := typed[0].(string); ok {
				return strings.TrimSpace(first)
			}
		}
	}
	return ""
}

func cloneMap(input map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{}, len(input))
	for key, value := range input {
		out[key] = value
	}
	return out
}
