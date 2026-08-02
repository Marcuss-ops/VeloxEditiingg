// Package payload fornisce funzioni di utility per estrarre, convertire e normalizzare
// valori da mappe map[string]interface{} (tipicamente da JSON deserializzato).
//
// Ogni funzione gestisce i type-switch necessari per lavorare con JSON deserializzato
// in Go (dove numeri possono essere float64, json.Number, int, etc.) e normalizza
// i risultati in tipi Go standard.
//
// File split by responsibility:
//   - payload.go         → string/map extraction (FirstString, StringParam, MapParam, ...)
//   - payload_numeric.go → numeric extraction + conversion (FloatParam, IntParam, ...)
//   - payload_list.go    → list normalization (NormalizeStringList, DedupeStrings, ...)
//   - payload_json.go    → MustJSON, DeepCopyMap, IsLikelyMediaSource
package payload

import (
	"fmt"
	"strings"
	"time"
)

func FirstString(source map[string]interface{}, keys ...string) string {
	if source == nil {
		return ""
	}
	for _, key := range keys {
		if v, ok := source[key]; ok {
			switch vv := v.(type) {
			case string:
				if s := strings.TrimSpace(vv); s != "" {
					return s
				}
			case fmt.Stringer:
				if s := strings.TrimSpace(vv.String()); s != "" {
					return s
				}
			}
		}
	}
	return ""
}

// StringParam estrae un parametro string da una mappa, con fallback predefinito.
// Utile per estrarre campi JSON opzionali con default.
func StringParam(params map[string]interface{}, key, fallback string) string {
	if v, ok := params[key].(string); ok {
		return v
	}
	return fallback
}

// MapParam estrae un parametro mappa da una mappa annidata.
// Restituisce una mappa vuota se la chiave non esiste o non è un map[string]interface{}.
func MapParam(params map[string]interface{}, key string) map[string]interface{} {
	if v, ok := params[key].(map[string]interface{}); ok {
		return v
	}
	return map[string]interface{}{}
}

// SliceParam estrae un parametro slice da una mappa.
// Restituisce una slice vuota se la chiave non esiste o non è []interface{}.
func SliceParam(params map[string]interface{}, key string) []interface{} {
	if v, ok := params[key].([]interface{}); ok {
		return v
	}
	return []interface{}{}
}

// ToSliceString converte un interface{} in []string, gestendo sia []string che []interface{}.
// Le stringhe vengono trimmate e gli elementi vuoti vengono scartati.
func ToSliceString(input interface{}) []string {
	if input == nil {
		return nil
	}
	switch v := input.(type) {
	case []string:
		return v
	case []interface{}:
		result := make([]string, 0, len(v))
		for _, item := range v {
			if str, ok := item.(string); ok && strings.TrimSpace(str) != "" {
				result = append(result, strings.TrimSpace(str))
			}
		}
		if len(result) == 0 {
			return nil
		}
		return result
	default:
		return nil
	}
}

// EnsureRFC3339 valida una stringa RFC3339, restituendo il fallback se vuota o malformata.
func EnsureRFC3339(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	if _, err := time.Parse(time.RFC3339, value); err == nil {
		return value
	}
	return fallback
}

// AsString converte un interface{} in string.
// Se il valore non è nil e non è string, usa fmt.Sprintf per la conversione.
func AsString(v interface{}) string {
	if v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprintf("%v", v)
}

// FirstNonEmpty returns the first non-empty (after trimming) string from the list.
func FirstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}
