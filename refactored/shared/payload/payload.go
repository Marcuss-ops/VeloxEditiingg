// Package payload fornisce funzioni di utility per estrarre, convertire e normalizzare
// valori da mappe map[string]interface{} (tipicamente da JSON deserializzato).
//
// Ogni funzione gestisce i type-switch necessari per lavorare con JSON deserializzato
// in Go (dove numeri possono essere float64, json.Number, int, etc.) e normalizza
// i risultati in tipi Go standard.
package payload

import (
	"encoding/json"
	"fmt"
	"strconv"
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

func FloatParam(source map[string]interface{}, fallback float64, keys ...string) float64 {
	if source == nil {
		return fallback
	}
	for _, key := range keys {
		if v, ok := source[key]; ok {
			switch vv := v.(type) {
			case float64:
				if vv > 0 {
					return vv
				}
			case float32:
				if vv > 0 {
					return float64(vv)
				}
			case int:
				if vv > 0 {
					return float64(vv)
				}
			case int64:
				if vv > 0 {
					return float64(vv)
				}
			case json.Number:
				if f, err := vv.Float64(); err == nil && f > 0 {
					return f
				}
			case string:
				if f, err := strconv.ParseFloat(strings.TrimSpace(vv), 64); err == nil && f > 0 {
					return f
				}
			}
		}
	}
	return fallback
}

func IntParam(source map[string]interface{}, fallback int, keys ...string) int {
	if source == nil {
		return fallback
	}
	for _, key := range keys {
		if v, ok := source[key]; ok {
			switch vv := v.(type) {
			case int:
				if vv > 0 {
					return vv
				}
			case int64:
				if vv > 0 {
					return int(vv)
				}
			case float64:
				if vv > 0 {
					return int(vv)
				}
			case json.Number:
				if n, err := vv.Int64(); err == nil && n > 0 {
					return int(n)
				}
			case string:
				if n, err := strconv.Atoi(strings.TrimSpace(vv)); err == nil && n > 0 {
					return n
				}
			}
		}
	}
	return fallback
}

func EnsureInt(value interface{}, fallback int) int {
	switch v := value.(type) {
	case int:
		if v > 0 {
			return v
		}
	case int64:
		if v > 0 {
			return int(v)
		}
	case float64:
		if v > 0 {
			return int(v)
		}
	case json.Number:
		if n, err := v.Int64(); err == nil && n > 0 {
			return int(n)
		}
	case string:
		if n, err := strconv.Atoi(strings.TrimSpace(v)); err == nil && n > 0 {
			return n
		}
	}
	return fallback
}

func EnsureRFC3339(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	if _, err := time.Parse(time.RFC3339, value); err == nil {
		return value
	}
	return fallback
}

func NormalizedDuration(value interface{}) float64 {
	switch v := value.(type) {
	case float64:
		return v
	case float32:
		return float64(v)
	case int:
		return float64(v)
	case int64:
		return float64(v)
	case json.Number:
		f, _ := v.Float64()
		return f
	case string:
		f, _ := strconv.ParseFloat(strings.TrimSpace(v), 64)
		return f
	default:
		return 0
	}
}

func NormalizeStringList(source map[string]interface{}, keys ...string) []string {
	if source == nil {
		return nil
	}
	var values []string
	for _, key := range keys {
		v, ok := source[key]
		if !ok {
			continue
		}
		switch vv := v.(type) {
		case []string:
			values = append(values, vv...)
		case []interface{}:
			for _, item := range vv {
				if s, ok := item.(string); ok && strings.TrimSpace(s) != "" {
					values = append(values, strings.TrimSpace(s))
				}
			}
		case string:
			for _, line := range strings.Split(vv, "\n") {
				if s := strings.TrimSpace(line); s != "" {
					values = append(values, s)
				}
			}
		}
	}
	return DedupeStrings(values)
}

func NormalizeToStrings(v interface{}) []string {
	switch val := v.(type) {
	case []string:
		return val
	case []interface{}:
		out := make([]string, 0, len(val))
		for _, item := range val {
			if s, ok := item.(string); ok && strings.TrimSpace(s) != "" {
				out = append(out, strings.TrimSpace(s))
			}
		}
		return out
	case string:
		s := strings.TrimSpace(val)
		if s == "" {
			return nil
		}
		if strings.Contains(s, "\n") {
			lines := strings.Split(s, "\n")
			out := make([]string, 0, len(lines))
			for _, line := range lines {
				if trimmed := strings.TrimSpace(line); trimmed != "" {
					out = append(out, trimmed)
				}
			}
			return out
		}
		return []string{s}
	default:
		return nil
	}
}

func DedupeStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		out = append(out, trimmed)
	}
	return out
}

func MustJSON(v interface{}) string {
	raw, _ := json.Marshal(v)
	return string(raw)
}

// ParseInt converte una string in int, ignorando errori.
func ParseInt(s string) int {
	n, _ := strconv.Atoi(strings.TrimSpace(s))
	return n
}

// ParseIntDef converte una string in int, con fallback se vuota, non valida o <= 0.
func ParseIntDef(s string, def int) int {
	n, err := strconv.Atoi(strings.TrimSpace(s))
	if err != nil || n <= 0 {
		return def
	}
	return n
}

// ParseIntParam converte una string in int, restituendo l'errore di parsing.
// Restituisce il default se la string è vuota.
func ParseIntParam(s string, def int) (int, error) {
	if strings.TrimSpace(s) == "" {
		return def, nil
	}
	n, err := strconv.Atoi(strings.TrimSpace(s))
	if err != nil {
		return def, err
	}
	return n, nil
}

// ParseInt64Param converte una string in int64, restituendo l'errore di parsing.
// Restituisce il default se la string è vuota.
func ParseInt64Param(s string, def int64) (int64, error) {
	if strings.TrimSpace(s) == "" {
		return def, nil
	}
	n, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	if err != nil {
		return def, err
	}
	return n, nil
}

// ParseFloatParam converte una string in float64, restituendo l'errore di parsing.
// Restituisce il default se la string è vuota.
func ParseFloatParam(s string, def float64) (float64, error) {
	if strings.TrimSpace(s) == "" {
		return def, nil
	}
	f, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
	if err != nil {
		return def, err
	}
	return f, nil
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

// AsInt converte un interface{} in int, gestendo int, int64, float64, json.Number e string.
func AsInt(v interface{}) int {
	switch t := v.(type) {
	case int:
		return t
	case int64:
		return int(t)
	case float64:
		return int(t)
	case json.Number:
		if n, err := t.Int64(); err == nil {
			return int(n)
		}
	default:
		if s, ok := v.(string); ok {
			if n, err := strconv.Atoi(strings.TrimSpace(s)); err == nil {
				return n
			}
		}
	}
	return 0
}

// FloatValue estrae un valore float64 da una mappa SENZA il guard > 0.
// A differenza di FloatParam, restituisce il valore raw anche se 0 o negativo.
// Utile per dati analytics dove 0 è un valore legittimo.
func FloatValue(data map[string]interface{}, key string) float64 {
	if data == nil {
		return 0
	}
	if v, ok := data[key]; ok {
		switch n := v.(type) {
		case float64:
			return n
		case float32:
			return float64(n)
		case int:
			return float64(n)
		case int64:
			return float64(n)
		case json.Number:
			if f, err := n.Float64(); err == nil {
				return f
			}
		}
	}
	return 0
}

// AsFloat converte un interface{} in float64, gestendo float64, float32, int, int64 e json.Number.
func AsFloat(v interface{}) float64 {
	switch t := v.(type) {
	case float64:
		return t
	case float32:
		return float64(t)
	case int:
		return float64(t)
	case int64:
		return float64(t)
	case json.Number:
		n, _ := t.Float64()
		return n
	default:
		return 0
	}
}

// NormalizeList normalizza un valore che può essere string o []interface{} in una stringa
// con elementi separati da newline. Utile per campi job come source_text, image_links, etc.
func NormalizeList(val interface{}) string {
	switch v := val.(type) {
	case []interface{}:
		var parts []string
		for _, item := range v {
			if s, ok := item.(string); ok {
				parts = append(parts, strings.TrimSpace(s))
			}
		}
		return strings.Join(parts, "\n")
	case string:
		return strings.TrimSpace(v)
	default:
		return ""
	}
}

// NormalizeListToArray normalizza un valore (string o []interface{}) in []string.
// Se è una stringa con newline, la divide in righe.
func NormalizeListToArray(val interface{}) []string {
	if val == nil {
		return nil
	}
	switch v := val.(type) {
	case []interface{}:
		var result []string
		for _, item := range v {
			if s, ok := item.(string); ok && strings.TrimSpace(s) != "" {
				result = append(result, strings.TrimSpace(s))
			}
		}
		return result
	case string:
		s := strings.TrimSpace(v)
		if s == "" {
			return nil
		}
		if strings.Contains(s, "\n") {
			var result []string
			for _, line := range strings.Split(s, "\n") {
				if trimmed := strings.TrimSpace(line); trimmed != "" {
					result = append(result, trimmed)
				}
			}
			return result
		}
		return []string{s}
	default:
		return nil
	}
}

// DeepCopyMap esegue una copia profonda di map[string]interface{} usando JSON marshal/unmarshal.
// Utile per clonare strutture annidate senza condividere riferimenti.
func DeepCopyMap(m map[string]interface{}) map[string]interface{} {
	if m == nil {
		return nil
	}
	data, _ := json.Marshal(m)
	var result map[string]interface{}
	json.Unmarshal(data, &result)
	return result
}

// IsLikelyMediaSource verifica se una stringa sembra essere una fonte multimediale
// (URL con http/https/file, o estensione video/audio comune).
func IsLikelyMediaSource(value string) bool {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		return false
	}
	return strings.HasPrefix(value, "http://") ||
		strings.HasPrefix(value, "https://") ||
		strings.HasPrefix(value, "file://") ||
		strings.HasSuffix(value, ".mp4") ||
		strings.HasSuffix(value, ".mov") ||
		strings.HasSuffix(value, ".mkv") ||
		strings.HasSuffix(value, ".webm") ||
		strings.HasSuffix(value, ".mp3") ||
		strings.HasSuffix(value, ".wav") ||
		strings.HasSuffix(value, ".m4a")
}
